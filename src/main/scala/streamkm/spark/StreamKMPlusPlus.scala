package streamkm.spark

import scala.util.Random
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

import streamkm.core.{KMeans, KMeansModel, Point, PointsSoA}
import streamkm.coreset.{BucketManager, CoresetTree}

final case class StreamKMParams(
                                 k:         Int,
                                 m:         Int,
                                 dim:       Int,
                                 nRestarts: Int = 5,
                                 seed:      Long = 42L
                               )

object StreamKMPlusPlus {

  /**
   * Un coreset partiel par PARTITION, accompagné de la graine qui a servi à le
   * produire — nécessaire pour que `mergeCoresets` puisse combiner les graines de
   * façon déterministe à chaque niveau de fusion (voir plus bas).
   */
  def partialCoresets(rdd: RDD[Point], p: StreamKMParams): RDD[(Array[Point], Long)] =
    rdd.mapPartitionsWithIndex { (idx, it) =>
      val partitionSeed  = p.seed + idx
      val bucketManager  = new BucketManager(p.m, p.dim, partitionSeed)
      while (it.hasNext) {
        val point = it.next()
        bucketManager.insert(point.coords, point.weight)
      }
      val partialCoreset = bucketManager.extractCoreset()
      val result          = PointsSoA.toPoints(partialCoreset)
      partialCoreset.free()
      bucketManager.free()
      Iterator.single((result, partitionSeed))
    }

  /**
   * Fusion en arbre sur des `(Array[Point], Long)` — jamais de PointsSoA sur le
   * réseau. À CHAQUE niveau de fusion, la graine du nœud résultant est
   * `seedA ^ seedB` (mêmes opérandes combinés que `BucketManager.merge` en E2,
   * CLAUDE.md décision 10) : ça restaure l'indépendance de l'aléa à chaque étage
   * de l'arbre de réduction, pas seulement au premier niveau partition → coreset
   * partiel.
   */
  def mergeCoresets(rdd: RDD[Point], p: StreamKMParams, depth: Int = 2): Array[Point] = {
    val partial = partialCoresets(rdd, p)
    val (finalPoints, _) = partial.treeReduce(
      (left, right) => {
        val (pointsA, seedA) = left
        val (pointsB, seedB) = right
        val combinedSeed     = seedA ^ seedB

        val storeA   = PointsSoA.fromPoints(pointsA)
        val storeB   = PointsSoA.fromPoints(pointsB)
        val combined = PointsSoA.concat(storeA, storeB)
        storeA.free(); storeB.free()

        val reduced = CoresetTree.build(combined, p.m, new Random(combinedSeed))
        val result  = PointsSoA.toPoints(reduced)
        reduced.free()

        (result, combinedSeed)
      },
      depth
    )
    finalPoints
  }

  def run(
           source:      DataFrame,
           p:           StreamKMParams,
           onModel:     KMeansModel => Unit,
           featuresCol: String = "features"
         ): StreamingQuery = {

    val global = new BucketManager(p.m, p.dim, p.seed)

    source.writeStream.foreachBatch { (batch: DataFrame, batchId: Long) =>
      val t0 = System.nanoTime()

      val points: RDD[Point] = batch.rdd.map { row =>
        val coords = row.getAs[Seq[Double]](featuresCol).toArray
        Point(coords)
      }
      val numPartitions = points.getNumPartitions

      val partial = mergeCoresets(points, p.copy(seed = p.seed + batchId))
      var i = 0
      while (i < partial.length) { global.insert(partial(i).coords, partial(i).weight); i += 1 }

      val coreset   = global.extractCoreset()
      val model     = KMeans.fit(coreset, p.k, p.nRestarts, seed = p.seed)
      coreset.free()
      val elapsedMs = (System.nanoTime() - t0) / 1e6

      // scalastyle:off println
      println(
        s"[StreamKM++] batch=$batchId partitions=$numPartitions m=${p.m} k=${p.k} " +
          f"pointsSeen=${global.numPointsSeen}%.0f buildTimeMs=$elapsedMs%.1f cost=${model.cost}%.4g"
      )
      // scalastyle:on println

      onModel(model)
    }.start()
  }
}