package streamkm.spark

import org.apache.spark.rdd.RDD

import streamkm.core.{KMeans, KMeansModel, Point, PointsSoA}

object CostEvaluator {

  /**
   * Coût total (SSE pondérée) de `centers` sur l'ensemble du RDD `points`.
   *
   * `centers` (PointsSoA, off-heap) n'est pas sérialisable — même contrainte que
   * pour `StreamKMPlusPlus.mergeCoresets` : conversion en `Array[Point]` (petit,
   * borné par k) AVANT le broadcast, reconstruction d'un `PointsSoA` local à
   * chaque partition, libéré aussitôt le coût partiel calculé. `centers` lui-même
   * n'est jamais libéré ici : cette fonction ne fait que le LIRE (via
   * `toPoints`), l'appelant en reste propriétaire — même convention que
   * `KMeans.cost`.
   */
  def cost(points: RDD[Point], centers: PointsSoA, depth: Int = 2): Double = {
    val centersAsPoints = PointsSoA.toPoints(centers)
    val bcCenters        = points.sparkContext.broadcast(centersAsPoints)

    points
      .mapPartitions { it =>
        val partitionPoints = it.toArray
        if (partitionPoints.isEmpty) Iterator.single(0.0)
        else {
          val pointsStore  = PointsSoA.fromPoints(partitionPoints)
          val centersStore = PointsSoA.fromPoints(bcCenters.value)
          val partialCost   = KMeans.cost(pointsStore, centersStore)
          pointsStore.free()
          centersStore.free()
          Iterator.single(partialCost)
        }
      }
      .treeReduce(_ + _, depth)
  }

  def cost(points: RDD[Point], model: KMeansModel): Double = cost(points, model.centers)
}