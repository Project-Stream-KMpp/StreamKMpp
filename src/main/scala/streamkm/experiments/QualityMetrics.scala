package streamkm.experiments

import java.io.{File, FileOutputStream, OutputStreamWriter, PrintWriter}
import java.nio.charset.StandardCharsets

import scala.io.Source

import org.apache.spark.ml.clustering.{KMeans => MLKMeans}
import org.apache.spark.ml.linalg.{SQLDataTypes, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row => SqlRow, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}

import streamkm.core.{KMeans, Point, PointsSoA}
import streamkm.coreset.BucketManager
import streamkm.spark.{CostEvaluator, StreamKMParams, StreamKMPlusPlus}

object QualityMetrics {

  final case class ResultRow(dataset: String, method: String, k: Int, m: Int, cost: Double)

  def loadCovertype(path: String, dim: Int = 54, maxPoints: Option[Int] = None): Array[Point] = {
    val src = Source.fromFile(path)
    try {
      val lines = maxPoints match {
        case Some(n) => src.getLines().take(n)
        case None    => src.getLines()
      }
      lines.flatMap { line =>
        val cols = line.trim.split(",")
        if (cols.length < dim) None
        else Point.parse(cols.take(dim).mkString(","), dim, ',')
      }.toArray
    } finally src.close()
  }

  /**
   * Référence séquentielle. `points` reste `Array[Point]` en entrée (interop avec
   * `loadCovertype`/MLlib) — converti en `PointsSoA` localement, libéré en sortie
   * de méthode, aucun `PointsSoA` ne s'échappe de cette fonction.
   */
  def sequentialCost(points: Array[Point], k: Int, m: Int, dim: Int, seed: Long): Double = {
    val pointsStore = PointsSoA.fromPoints(points)
    val manager     = BucketManager(m, dim, seed)
    manager.insertAll(pointsStore)

    val coreset = manager.extractCoreset()
    val model   = KMeans.fit(coreset, k, nRestarts = 5, seed = seed)
    coreset.free() // consommé par fit (copié dans seedPlusPlus/lloyd), plus utile après

    val result = KMeans.cost(pointsStore, model.centers)

    pointsStore.free()
    model.centers.free()
    manager.free()
    result
  }

  /**
   * Version distribuée. `StreamKMParams` exige maintenant `dim`. Le coreset renvoyé
   * par `mergeCoresets` est `Array[Point]` (sérialisable, cf. adaptation de
   * `StreamKMPlusPlus`) — reconverti en `PointsSoA` ici pour `KMeans.fit`, qui EN
   * DEVIENT propriétaire pour la durée de l'ajustement mais ne le libère pas lui-même
   * (seul `init`, le second argument, est libéré par `lloyd` — cf. décision prise
   * lors de l'adaptation de `KMeans`). D'où le `.free()` explicite ici après usage.
   */
  def distributedCost(rdd: RDD[Point], k: Int, m: Int, dim: Int, seed: Long, numPartitions: Int): Double = {
    val partitioned = rdd.repartition(numPartitions)

    val coresetPoints = StreamKMPlusPlus.mergeCoresets(partitioned, StreamKMParams(k = k, m = m, dim = dim, seed = seed))
    val coresetStore   = PointsSoA.fromPoints(coresetPoints)

    val model = KMeans.fit(coresetStore, k, nRestarts = 5, seed = seed)
    coresetStore.free()

    val result = CostEvaluator.cost(partitioned, model.centers)
    model.centers.free()
    result
  }

  /** Inchangé — ne touche jamais à PointsSoA, reste sur Point.coords directement. */
  def mllibCost(spark: SparkSession, points: Array[Point], k: Int, seed: Long, numPartitions: Int): Double = {
    val schema = StructType(Seq(StructField("features", SQLDataTypes.VectorType, nullable = false)))
    val rowRdd = spark.sparkContext
      .parallelize(points, numPartitions)
      .map(p => SqlRow(Vectors.dense(p.coords)))
    val df = spark.createDataFrame(rowRdd, schema)

    val model = new MLKMeans().setK(k).setSeed(seed).setFeaturesCol("features").fit(df)
    model.summary.trainingCost
  }

  def runKSweep(
                 spark:         SparkSession,
                 datasetName:   String,
                 points:        Array[Point],
                 dim:           Int,
                 ks:            Seq[Int],
                 numPartitions: Int,
                 seed:          Long
               ): Seq[ResultRow] = {
    val rdd = spark.sparkContext.parallelize(points, numPartitions)
    ks.flatMap { k =>
      val m = BucketManager.recommendedM(k)
      println(s"[QualityMetrics] k-sweep : k=$k, m=$m ...")

      val seq  = sequentialCost(points, k, m, dim, seed)
      val dist = distributedCost(rdd, k, m, dim, seed, numPartitions)
      val mll  = mllibCost(spark, points, k, seed, numPartitions)

      Seq(
        ResultRow(datasetName, "Séquentiel (E2)",                       k, m, seq),
        ResultRow(datasetName, "Spark distribué (notre implémentation)", k, m, dist),
        ResultRow(datasetName, "MLlib KMeans (batch, sans coreset)",     k, m, mll)
      )
    }
  }

  def runMSweep(
                 spark:         SparkSession,
                 datasetName:   String,
                 points:        Array[Point],
                 dim:           Int,
                 k:             Int,
                 ms:            Seq[Int],
                 numPartitions: Int,
                 seed:          Long
               ): Seq[ResultRow] = {
    val rdd = spark.sparkContext.parallelize(points, numPartitions)
    ms.flatMap { m =>
      println(s"[QualityMetrics] m-sweep : m=$m (k=$k) ...")
      val seq  = sequentialCost(points, k, m, dim, seed)
      val dist = distributedCost(rdd, k, m, dim, seed, numPartitions)
      Seq(
        ResultRow(datasetName, "Séquentiel (E2)",                       k, m, seq),
        ResultRow(datasetName, "Spark distribué (notre implémentation)", k, m, dist)
      )
    }
  }

  def writeCsv(path: String, rows: Seq[ResultRow]): Unit = {
    val file = new File(path)
    Option(file.getParentFile).foreach(_.mkdirs())
    val pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8))
    try {
      pw.println("dataset,method,k,m,cost")
      rows.foreach(r => pw.println(s"${r.dataset},${r.method},${r.k},${r.m},${r.cost}"))
    } finally pw.close()
  }

  def main(args: Array[String]): Unit = {
    val path          = if (args.length > 0) args(0) else "data/covertype/covtype.data"
    val maxPoints     = if (args.length > 1 && args(1).toInt > 0) Some(args(1).toInt) else None
    val dim           = 54
    val seed          = 2026L
    val numPartitions = 8

    val spark = SparkSession.builder()
      .appName("QualityMetrics")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      println(s"[QualityMetrics] chargement de $path (maxPoints=$maxPoints)...")
      val points = loadCovertype(path, dim = dim, maxPoints = maxPoints)
      println(s"[QualityMetrics] ${points.length} points chargés.")

      val kSweepRows = runKSweep(spark, "Covertype", points, dim, Seq(10, 20, 30, 40, 50), numPartitions, seed)
      writeCsv("results/quality_k_sweep_covertype.csv", kSweepRows)

      val mSweepRows = runMSweep(spark, "Covertype", points, dim, k = 25, Seq(1000, 5000, 10000, 20000, 50000), numPartitions, seed)
      writeCsv("results/quality_m_sweep_covertype.csv", mSweepRows)

      println("[QualityMetrics] terminé.")
      println("  -> results/quality_k_sweep_covertype.csv")
      println("  -> results/quality_m_sweep_covertype.csv")
    } finally {
      spark.stop()
    }
  }
}