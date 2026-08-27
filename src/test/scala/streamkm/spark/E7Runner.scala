package streamkm.spark

import java.io.{File, PrintWriter}

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream

import streamkm.TestData
import streamkm.core.{KMeans, KMeansModel, PointsSoA}
import streamkm.coreset.BucketManager

/**
 * Runner des expériences E7 (ch. 5 du rapport).
 *
 * Chaque sous-expérience produit un CSV dans results/ :
 *   E7a — runtime vs. quality (≡ §5.2.1 Bitsakis)
 *   E7b — runtime & cost vs. numPartitions (≡ §5.3.1)
 *   E7c — throughput vs. volume (≡ §5.3.2)
 *   E7d — latence micro-batch par trigger (propre à Structured Streaming)
 *
 * Lancement : sbt "Test/runMain streamkm.spark.E7Runner [a|b|c|d|all]"
 * Sans argument, exécute toutes les expériences.
 */
object E7Runner {

  def main(args: Array[String]): Unit = {
    val which = if (args.isEmpty) "all" else args(0).toLowerCase

    new File("results").mkdirs()

    val spark = SparkSession.builder()
      .appName("E7Runner")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try {
      if (which == "a" || which == "all") runE7a(spark)
      if (which == "b" || which == "all") runE7b(spark)
      if (which == "c" || which == "all") runE7c(spark)
      if (which == "d" || which == "all") runE7d(spark)
    } finally {
      spark.stop()
    }
  }

  // ---------------------------------------------------------------------------
  // E7a — Runtime vs. Quality  (≡ §5.2.1)
  // ---------------------------------------------------------------------------
  def runE7a(spark: SparkSession): Unit = {
    val pw = new PrintWriter(new File("results/e7a_runtime_vs_quality.csv"))
    pw.println("k,m,rep,timeMs,cost")

    val n              = 500000
    val dim            = 10
    val ks             = Seq(10, 25, 50)
    val ms             = Seq(500, 1000, 2000, 5000, 10000, 20000)
    val numPartitions  = 4
    val nReps          = 5

    for (k <- ks; m <- ms; rep <- 1 to nReps) {
      val mix    = TestData.gaussianMixture(n = n, k = k, d = dim, seed = 100L + rep)
      val rdd    = spark.sparkContext.parallelize(mix.points, numSlices = numPartitions)
      val params = StreamKMParams(k = k, m = m, dim = dim, seed = 42L + rep)

      val t0          = System.nanoTime()
      val coresetPts  = StreamKMPlusPlus.mergeCoresets(rdd, params)
      val coresetStore = PointsSoA.fromPoints(coresetPts)
      val model        = KMeans.fit(coresetStore, k, nRestarts = 1, seed = 42L + rep)
      coresetStore.free()
      val timeMs = (System.nanoTime() - t0) / 1e6

      val fullCost = CostEvaluator.cost(rdd, model)
      model.centers.free()

      pw.println(s"$k,$m,$rep,${timeMs.toLong},$fullCost")
      pw.flush()
      println(s"[E7a] k=$k m=$m rep=$rep time=${timeMs.toLong}ms cost=$fullCost")
    }

    pw.close()
    println("[E7a] → results/e7a_runtime_vs_quality.csv")
  }

  // ---------------------------------------------------------------------------
  // E7b — Runtime & Cost vs. numPartitions  (≡ §5.3.1)
  // ---------------------------------------------------------------------------
  def runE7b(spark: SparkSession): Unit = {
    val pw = new PrintWriter(new File("results/e7b_runtime_vs_partitions.csv"))
    pw.println("k,numPartitions,rep,timeMs,cost")

    val n             = 1000000
    val dim           = 10
    val ks            = Seq(10, 25, 50)
    val numPartitions = Seq(1, 2, 4, 8)
    val nReps         = 4

    for (k <- ks; nParts <- numPartitions; rep <- 1 to nReps) {
      val m      = BucketManager.recommendedM(k)
      val mix    = TestData.gaussianMixture(n = n, k = k, d = dim, seed = 200L + rep)
      val rdd    = spark.sparkContext.parallelize(mix.points, numSlices = nParts)
      val params = StreamKMParams(k = k, m = m, dim = dim, seed = 42L + rep)

      val t0          = System.nanoTime()
      val coresetPts  = StreamKMPlusPlus.mergeCoresets(rdd, params)
      val coresetStore = PointsSoA.fromPoints(coresetPts)
      val model        = KMeans.fit(coresetStore, k, nRestarts = 1, seed = 42L + rep)
      coresetStore.free()
      val timeMs = (System.nanoTime() - t0) / 1e6

      val fullCost = CostEvaluator.cost(rdd, model)
      model.centers.free()

      pw.println(s"$k,$nParts,$rep,${timeMs.toLong},$fullCost")
      pw.flush()
      println(s"[E7b] k=$k nParts=$nParts rep=$rep time=${timeMs.toLong}ms cost=$fullCost")
    }

    pw.close()
    println("[E7b] → results/e7b_runtime_vs_partitions.csv")
  }

  // ---------------------------------------------------------------------------
  // E7c — Throughput vs. volume  (≡ §5.3.2)
  // ---------------------------------------------------------------------------
  def runE7c(spark: SparkSession): Unit = {
    val pw = new PrintWriter(new File("results/e7c_throughput.csv"))
    pw.println("numPartitions,k,n,rep,timeMs,throughput")

    val ns            = Seq(100000, 200000, 500000, 1000000, 2000000)
    val dim           = 10
    val numPartitions = Seq(4, 8)
    val k             = 10
    val m             = 2000
    val nReps         = 4

    for (nParts <- numPartitions; n <- ns; rep <- 1 to nReps) {
      val mix    = TestData.gaussianMixture(n = n, k = k, d = dim, seed = 300L + rep)
      val rdd    = spark.sparkContext.parallelize(mix.points, numSlices = nParts)
      val params = StreamKMParams(k = k, m = m, dim = dim, seed = 42L + rep)

      val t0          = System.nanoTime()
      val coresetPts  = StreamKMPlusPlus.mergeCoresets(rdd, params)
      val coresetStore = PointsSoA.fromPoints(coresetPts)
      val model        = KMeans.fit(coresetStore, k, nRestarts = 1, seed = 42L + rep)
      coresetStore.free()
      model.centers.free()
      val timeMs = (System.nanoTime() - t0) / 1e6

      val throughput = n / timeMs * 1000
      pw.println(s"$nParts,$k,$n,$rep,${timeMs.toLong},${throughput.toLong}")
      pw.flush()
      println(s"[E7c] nParts=$nParts n=$n rep=$rep time=${timeMs.toLong}ms throughput=${throughput.toLong} tuples/s")
    }

    pw.close()
    println("[E7c] → results/e7c_throughput.csv")
  }

  // ---------------------------------------------------------------------------
  // E7d — Latence micro-batch  (spécifique Structured Streaming, sans équivalent §5)
  // ---------------------------------------------------------------------------
  def runE7d(spark: SparkSession): Unit = {
    val pw = new PrintWriter(new File("results/e7d_microbatch_latency.csv"))
    pw.println("batchSize,trigger,latencyMs")

    implicit val sqlCtx: SQLContext = spark.sqlContext
    import spark.implicits._

    val batchSizes    = Seq(500, 2000, 10000, 50000)
    val dim           = 10
    val k             = 10
    val numPartitions = 4
    val nTriggers     = 10

    for (batchSize <- batchSizes) {
      val m      = BucketManager.recommendedM(k)
      val params = StreamKMParams(k = k, m = m, dim = dim, seed = 42L)

      val source = MemoryStream[Array[Double]]
      val df     = source.toDF()
        .withColumnRenamed("value", "features")
        .repartition(numPartitions)

      // Chaque KMeansModel reçu porte un PointsSoA off-heap (model.centers) — libéré
      // dès qu'il n'est plus le modèle courant, pour ne pas accumuler les buffers
      // natifs des triggers précédents sur toute la durée du sweep de batchSizes.
      @volatile var onModelTime = 0L
      @volatile var lastModel: KMeansModel = null
      val query = StreamKMPlusPlus.run(df, params, (model: KMeansModel) => {
        if (lastModel != null) lastModel.centers.free()
        lastModel    = model
        onModelTime  = System.nanoTime()
      })

      // Warmup : 2 batches non enregistrés
      for (_ <- 1 to 2) {
        val chunk = TestData.gaussianMixture(n = batchSize, k = k, d = dim, seed = 400L).points
        source.addData(chunk.map(_.coords))
        query.processAllAvailable()
      }

      // Mesure
      for (trigger <- 1 to nTriggers) {
        val chunk = TestData.gaussianMixture(n = batchSize, k = k, d = dim, seed = 400L + trigger).points
        val t0    = System.nanoTime()
        source.addData(chunk.map(_.coords))
        query.processAllAvailable()
        val latencyMs = (onModelTime - t0) / 1e6
        pw.println(s"$batchSize,$trigger,${latencyMs.toLong}")
        pw.flush()
        println(s"[E7d] batchSize=$batchSize trigger=$trigger latency=${latencyMs.toLong}ms")
      }

      query.stop()
      if (lastModel != null) { lastModel.centers.free(); lastModel = null }
    }

    pw.close()
    println("[E7d] → results/e7d_microbatch_latency.csv")
  }
}