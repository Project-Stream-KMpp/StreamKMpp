package streamkm.spark

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import streamkm.TestData
import streamkm.core.{KMeans, KMeansModel, PointsSoA}
import streamkm.coreset.BucketManager

private[spark] object CoresetErrorModel {
  // Inchangé — aucune dépendance à Point/PointsSoA, purement arithmétique.
  def scaleFactor(numPartitions: Int, depth: Int): Int =
    math.max(2, math.ceil(math.pow(numPartitions.toDouble, 1.0 / depth)).toInt)

  def worstCaseCompositionLevels(numPartitions: Int, depth: Int): Int = {
    val scale = scaleFactor(numPartitions, depth)
    depth * (scale - 1)
  }

  def compoundedTolerance(epsPerLevel: Double, levels: Int): Double =
    math.pow(1.0 + epsPerLevel, levels) - 1.0

  def treeReduceTolerance(numPartitions: Int, depth: Int, epsPerLevel: Double = 0.10): Double =
    compoundedTolerance(epsPerLevel, worstCaseCompositionLevels(numPartitions, depth))
}

class StreamKMSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("StreamKMSpec")
      .master("local[4]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  /** Reconstruit un `PointsSoA`, exécute `f`, libère, renvoie le résultat de `f`.
   * Évite de répéter le triplet fromPoints/appel/free à chaque test. */
  private def withStore[A](points: Array[streamkm.core.Point])(f: PointsSoA => A): A = {
    val store = PointsSoA.fromPoints(points)
    try f(store) finally store.free()
  }

  // ==================================================================
  // partialCoresets
  // ==================================================================

  test("partialCoresets : un coreset par partition, masse totale conservée") {
    val mix = TestData.gaussianMixture(n = 8000, k = 4, d = 2, seed = 31L)
    val rdd = spark.sparkContext.parallelize(mix.points, numSlices = 4)

    val params   = StreamKMParams(k = 4, m = 200, dim = 2)
    // partialCoresets renvoie désormais RDD[(Array[Point], Long)] — le Long est la
    // graine de partition, cf. décision "indépendance des graines" (StreamKMPlusPlus).
    val partials = StreamKMPlusPlus.partialCoresets(rdd, params).collect()

    partials.length shouldBe 4
    partials.foreach { case (points, _) => points.length should be <= 200 }

    val totalMass = partials.iterator.flatMap(_._1).map(_.weight).sum
    totalMass shouldBe 8000.0 +- 1e-6
  }

  // ==================================================================
  // mergeCoresets
  // ==================================================================

  test("mergeCoresets : conserve la masse totale, quel que soit le nombre de partitions") {
    val mix    = TestData.gaussianMixture(n = 10000, k = 5, d = 2, seed = 32L)
    val params = StreamKMParams(k = 5, m = BucketManager.recommendedM(5), dim = 2)

    for (nParts <- Seq(1, 2, 8)) {
      val rdd     = spark.sparkContext.parallelize(mix.points, numSlices = nParts)
      val coreset = StreamKMPlusPlus.mergeCoresets(rdd, params) // Array[Point], sérialisable
      withClue(s"nParts=$nParts : ") {
        coreset.map(_.weight).sum shouldBe 10000.0 +- 1e-6
        coreset.length should be <= params.m
      }
    }
  }

  // ==================================================================
  // LE TEST LE PLUS IMPORTANT : non-régression distribué vs séquentiel
  // ==================================================================

  test("mergeCoresets sur 8 partitions produit un clustering de qualité comparable au séquentiel") {
    val k         = 5
    val m         = BucketManager.recommendedM(k)
    val dim       = 2
    val mix       = TestData.gaussianMixture(n = 40000, k = k, d = dim, sigma = 0.5, seed = 33L)
    val numParts  = 8
    val treeDepth = 2

    // Référence séquentielle : un seul BucketManager, tout le flux d'un coup.
    val (seqCost, seqCenters) = withStore(mix.points) { seqStore =>
      val seqBm = BucketManager(m, dim, seed = 40L)
      seqBm.insertAll(seqStore)
      val seqCoreset = seqBm.extractCoreset()
      val seqModel   = KMeans.fit(seqCoreset, k, nRestarts = 5, seed = 41L)
      seqCoreset.free()
      val cost = KMeans.cost(seqStore, seqModel.centers)
      seqBm.free()
      (cost, seqModel.centers)
    }

    // Version distribuée : mêmes données, réparties sur `numParts` partitions Spark.
    val rdd            = spark.sparkContext.parallelize(mix.points, numSlices = numParts)
    val distParams     = StreamKMParams(k = k, m = m, dim = dim, seed = 40L)
    val distCoresetPts = StreamKMPlusPlus.mergeCoresets(rdd, distParams, depth = treeDepth)
    val distCoreset     = PointsSoA.fromPoints(distCoresetPts)
    val distModel        = KMeans.fit(distCoreset, k, nRestarts = 5, seed = 41L)
    distCoreset.free()

    val distCost = withStore(mix.points)(store => KMeans.cost(store, distModel.centers))

    val tolerance = CoresetErrorModel.treeReduceTolerance(numParts, treeDepth)

    withClue(
      s"séquentiel=$seqCost, distribué=$distCost, tolérance théorique=${tolerance * 100}%% : "
    ) {
      distCost should be <= seqCost * (1.0 + tolerance)
    }

    val distCentersArr = toArrayOfArrays(distModel.centers)
    TestData.maxCenterDeviation(mix.trueCenters, distCentersArr) should be < 2.0

    seqCenters.free(); distModel.centers.free()
  }

  test("mergeCoresets : le coût ne se dégrade pas franchement entre 1 et 8 partitions") {
    val k   = 4
    val m   = BucketManager.recommendedM(k)
    val dim = 2
    val mix = TestData.gaussianMixture(n = 20000, k = k, d = dim, sigma = 0.5, seed = 34L)

    val params = StreamKMParams(k = k, m = m, dim = dim, seed = 50L)

    def costWith(nParts: Int): Double = {
      val rdd         = spark.sparkContext.parallelize(mix.points, numSlices = nParts)
      val coresetPts  = StreamKMPlusPlus.mergeCoresets(rdd, params)
      val coresetStore = PointsSoA.fromPoints(coresetPts)
      val model         = KMeans.fit(coresetStore, k, nRestarts = 5, seed = 51L)
      coresetStore.free()
      val result = withStore(mix.points)(store => KMeans.cost(store, model.centers))
      model.centers.free()
      result
    }

    val cost1 = costWith(1)
    val cost8 = costWith(8)

    withClue(s"1 partition=$cost1, 8 partitions=$cost8 : ") {
      math.abs(cost1 - cost8) should be <= 0.3 * math.max(cost1, cost8)
    }
  }

  test("partialCoresets et mergeCoresets utilisent des graines distinctes par partition") {
    val pts    = TestData.gaussianMixture(n = 2000, k = 3, d = 2, seed = 60L).points
    val rdd    = spark.sparkContext.parallelize(pts ++ pts, numSlices = 2)
    val params = StreamKMParams(k = 3, m = 50, dim = 2)

    val partials = StreamKMPlusPlus.partialCoresets(rdd, params).collect()
    partials.length shouldBe 2
    // Déstructuration : on ne compare que les points, la graine de partition (second
    // élément du tuple) n'a pas à être identique — au contraire, c'est justement ce
    // qu'on vérifie indirectement via la divergence des coresets produits.
    val coordsA = partials(0)._1.map(_.coords.toSeq).toSet
    val coordsB = partials(1)._1.map(_.coords.toSeq).toSet
    coordsA should not equal coordsB
  }

  // ==================================================================
  // ÉTAT STREAMING DANS LE TEMPS
  // ==================================================================

  test(
    "état streaming : l'insertion multi micro-batches conserve exactement la masse, " +
      "sans double-comptage ni perte"
  ) {
    val k   = 4
    val m   = BucketManager.recommendedM(k)
    val dim = 2
    val mix = TestData.gaussianMixture(n = 30000, k = k, d = dim, sigma = 0.5, seed = 70L)

    val chunks = mix.points.grouped(10000).toArray
    chunks.length shouldBe 3

    val global = BucketManager(m, dim, seed = 71L)
    var cumulativeExpected = 0.0

    chunks.zipWithIndex.foreach { case (chunk, batchId) =>
      val rdd     = spark.sparkContext.parallelize(chunk, numSlices = 4)
      val partial = StreamKMPlusPlus.mergeCoresets(rdd, StreamKMParams(k = k, m = m, dim = dim, seed = 71L + batchId))
      // partial est Array[Point] (AoS, sérialisable) — insertion point par point dans
      // le BucketManager global, qui attend (coords, weight).
      var i = 0
      while (i < partial.length) { global.insert(partial(i).coords, partial(i).weight); i += 1 }

      cumulativeExpected += chunk.length.toDouble
      withClue(s"après le micro-batch $batchId : ") {
        global.numPointsSeen shouldBe cumulativeExpected +- 1e-6
      }
    }

    val finalCoreset = global.extractCoreset()
    var mass = 0.0
    var i = 0
    while (i < finalCoreset.n) { mass += finalCoreset.weight(i); i += 1 }
    mass shouldBe 30000.0 +- 1e-6

    finalCoreset.free(); global.free()
  }

  // Ignoré sous Windows sans winutils.exe/HADOOP_HOME — cf. CLAUDE.md, décision 13.
  test("run() sur plusieurs micro-batches produit une qualité comparable à un traitement en un seul bloc") {
    val k   = 4
    val m   = BucketManager.recommendedM(k)
    val dim = 2
    val mix = TestData.gaussianMixture(n = 30000, k = k, d = dim, sigma = 0.5, seed = 72L)
    val chunks = mix.points.grouped(10000).toArray
    chunks.length shouldBe 3

    val sparkSession: SparkSession = spark
    implicit val sqlCtx: SQLContext = sparkSession.sqlContext
    import sparkSession.implicits._
    val source = MemoryStream[Array[Double]]
    val df     = source.toDF().withColumnRenamed("value", "features")

    val models = scala.collection.mutable.ArrayBuffer.empty[KMeansModel]
    val query  = StreamKMPlusPlus.run(df, StreamKMParams(k = k, m = m, dim = dim, seed = 73L), models += _)

    try {
      chunks.foreach { chunk =>
        source.addData(chunk.map(_.coords))
        query.processAllAvailable()
      }
    } finally {
      query.stop()
    }

    models should have length 3

    val streamCost = withStore(mix.points)(store => KMeans.cost(store, models.last.centers))
    val lastCentersArr = toArrayOfArrays(models.last.centers)
    TestData.maxCenterDeviation(mix.trueCenters, lastCentersArr) should be < 2.0

    // Référence : les mêmes points, en un seul bloc.
    val refBm = BucketManager(m, dim, seed = 71L)
    val rdd   = spark.sparkContext.parallelize(mix.points, numSlices = 4)
    val refPartial = StreamKMPlusPlus.mergeCoresets(rdd, StreamKMParams(k = k, m = m, dim = dim, seed = 71L))
    var i = 0
    while (i < refPartial.length) { refBm.insert(refPartial(i).coords, refPartial(i).weight); i += 1 }
    val refCoreset = refBm.extractCoreset()
    val refModel   = KMeans.fit(refCoreset, k, nRestarts = 5, seed = 73L)
    refCoreset.free()
    val refCost = withStore(mix.points)(store => KMeans.cost(store, refModel.centers))

    val tolerance = CoresetErrorModel.treeReduceTolerance(numPartitions = 4, depth = 2)
    withClue(s"streaming (3 micro-batches)=$streamCost, référence (1 bloc)=$refCost : ") {
      streamCost should be <= refCost * (1.0 + tolerance)
    }

    models.foreach(_.centers.free())
    refModel.centers.free(); refBm.free()
  }

  /** Conversion PointsSoA → Array[Array[Double]], pour réutiliser TestData.maxCenterDeviation
   * tel quel (resté en AoS). */
  private def toArrayOfArrays(store: PointsSoA): Array[Array[Double]] = {
    val out = new Array[Array[Double]](store.n)
    val tmp = new Array[Double](store.dimension)
    var i = 0
    while (i < store.n) { store.copyCoordinatesInto(i, tmp); out(i) = tmp.clone(); i += 1 }
    out
  }
}