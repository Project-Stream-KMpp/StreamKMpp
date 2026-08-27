package streamkm.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import streamkm.TestData
import streamkm.core.{KMeans, PointsSoA}

class CostEvaluatorSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("CostEvaluatorSpec")
      .master("local[4]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  test("cost : égal (à la précision flottante près) au calcul séquentiel KMeans.cost") {
    val mix         = TestData.gaussianMixture(n = 12000, k = 4, d = 3, seed = 80L)
    val pointsStore = PointsSoA.fromPoints(mix.points)
    val model       = KMeans.fit(pointsStore, k = 4, nRestarts = 3, seed = 81L)
    val refCost     = KMeans.cost(pointsStore, model.centers)

    val rdd      = spark.sparkContext.parallelize(mix.points, numSlices = 6)
    val distCost = CostEvaluator.cost(rdd, model.centers)

    distCost shouldBe refCost +- (refCost.abs * 1e-9 + 1e-9)

    pointsStore.free()
    model.centers.free()
  }

  test("cost : invariant au nombre de partitions") {
    val mix         = TestData.gaussianMixture(n = 9000, k = 3, d = 2, seed = 82L)
    val pointsStore = PointsSoA.fromPoints(mix.points)
    val model       = KMeans.fit(pointsStore, k = 3, nRestarts = 3, seed = 83L)

    val costs = Seq(1, 3, 9).map { nParts =>
      val rdd = spark.sparkContext.parallelize(mix.points, numSlices = nParts)
      nParts -> CostEvaluator.cost(rdd, model.centers)
    }

    val values = costs.map(_._2)
    withClue(s"coûts par partitionnement : $costs : ") {
      values.max - values.min should be <= (values.max.abs * 1e-9 + 1e-9)
    }

    pointsStore.free()
    model.centers.free()
  }

  test("cost : détecte une régression grossière (mauvais centres = coût très supérieur)") {
    val mix           = TestData.gaussianMixture(n = 6000, k = 4, d = 2, sigma = 0.5, seed = 84L)
    val pointsStore   = PointsSoA.fromPoints(mix.points)
    val goodModel     = KMeans.fit(pointsStore, k = 4, nRestarts = 5, seed = 85L)

    // Centres délibérément mauvais — construits directement en PointsSoA plutôt que
    // via Array[Array[Double]] (ancienne API), pour rester cohérent avec la signature
    // que CostEvaluator.cost attend désormais.
    val badCenters = PointsSoA.allocate(4, dimension = 2)
    var i = 0
    while (i < 4) { badCenters.append(Array(1000.0, 1000.0), 1.0); i += 1 }

    val rdd      = spark.sparkContext.parallelize(mix.points, numSlices = 4)
    val goodCost = CostEvaluator.cost(rdd, goodModel.centers)
    val badCost  = CostEvaluator.cost(rdd, badCenters)

    badCost should be > goodCost * 10.0

    pointsStore.free()
    goodModel.centers.free()
    badCenters.free()
  }

  test("cost : accepte directement un KMeansModel (surcharge de confort)") {
    val mix         = TestData.gaussianMixture(n = 3000, k = 2, d = 2, seed = 86L)
    val pointsStore = PointsSoA.fromPoints(mix.points)
    val model       = KMeans.fit(pointsStore, k = 2, nRestarts = 3, seed = 87L)
    val rdd         = spark.sparkContext.parallelize(mix.points, numSlices = 3)

    CostEvaluator.cost(rdd, model) shouldBe CostEvaluator.cost(rdd, model.centers)

    pointsStore.free()
    model.centers.free()
  }
}