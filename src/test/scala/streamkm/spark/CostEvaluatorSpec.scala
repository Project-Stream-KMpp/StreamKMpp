package streamkm.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import streamkm.TestData
import streamkm.core.KMeans

/**
 * Critères de sortie de l'étape E5.
 *
 * Contrairement à `StreamKMSpec` (qui compare des coresets construits avec de l'aléa,
 * donc avec tolérance), le coût est une simple somme : le test central ici est une
 * égalité STRICTE (à la précision flottante près) entre le calcul distribué et le calcul
 * séquentiel de référence (`KMeans.cost`, déjà validé par `KMeansSpec`). Un écart
 * signifierait un vrai bug de comptage (points oubliés ou comptés deux fois lors du
 * partitionnement), pas une variance d'échantillonnage.
 */
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
    val mix     = TestData.gaussianMixture(n = 12000, k = 4, d = 3, seed = 80L)
    val model   = KMeans.fit(mix.points, k = 4, nRestarts = 3, seed = 81L)
    val refCost = KMeans.cost(mix.points, model.centers)

    val rdd = spark.sparkContext.parallelize(mix.points, numSlices = 6)
    val distCost = CostEvaluator.cost(rdd, model.centers)

    // Tolérance relative très fine : seule source d'écart possible = ordre de sommation
    // flottant entre partitions, pas un biais d'échantillonnage (cf. CoresetErrorModel,
    // sans objet ici puisqu'aucun coreset n'est construit par CostEvaluator).
    distCost shouldBe refCost +- (refCost.abs * 1e-9 + 1e-9)
  }

  test("cost : invariant au nombre de partitions") {
    val mix   = TestData.gaussianMixture(n = 9000, k = 3, d = 2, seed = 82L)
    val model = KMeans.fit(mix.points, k = 3, nRestarts = 3, seed = 83L)

    val costs = Seq(1, 3, 9).map { nParts =>
      val rdd = spark.sparkContext.parallelize(mix.points, numSlices = nParts)
      nParts -> CostEvaluator.cost(rdd, model.centers)
    }

    val values = costs.map(_._2)
    withClue(s"coûts par partitionnement : $costs : ") {
      values.max - values.min should be <= (values.max.abs * 1e-9 + 1e-9)
    }
  }

  test("cost : détecte une régression grossière (mauvais centres = coût très supérieur)") {
    // Garde-fou : vérifie que la fonction n'est pas trivialement toujours d'accord avec
    // elle-même (un test qui ne peut jamais échouer ne teste rien). Des centres tirés
    // loin de la vraie structure doivent produire un coût nettement plus élevé.
    val mix = TestData.gaussianMixture(n = 6000, k = 4, d = 2, sigma = 0.5, seed = 84L)
    val goodModel = KMeans.fit(mix.points, k = 4, nRestarts = 5, seed = 85L)

    val badCenters = Array.fill(4)(Array(1000.0, 1000.0)) // loin de toutes les gaussiennes

    val rdd = spark.sparkContext.parallelize(mix.points, numSlices = 4)
    val goodCost = CostEvaluator.cost(rdd, goodModel.centers)
    val badCost  = CostEvaluator.cost(rdd, badCenters)

    badCost should be > goodCost * 10.0
  }

  test("cost : accepte directement un KMeansModel (surcharge de confort)") {
    val mix   = TestData.gaussianMixture(n = 3000, k = 2, d = 2, seed = 86L)
    val model = KMeans.fit(mix.points, k = 2, nRestarts = 3, seed = 87L)
    val rdd   = spark.sparkContext.parallelize(mix.points, numSlices = 3)

    CostEvaluator.cost(rdd, model) shouldBe CostEvaluator.cost(rdd, model.centers)
  }
}
