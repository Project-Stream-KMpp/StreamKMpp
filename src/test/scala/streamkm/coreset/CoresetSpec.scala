package streamkm.coreset

import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import streamkm.TestData
import streamkm.core.{KMeans, Point}

/**
 * Critères de sortie de l'étape E2.
 *
 * Le test central est la CONSERVATION DE LA MASSE. Un coreset dont la somme des poids
 * ne vaut pas n est faux, quelle que soit son apparence — et c'est le seul symptôme
 * détectable à coup sûr, parce qu'un coreset légèrement erroné produit tout de même
 * des centres plausibles.
 */
class CoresetSpec extends AnyFunSuite with Matchers {

  // ==================================================================
  // CoresetTree
  // ==================================================================

  test("CoresetTree : renvoie l'ensemble tel quel si n <= m") {
    val pts = TestData.gaussianMixture(n = 50, k = 3, d = 2, seed = 1L).points
    val cs  = CoresetTree.build(pts, m = 100, new Random(1L))
    cs.length shouldBe 50
    TestData.totalMass(cs) shouldBe 50.0 +- 1e-9
  }

  test("CoresetTree : produit au plus m points") {
    val pts = TestData.gaussianMixture(n = 5000, k = 5, d = 3, seed = 2L).points
    for (m <- Seq(10, 100, 1000)) {
      CoresetTree.build(pts, m, new Random(m.toLong)).length should be <= m
    }
  }

  test("CoresetTree : conserve exactement la masse totale") {
    val pts = TestData.gaussianMixture(n = 5000, k = 5, d = 3, seed = 3L).points
    for (m <- Seq(10, 100, 500)) {
      val cs = CoresetTree.build(pts, m, new Random(m.toLong))
      withClue(s"m=$m : ") {
        TestData.totalMass(cs) shouldBe 5000.0 +- 1e-6
      }
    }
  }

  test("CoresetTree : conserve la masse sur des points déjà pondérés") {
    val rng = new Random(4L)
    val pts = Array.tabulate(3000)(_ =>
      Point(Array(rng.nextGaussian() * 10, rng.nextGaussian() * 10), 1.0 + rng.nextInt(9))
    )
    val expected = TestData.totalMass(pts)
    val cs       = CoresetTree.build(pts, m = 200, new Random(5L))
    TestData.totalMass(cs) shouldBe expected +- 1e-6
  }

  test("CoresetTree : gère un ensemble de points tous identiques") {
    // Coût global nul : la boucle de construction doit s'arrêter sans boucler à l'infini.
    val pts = Array.fill(1000)(Point(Array(3.0, 4.0)))
    val cs  = CoresetTree.build(pts, m = 50, new Random(6L))
    cs.length should be <= 50
    TestData.totalMass(cs) shouldBe 1000.0 +- 1e-9
    cs.foreach { p => p.coords(0) shouldBe 3.0; p.coords(1) shouldBe 4.0 }
  }

  test("CoresetTree : le coreset préserve la qualité du clustering") {
    val mix  = TestData.gaussianMixture(n = 20000, k = 5, d = 2, sigma = 0.5, seed = 7L)
    val full = KMeans.fit(mix.points, k = 5, nRestarts = 5, seed = 11L)

    val cs           = CoresetTree.build(mix.points, m = 200, new Random(12L))
    val onCoreset    = KMeans.fit(cs, k = 5, nRestarts = 5, seed = 11L)
    // On évalue les centres issus du coreset sur les DONNÉES COMPLÈTES : c'est la seule
    // comparaison honnête (le coût mesuré sur le coreset n'est pas comparable).
    val costViaCores = KMeans.cost(mix.points, onCoreset.centers)

    withClue(s"complet=${full.cost}, via coreset=$costViaCores : ") {
      costViaCores should be <= full.cost * 1.10
    }
  }

  // ==================================================================
  // BucketManager
  // ==================================================================

  test("BucketManager : compte correctement les points consommés") {
    val bm  = BucketManager(m = 100, seed = 1L)
    val pts = TestData.gaussianMixture(n = 5000, k = 4, d = 2, seed = 8L).points
    bm.insertAll(pts)
    bm.numPointsSeen shouldBe 5000.0 +- 1e-9
  }

  test("BucketManager : respecte les invariants de taille des buckets") {
    val bm  = BucketManager(m = 64, seed = 2L)
    val pts = TestData.gaussianMixture(n = 4000, k = 4, d = 2, seed = 9L).points
    // On vérifie après chaque insertion, pas seulement à la fin.
    pts.foreach { p =>
      bm.insert(p)
      bm.checkInvariants() shouldBe true
    }
  }

  test("BucketManager : le nombre de niveaux croît logarithmiquement") {
    val m  = 100
    val bm = BucketManager(m, seed = 3L)
    bm.insertAll(TestData.gaussianMixture(n = 100000, k = 3, d = 2, seed = 10L).points)
    // O(log2(n/m)) + 2 ≈ 10 + 2. On laisse une marge confortable ; le point du test est
    // qu'on ne soit PAS en O(n/m) = 1000.
    bm.numLevels should be <= 16
  }

  test("BucketManager : union() et extractCoreset() conservent la masse") {
    val bm = BucketManager(m = 128, seed = 4L)
    bm.insertAll(TestData.gaussianMixture(n = 10000, k = 5, d = 3, seed = 11L).points)

    TestData.totalMass(bm.union())          shouldBe 10000.0 +- 1e-6
    TestData.totalMass(bm.extractCoreset()) shouldBe 10000.0 +- 1e-6
    bm.extractCoreset().length              should be <= 128
  }

  test("BucketManager : extractCoreset ne détruit pas l'état") {
    val bm = BucketManager(m = 64, seed = 5L)
    bm.insertAll(TestData.gaussianMixture(n = 5000, k = 3, d = 2, seed = 12L).points)

    val first = bm.extractCoreset()
    bm.insertAll(TestData.gaussianMixture(n = 5000, k = 3, d = 2, seed = 13L).points)
    val second = bm.extractCoreset()

    first.length  should be <= 64
    bm.numPointsSeen shouldBe 10000.0 +- 1e-9
    TestData.totalMass(second) shouldBe 10000.0 +- 1e-6
  }

  test("BucketManager : merge conserve la masse des deux opérandes") {
    val a = BucketManager(m = 100, seed = 6L)
    val b = BucketManager(m = 100, seed = 7L)
    a.insertAll(TestData.gaussianMixture(n = 4000, k = 4, d = 2, seed = 14L).points)
    b.insertAll(TestData.gaussianMixture(n = 6000, k = 4, d = 2, seed = 15L).points)

    val merged = a.merge(b)
    merged.numPointsSeen shouldBe 10000.0 +- 1e-9
    TestData.totalMass(merged.union()) shouldBe 10000.0 +- 1e-6

    // Les opérandes ne sont pas modifiés (indispensable pour un combOp de treeAggregate,
    // que Spark peut ré-exécuter en cas de perte de tâche).
    a.numPointsSeen shouldBe 4000.0 +- 1e-9
    b.numPointsSeen shouldBe 6000.0 +- 1e-9
  }

  test("BucketManager : insertion refusée après merge") {
    val a = BucketManager(m = 32, seed = 8L)
    val b = BucketManager(m = 32, seed = 9L)
    a.insertAll(TestData.gaussianMixture(n = 500, k = 2, d = 2, seed = 16L).points)
    b.insertAll(TestData.gaussianMixture(n = 500, k = 2, d = 2, seed = 17L).points)

    an[IllegalStateException] should be thrownBy a.merge(b).insert(Point.of(0.0, 0.0))
  }

  // ==================================================================
  // Bout en bout : StreamKM++ séquentiel
  // ==================================================================

  test("StreamKM++ séquentiel : qualité comparable au k-means++ batch") {
    // C'est l'algorithme complet du §2.3.4, en un seul thread : merge-and-reduce sur
    // tout le flux, puis k-means++ sur l'union finale des buckets.
    // Il servira de référence (« parallélisme 1 ») pour toutes les expériences de E7.
    val k   = 5
    val m   = BucketManager.recommendedM(k) // 200·k, cf. §2.3.4
    val mix = TestData.gaussianMixture(n = 50000, k = k, d = 2, sigma = 0.5, seed = 18L)

    val bm = BucketManager(m, seed = 19L)
    bm.insertAll(mix.points)

    val finalCoreset = bm.union()
    val streamModel  = KMeans.fit(finalCoreset, k, nRestarts = 5, seed = 20L)
    val streamCost   = KMeans.cost(mix.points, streamModel.centers)

    val batchCost = KMeans.fit(mix.points, k, nRestarts = 5, seed = 20L).cost

    withClue(s"batch=$batchCost, stream=$streamCost : ") {
      streamCost should be <= batchCost * 1.10
    }
    TestData.maxCenterDeviation(mix.trueCenters, streamModel.centers) should be < 0.5
  }
}
