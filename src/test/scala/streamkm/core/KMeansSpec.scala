package streamkm.core

import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import streamkm.TestData

class KMeansSpec extends AnyFunSuite with Matchers {

  /** Convertit un PointsSoA en Array[Array[Double]] pour réutiliser TestData tel quel
   * (TestData reste en AoS, cf. décision prise pour CostEvaluatorSpec). */
  private def toArrayOfArrays(store: PointsSoA): Array[Array[Double]] = {
    val out = new Array[Array[Double]](store.n)
    val tmp = new Array[Double](store.dimension)
    var i = 0
    while (i < store.n) {
      store.copyCoordinatesInto(i, tmp)
      out(i) = tmp.clone()
      i += 1
    }
    out
  }

  // ------------------------------------------------------------------
  // Test 1 — valeur analytique connue
  // ------------------------------------------------------------------
  test("cost : trois points colinéaires, k=1, valeur analytique") {
    val pts   = PointsSoA.fromPoints(Array(Point.of(-1.0), Point.of(0.0), Point.of(1.0)))
    val zero  = PointsSoA.allocate(1, dimension = 1)
    zero.append(Array(0.0), 1.0)

    KMeans.cost(pts, zero) shouldBe 2.0 +- 1e-12

    val model = KMeans.fit(pts, k = 1, nRestarts = 3, seed = 1L)
    model.centers.coordinates(0, 0) shouldBe 0.0 +- 1e-9
    model.cost shouldBe 2.0 +- 1e-9

    pts.free(); zero.free(); model.centers.free()
  }

  // ------------------------------------------------------------------
  // Test 2 — deux gaussiennes bien séparées
  // ------------------------------------------------------------------
  test("lloyd : retrouve deux gaussiennes séparées") {
    val rng = new Random(7L)
    val pts = Array.tabulate(2000) { i =>
      val c = if (i % 2 == 0) 0.0 else 10.0
      Point(Array(c + rng.nextGaussian() * 0.1, c + rng.nextGaussian() * 0.1))
    }
    val store = PointsSoA.fromPoints(pts)

    val model = KMeans.fit(store, k = 2, nRestarts = 5, seed = 3L)
    val dev   = TestData.maxCenterDeviation(Array(Array(0.0, 0.0), Array(10.0, 10.0)), toArrayOfArrays(model.centers))
    dev should be < 0.05

    store.free(); model.centers.free()
  }

  // ------------------------------------------------------------------
  // Test 3 — monotonie du coût
  // ------------------------------------------------------------------
  test("lloyd : le coût ne remonte jamais") {
    val mix     = TestData.gaussianMixture(n = 3000, k = 4, d = 3, seed = 99L)
    val store   = PointsSoA.fromPoints(mix.points)
    val badInit = PointsSoA.allocate(4, dimension = 3)
    var c = 0
    while (c < 4) { badInit.append(Array(0.0, 0.0, 0.0), 1.0); c += 1 }

    val before = KMeans.cost(store, badInit)
    // lloyd consomme et libère `badInit` (contrat établi lors de l'adaptation de KMeans) :
    // on ne peut donc plus s'en servir après l'appel, d'où le calcul de `before` en premier.
    val model  = KMeans.lloyd(store, badInit, maxIter = 50)
    model.cost should be <= before

    store.free(); model.centers.free()
  }

  // ------------------------------------------------------------------
  // Test 4 — pondération (LE test qui attrape le bug classique)
  // ------------------------------------------------------------------
  test("lloyd : un point de poids w équivaut à w copies du même point") {
    val a = Array(0.0, 0.0)
    val b = Array(10.0, 0.0)

    val replicated = PointsSoA.fromPoints(Array(Point(a), Point(a), Point(a), Point(b)))
    val weighted   = PointsSoA.fromPoints(Array(Point(a, 3.0), Point(b, 1.0)))

    def freshInit(): PointsSoA = {
      val init = PointsSoA.allocate(1, dimension = 2)
      init.append(Array(-5.0, 0.0), 1.0)
      init
    }

    val mR = KMeans.lloyd(replicated, freshInit(), maxIter = 50)
    val mW = KMeans.lloyd(weighted, freshInit(), maxIter = 50)

    mR.centers.coordinates(0, 0) shouldBe 2.5 +- 1e-9
    mW.centers.coordinates(0, 0) shouldBe 2.5 +- 1e-9
    mW.cost shouldBe mR.cost +- 1e-9

    replicated.free(); weighted.free()
    mR.centers.free(); mW.centers.free()
  }

  // ------------------------------------------------------------------
  // Test 5 — seeding
  // ------------------------------------------------------------------
  test("seedPlusPlus : produit k centres, bornés par n, de la bonne dimension") {
    val mix   = TestData.gaussianMixture(n = 500, k = 5, d = 4, seed = 21L)
    val store = PointsSoA.fromPoints(mix.points)

    val s = KMeans.seedPlusPlus(store, k = 5, new Random(1L))
    s.n shouldBe 5
    s.dimension shouldBe 4
    s.free()

    val tinyStore = PointsSoA.fromPoints(Array(Point.of(0.0), Point.of(1.0)))
    val s2 = KMeans.seedPlusPlus(tinyStore, k = 10, new Random(1L))
    s2.n shouldBe 2
    s2.free()

    store.free(); tinyStore.free()
  }

  test("seedPlusPlus : le seeding D² sépare les groupes éloignés") {
    val rng = new Random(5L)
    val pts = Array.tabulate(900) { i =>
      val c = (i % 3) * 1000.0
      Point(Array(c + rng.nextGaussian()))
    }
    val store = PointsSoA.fromPoints(pts)

    var successes = 0
    (0 until 20).foreach { t =>
      val s      = KMeans.seedPlusPlus(store, 3, new Random(t.toLong))
      val groups = (0 until s.n).map(i => math.round(s.coordinates(i, 0) / 1000.0)).distinct
      if (groups.length == 3) successes += 1
      s.free()
    }
    successes should be >= 18

    store.free()
  }

  // ------------------------------------------------------------------
  // Test 6 — fit garde bien le meilleur redémarrage
  // ------------------------------------------------------------------
  test("fit : le coût décroît (au sens large) avec le nombre de redémarrages") {
    val mix   = TestData.gaussianMixture(n = 2000, k = 6, d = 2, sigma = 2.0, seed = 33L)
    val store = PointsSoA.fromPoints(mix.points)

    val one  = KMeans.fit(store, k = 6, nRestarts = 1, seed = 8L)
    val five = KMeans.fit(store, k = 6, nRestarts = 5, seed = 8L)
    five.cost should be <= one.cost + 1e-9

    store.free(); one.centers.free(); five.centers.free()
  }
}