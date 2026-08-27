package streamkm.coreset

import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import streamkm.TestData
import streamkm.core.{KMeans, Point, PointsSoA}

class CoresetSpec extends AnyFunSuite with Matchers {

  private def totalMass(store: PointsSoA): Double = {
    var sum = 0.0
    var i = 0
    while (i < store.n) { sum += store.weight(i); i += 1 }
    sum
  }

  // ==================================================================
  // CoresetTree
  // ==================================================================

  test("CoresetTree : renvoie l'ensemble tel quel si n <= m") {
    val store = PointsSoA.fromPoints(TestData.gaussianMixture(n = 50, k = 3, d = 2, seed = 1L).points)
    val cs    = CoresetTree.build(store, coresetSize = 100, new Random(1L))
    cs.n shouldBe 50
    totalMass(cs) shouldBe 50.0 +- 1e-9
    cs.free() // == store ici (renvoyé tel quel, n <= m) : un seul free() suffit
  }

  test("CoresetTree : produit au plus m points") {
    for (m <- Seq(10, 100, 1000)) {
      val store = PointsSoA.fromPoints(TestData.gaussianMixture(n = 5000, k = 5, d = 3, seed = 2L).points)
      val cs    = CoresetTree.build(store, m, new Random(m.toLong))
      cs.n should be <= m
      cs.free()
    }
  }

  test("CoresetTree : conserve exactement la masse totale") {
    for (m <- Seq(10, 100, 500)) {
      val store = PointsSoA.fromPoints(TestData.gaussianMixture(n = 5000, k = 5, d = 3, seed = 3L).points)
      val cs    = CoresetTree.build(store, m, new Random(m.toLong))
      withClue(s"m=$m : ") { totalMass(cs) shouldBe 5000.0 +- 1e-6 }
      cs.free()
    }
  }

  test("CoresetTree : conserve la masse sur des points déjà pondérés") {
    val rng = new Random(4L)
    val pts = Array.tabulate(3000)(_ =>
      Point(Array(rng.nextGaussian() * 10, rng.nextGaussian() * 10), 1.0 + rng.nextInt(9))
    )
    val store    = PointsSoA.fromPoints(pts)
    val expected = totalMass(store)
    // `store` sera consommé par build ; il faut donc calculer `expected` avant l'appel.
    val cs = CoresetTree.build(store, coresetSize = 200, new Random(5L))
    totalMass(cs) shouldBe expected +- 1e-6
    cs.free()
  }

  test("CoresetTree : gère un ensemble de points tous identiques") {
    val store = PointsSoA.fromPoints(Array.fill(1000)(Point(Array(3.0, 4.0))))
    val cs    = CoresetTree.build(store, coresetSize = 50, new Random(6L))
    cs.n should be <= 50
    totalMass(cs) shouldBe 1000.0 +- 1e-9
    var i = 0
    while (i < cs.n) {
      cs.coordinates(i, 0) shouldBe 3.0
      cs.coordinates(i, 1) shouldBe 4.0
      i += 1
    }
    cs.free()
  }

  test("CoresetTree : le coreset préserve la qualité du clustering") {
    val mix        = TestData.gaussianMixture(n = 20000, k = 5, d = 2, sigma = 0.5, seed = 7L)
    val fullStore  = PointsSoA.fromPoints(mix.points)
    val full       = KMeans.fit(fullStore, k = 5, nRestarts = 5, seed = 11L)

    val coresetSrc = PointsSoA.fromPoints(mix.points) // second store : le premier a servi à `full`, indépendant du coreset
    val cs         = CoresetTree.build(coresetSrc, coresetSize = 200, new Random(12L))
    val onCoreset  = KMeans.fit(cs, k = 5, nRestarts = 5, seed = 11L)
    val costViaCores = KMeans.cost(fullStore, onCoreset.centers)

    withClue(s"complet=${full.cost}, via coreset=$costViaCores : ") {
      costViaCores should be <= full.cost * 1.10
    }

    fullStore.free(); full.centers.free(); onCoreset.centers.free()
  }

  // ==================================================================
  // BucketManager
  // ==================================================================

  test("BucketManager : compte correctement les points consommés") {
    val bm    = BucketManager(coresetSize = 100, dimension = 2, seed = 1L)
    val store = PointsSoA.fromPoints(TestData.gaussianMixture(n = 5000, k = 4, d = 2, seed = 8L).points)
    bm.insertAll(store)
    bm.numPointsSeen shouldBe 5000.0 +- 1e-9
    store.free(); bm.free()
  }

  test("BucketManager : respecte les invariants de taille des buckets") {
    val bm  = BucketManager(coresetSize = 64, dimension = 2, seed = 2L)
    val pts = TestData.gaussianMixture(n = 4000, k = 4, d = 2, seed = 9L).points
    pts.foreach { p =>
      bm.insert(p.coords, p.weight)
      bm.checkInvariants() shouldBe true
    }
    bm.free()
  }

  test("BucketManager : le nombre de niveaux croît logarithmiquement") {
    val m     = 100
    val bm    = BucketManager(m, dimension = 2, seed = 3L)
    val store = PointsSoA.fromPoints(TestData.gaussianMixture(n = 100000, k = 3, d = 2, seed = 10L).points)
    bm.insertAll(store)
    bm.numLevels should be <= 16
    store.free(); bm.free()
  }

  test("BucketManager : union() et extractCoreset() conservent la masse") {
    val bm    = BucketManager(coresetSize = 128, dimension = 3, seed = 4L)
    val store = PointsSoA.fromPoints(TestData.gaussianMixture(n = 10000, k = 5, d = 3, seed = 11L).points)
    bm.insertAll(store)

    val u = bm.union()
    totalMass(u) shouldBe 10000.0 +- 1e-6
    u.free()

    val ec = bm.extractCoreset()
    totalMass(ec) shouldBe 10000.0 +- 1e-6
    ec.n should be <= 128
    ec.free()

    store.free(); bm.free()
  }

  test("BucketManager : extractCoreset ne détruit pas l'état") {
    val bm     = BucketManager(coresetSize = 64, dimension = 2, seed = 5L)
    val store1 = PointsSoA.fromPoints(TestData.gaussianMixture(n = 5000, k = 3, d = 2, seed = 12L).points)
    bm.insertAll(store1)

    val first = bm.extractCoreset()
    first.n should be <= 64
    first.free()

    val store2 = PointsSoA.fromPoints(TestData.gaussianMixture(n = 5000, k = 3, d = 2, seed = 13L).points)
    bm.insertAll(store2)
    val second = bm.extractCoreset()

    bm.numPointsSeen shouldBe 10000.0 +- 1e-9
    totalMass(second) shouldBe 10000.0 +- 1e-6

    second.free(); store1.free(); store2.free(); bm.free()
  }

  test("BucketManager : merge conserve la masse des deux opérandes") {
    val a = BucketManager(coresetSize = 100, dimension = 2, seed = 6L)
    val b = BucketManager(coresetSize = 100, dimension = 2, seed = 7L)
    val storeA = PointsSoA.fromPoints(TestData.gaussianMixture(n = 4000, k = 4, d = 2, seed = 14L).points)
    val storeB = PointsSoA.fromPoints(TestData.gaussianMixture(n = 6000, k = 4, d = 2, seed = 15L).points)
    a.insertAll(storeA); b.insertAll(storeB)

    val merged = a.merge(b)
    merged.numPointsSeen shouldBe 10000.0 +- 1e-9
    val mu = merged.union()
    totalMass(mu) shouldBe 10000.0 +- 1e-6
    mu.free()

    a.numPointsSeen shouldBe 4000.0 +- 1e-9
    b.numPointsSeen shouldBe 6000.0 +- 1e-9

    storeA.free(); storeB.free(); a.free(); b.free(); merged.free()
  }

  test("BucketManager : insertion refusée après merge") {
    val a = BucketManager(coresetSize = 32, dimension = 2, seed = 8L)
    val b = BucketManager(coresetSize = 32, dimension = 2, seed = 9L)
    val storeA = PointsSoA.fromPoints(TestData.gaussianMixture(n = 500, k = 2, d = 2, seed = 16L).points)
    val storeB = PointsSoA.fromPoints(TestData.gaussianMixture(n = 500, k = 2, d = 2, seed = 17L).points)
    a.insertAll(storeA); b.insertAll(storeB)

    an[IllegalStateException] should be thrownBy a.merge(b).insert(Array(0.0, 0.0), 1.0)

    storeA.free(); storeB.free(); a.free(); b.free()
  }

  // ==================================================================
  // Bout en bout : StreamKM++ séquentiel
  // ==================================================================

  test("StreamKM++ séquentiel : qualité comparable au k-means++ batch") {
    val k   = 5
    val m   = BucketManager.recommendedM(k)
    val mix = TestData.gaussianMixture(n = 50000, k = k, d = 2, sigma = 0.5, seed = 18L)

    val bm    = BucketManager(m, dimension = 2, seed = 19L)
    val store = PointsSoA.fromPoints(mix.points)
    bm.insertAll(store)

    val finalCoreset = bm.union()
    val streamModel  = KMeans.fit(finalCoreset, k, nRestarts = 5, seed = 20L)
    val streamCost   = KMeans.cost(store, streamModel.centers)

    val refStore  = PointsSoA.fromPoints(mix.points)
    val batchModel = KMeans.fit(refStore, k, nRestarts = 5, seed = 20L)
    val batchCost  = batchModel.cost

    withClue(s"batch=$batchCost, stream=$streamCost : ") {
      streamCost should be <= batchCost * 1.10
    }

    val centersArr = {
      val out = new Array[Array[Double]](streamModel.centers.n)
      val tmp = new Array[Double](streamModel.centers.dimension)
      var i = 0
      while (i < streamModel.centers.n) { streamModel.centers.copyCoordinatesInto(i, tmp); out(i) = tmp.clone(); i += 1 }
      out
    }
    TestData.maxCenterDeviation(mix.trueCenters, centersArr) should be < 0.5

    store.free(); finalCoreset.free(); streamModel.centers.free()
    refStore.free(); batchModel.centers.free(); bm.free()
  }
}