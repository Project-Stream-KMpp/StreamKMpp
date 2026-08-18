package streamkm.core

import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import streamkm.TestData

/**
 * Critères de sortie de l'étape E1.
 * Tant qu'un de ces tests est rouge, on ne commence pas E2.
 */
class KMeansSpec extends AnyFunSuite with Matchers {

  // ------------------------------------------------------------------
  // Test 1 — valeur analytique connue
  // ------------------------------------------------------------------
  test("cost : trois points colinéaires, k=1, valeur analytique") {
    val pts = Array(Point.of(-1.0), Point.of(0.0), Point.of(1.0))
    // Centre optimal = moyenne = 0 ; SSE = 1 + 0 + 1 = 2
    KMeans.cost(pts, Array(Array(0.0))) shouldBe 2.0 +- 1e-12

    val model = KMeans.fit(pts, k = 1, nRestarts = 3, seed = 1L)
    model.centers(0)(0) shouldBe 0.0 +- 1e-9
    model.cost shouldBe 2.0 +- 1e-9
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

    val model = KMeans.fit(pts, k = 2, nRestarts = 5, seed = 3L)
    val dev   = TestData.maxCenterDeviation(Array(Array(0.0, 0.0), Array(10.0, 10.0)), model.centers)
    dev should be < 0.05
  }

  // ------------------------------------------------------------------
  // Test 3 — monotonie du coût
  // ------------------------------------------------------------------
  test("lloyd : le coût ne remonte jamais") {
    // L'assertion de monotonie est dans la boucle de `lloyd` et n'est active
    // qu'avec -ea (cf. build.sbt). Ce test la déclenche sur une initialisation
    // volontairement mauvaise, et vérifie en plus la propriété de l'extérieur.
    val mix     = TestData.gaussianMixture(n = 3000, k = 4, d = 3, seed = 99L)
    val badInit = Array.fill(4)(Array.fill(3)(0.0))
    val before  = KMeans.cost(mix.points, badInit)
    val model   = KMeans.lloyd(mix.points, badInit, maxIter = 50)
    model.cost should be <= before
  }

  // ------------------------------------------------------------------
  // Test 4 — pondération (LE test qui attrape le bug classique)
  // ------------------------------------------------------------------
  test("lloyd : un point de poids w équivaut à w copies du même point") {
    val a = Array(0.0, 0.0)
    val b = Array(10.0, 0.0)

    // Version « répliquée » : 3 copies de a, 1 de b
    val replicated = Array(Point(a), Point(a), Point(a), Point(b))
    // Version « pondérée » : a de poids 3, b de poids 1
    val weighted   = Array(Point(a, 3.0), Point(b, 1.0))

    val init = Array(Array(-5.0, 0.0))

    val mR = KMeans.lloyd(replicated, init, maxIter = 50)
    val mW = KMeans.lloyd(weighted, init, maxIter = 50)

    // Barycentre attendu : (3·0 + 1·10)/4 = 2.5
    mR.centers(0)(0) shouldBe 2.5 +- 1e-9
    mW.centers(0)(0) shouldBe 2.5 +- 1e-9
    mW.cost shouldBe mR.cost +- 1e-9
  }

  // ------------------------------------------------------------------
  // Test 5 — seeding
  // ------------------------------------------------------------------
  test("seedPlusPlus : produit k centres, bornés par n, de la bonne dimension") {
    val mix = TestData.gaussianMixture(n = 500, k = 5, d = 4, seed = 21L)

    val s = KMeans.seedPlusPlus(mix.points, k = 5, new Random(1L))
    s.length shouldBe 5
    s.foreach(_.length shouldBe 4)

    // k > n : on ne peut pas produire plus de centres que de points
    val tiny = Array(Point.of(0.0), Point.of(1.0))
    KMeans.seedPlusPlus(tiny, k = 10, new Random(1L)).length shouldBe 2
  }

  test("seedPlusPlus : le seeding D² sépare les groupes éloignés") {
    // 3 amas très distants : sur 20 tirages, le seeding doit quasi toujours
    // placer un centre dans chaque amas — c'est toute la valeur de k-means++.
    val rng = new Random(5L)
    val pts = Array.tabulate(900) { i =>
      val c = (i % 3) * 1000.0
      Point(Array(c + rng.nextGaussian()))
    }

    var successes = 0
    (0 until 20).foreach { t =>
      val s      = KMeans.seedPlusPlus(pts, 3, new Random(t.toLong))
      val groups = s.map(c => math.round(c(0) / 1000.0)).distinct
      if (groups.length == 3) successes += 1
    }
    successes should be >= 18
  }

  // ------------------------------------------------------------------
  // Test 6 — fit garde bien le meilleur redémarrage
  // ------------------------------------------------------------------
  test("fit : le coût décroît (au sens large) avec le nombre de redémarrages") {
    val mix  = TestData.gaussianMixture(n = 2000, k = 6, d = 2, sigma = 2.0, seed = 33L)
    val one  = KMeans.fit(mix.points, k = 6, nRestarts = 1, seed = 8L)
    val five = KMeans.fit(mix.points, k = 6, nRestarts = 5, seed = 8L)
    // Même graine : les 5 redémarrages incluent celui de nRestarts=1.
    five.cost should be <= one.cost + 1e-9
  }
}
