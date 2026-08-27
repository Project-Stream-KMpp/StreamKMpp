package streamkm.demo

import scala.util.Random

import streamkm.core.{Distance, KMeans, NearestResult, Point, PointsSoA}
import streamkm.coreset.BucketManager

object Main {

  private def gaussianMixture(
                               n: Int, k: Int, d: Int, sigma: Double, spread: Double, rng: Random
                             ): (Array[Point], Array[Array[Double]]) = {
    val trueCenters = Array.tabulate(k) { c =>
      Array.tabulate(d)(j => ((c + 1) * (j + 1) % k).toDouble * spread)
    }
    val points = Array.tabulate(n) { _ =>
      val ctr = trueCenters(rng.nextInt(k))
      Point(Array.tabulate(d)(j => ctr(j) + rng.nextGaussian() * sigma))
    }
    (points, trueCenters)
  }

  def main(args: Array[String]): Unit = {
    val n    = 200000
    val k    = 5
    val d    = 2
    val m    = BucketManager.recommendedM(k)
    val seed = 42L
    val rng  = new Random(seed)

    val (points, trueCenters) = gaussianMixture(n, k, d, sigma = 0.5, spread = 20.0, rng)

    println(s"StreamKM++ (démo E1/E2, SoA off-heap) — n=$n, k=$k, d=$d, m=$m")

    val pointsStore = PointsSoA.fromPoints(points)

    val t0      = System.nanoTime()
    val manager = BucketManager(m, d, seed)
    manager.insertAll(pointsStore)
    val tIngest = (System.nanoTime() - t0) / 1e6

    val t1          = System.nanoTime()
    val coreset     = manager.extractCoreset()
    val coresetSize = coreset.n
    val tExtract    = (System.nanoTime() - t1) / 1e6

    val t2      = System.nanoTime()
    val model   = KMeans.fit(coreset, k, nRestarts = 5, seed = seed)
    val tKmeans = (System.nanoTime() - t2) / 1e6
    coreset.free()

    val costOnFullData = KMeans.cost(pointsStore, model.centers)
    val batchModel      = KMeans.fit(pointsStore, k, nRestarts = 5, seed = seed)
    val batchCost        = batchModel.cost

    val trueCentersStore = PointsSoA.fromPoints(trueCenters.map(Point(_)))
    val nearestResult    = new NearestResult(0, 0.0)
    var maxDeviation = 0.0
    var i = 0
    while (i < trueCentersStore.n) {
      Distance.nearest(trueCentersStore, i, model.centers, nearestResult)
      val deviation = math.sqrt(nearestResult.distance)
      if (deviation > maxDeviation) maxDeviation = deviation
      i += 1
    }

    println(f"Points vus par le manager : ${manager.numPointsSeen}%.0f (attendu $n)")
    println(f"Coreset final             : $coresetSize points (m=$m)")
    println(f"Temps ingestion flux      : $tIngest%.1f ms")
    println(f"Temps extraction coreset  : $tExtract%.1f ms")
    println(f"Temps k-means++ (coreset) : $tKmeans%.1f ms")
    println()
    println(f"Coût (coreset) sur données complètes : $costOnFullData%.2f")
    println(f"Coût k-means++ batch (référence)      : $batchCost%.2f (ratio ${costOnFullData / batchCost}%.3f)")
    println(f"Écart max centre trouvé / vrai centre : $maxDeviation%.3f")
    println()
    println("Centres trouvés :")
    val centerCoords = new Array[Double](d)
    i = 0
    while (i < model.centers.n) {
      model.centers.copyCoordinatesInto(i, centerCoords)
      println("  " + centerCoords.map(v => f"$v%.2f").mkString("(", ", ", ")"))
      i += 1
    }

    pointsStore.free()
    trueCentersStore.free()
    model.centers.free()
    batchModel.centers.free()
    manager.free()
  }
}