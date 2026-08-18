package streamkm.demo

import scala.util.Random

import streamkm.core.{Distance, KMeans, Point}
import streamkm.coreset.BucketManager

/**
 * Démonstrateur de bout en bout pour les couches E1 (`streamkm.core`) et
 * E2 (`streamkm.coreset`), en l'absence de la couche `streamkm.spark` (E4,
 * volontairement hors scope de cette session).
 *
 * Rejoue en un seul thread l'algorithme StreamKM++ complet tel que décrit au
 * §2.3.4 de la thèse (« The StreamKM++ Algorithm ») :
 *   1. consommer le flux point par point via le merge-and-reduce (algo 2.5) ;
 *   2. extraire le coreset final (union des buckets non vides, §2.3.4) ;
 *   3. appliquer k-means++ (seeding D² algo 2.2 + Lloyd algo 2.1) sur ce
 *      coreset et garder le meilleur des `nRestarts` redémarrages — c'est la
 *      même politique que l'opérateur `FlatMapToKmeansPP` du §4.1 de la thèse.
 *
 * C'est exactement le scénario « parallélisme 1 » qui sert de référence à
 * l'expérience E7 du METHODO (fusion séquentielle vs `treeAggregate`).
 */
object Main {

  /**
   * Génère un mélange de `k` gaussiennes isotropes en dimension `d`, à vérité
   * terrain connue. Les vrais centres sont placés sur une grille de pas
   * `spread` pour garantir leur séparabilité — reproduit ici la même
   * construction que `TestData.gaussianMixture` (scope test), qu'on ne peut
   * pas référencer depuis `src/main`.
   */
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
    val n      = 200000 // taille du flux
    val k      = 5
    val d      = 2
    val m      = BucketManager.recommendedM(k) // 200·k, thèse §2.3.4 clarification 1
    val seed   = 42L
    val rng    = new Random(seed)

    val (points, trueCenters) = gaussianMixture(n, k, d, sigma = 0.5, spread = 20.0, rng)

    println(s"StreamKM++ (démo E1/E2, sans Spark) — n=$n, k=$k, d=$d, m=$m")

    // --- Étape 1 : consommation du flux, un point à la fois (algo 2.5) -----
    // Un seul BucketManager = un seul subtask Flink / une seule partition Spark.
    // C'est la structure que E4 fera tourner en parallèle via treeAggregate.
    val t0 = System.nanoTime()
    val manager = BucketManager(m, seed)
    manager.insertAll(points.iterator)
    val tIngest = (System.nanoTime() - t0) / 1e6

    // --- Étape 2 : extraction du coreset final (union des buckets, §2.3.4) -
    val t1       = System.nanoTime()
    val coreset  = manager.extractCoreset()
    val tExtract = (System.nanoTime() - t1) / 1e6

    // --- Étape 3 : k-means++ sur le coreset (5 redémarrages, §4.1) ----------
    val t2      = System.nanoTime()
    val model   = KMeans.fit(coreset, k, nRestarts = 5, seed = seed)
    val tKmeans = (System.nanoTime() - t2) / 1e6

    // Coût mesuré sur les DONNÉES COMPLÈTES avec les centres issus du coreset :
    // seule comparaison honnête (thèse §4.2, cost(P,C) = Σ D²(x,C)), le coût
    // évalué sur le coreset lui-même n'est pas comparable à un coût batch.
    val costOnFullData = KMeans.cost(points, model.centers)
    val batchCost       = KMeans.fit(points, k, nRestarts = 5, seed = seed).cost
    val maxDeviation     = trueCenters.map(tc => math.sqrt(Distance.nearest(tc, model.centers)._2)).max

    println(f"Points vus par le manager : ${manager.numPointsSeen}%.0f (attendu $n)")
    println(f"Coreset final             : ${coreset.length} points (m=$m)")
    println(f"Temps ingestion flux      : $tIngest%.1f ms")
    println(f"Temps extraction coreset  : $tExtract%.1f ms")
    println(f"Temps k-means++ (coreset) : $tKmeans%.1f ms")
    println()
    println(f"Coût (coreset) sur données complètes : $costOnFullData%.2f")
    println(f"Coût k-means++ batch (référence)      : $batchCost%.2f (ratio ${costOnFullData / batchCost}%.3f)")
    println(f"Écart max centre trouvé / vrai centre : $maxDeviation%.3f")
    println()
    println("Centres trouvés :")
    model.centers.foreach(c => println("  " + c.map(v => f"$v%.2f").mkString("(", ", ", ")")))
  }
}
