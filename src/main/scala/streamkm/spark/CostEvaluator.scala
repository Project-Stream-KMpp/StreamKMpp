package streamkm.spark

import org.apache.spark.rdd.RDD

import streamkm.core.{KMeans, KMeansModel, Point}

/**
 * Portage Spark de la méthodologie d'évaluation du coût (thèse §4.2 : job séparé,
 * indépendant du dataflow §4.1, qui recalcule `cost(P, C) = Σ_x w_x · D²(x, C)` en
 * parallèle sur l'ensemble du flux à partir d'un jeu de centres déjà connu).
 *
 * RÈGLE D'OR (SPEC §1) : aucune logique algorithmique ici. La formule du coût vit dans
 * `KMeans.cost` (couche 1, déjà testée par `KMeansSpec`) ; ce fichier ne fait que la
 * distribuer sur les partitions d'un RDD et diffuser les centres par broadcast.
 *
 * C'est l'instrument de mesure nécessaire à TOUTES les expériences du ch. 5 (E1 —
 * correction contre une vérité terrain ou MLlib, E2 — trade-off taille de coreset, E6 —
 * comparaison de référence) : elles comparent toutes des valeurs produites par cette
 * fonction, jamais le coût mesuré sur le coreset lui-même (qui n'est pas comparable,
 * cf. le commentaire de `demo.Main`).
 *
 * Correspondance avec la thèse (METHODO §3, table "§4.2") :
 *   - `FlatMapToCentroid` + `.broadcast()`         → `sc.broadcast(centers)`
 *   - `CoFlatMapToPartialCost` (connected stream)   → `mapPartitions(... KMeans.cost ...)`
 *   - `CoFlatMapToFinalCost` (opérateur par. 1)     → `treeReduce(_ + _)`
 * Ce que la thèse implémente avec un connected stream, un buffer d'attente et trois
 * opérateurs Flink dédiés tient ici en trois lignes de `broadcast` + `treeReduce`.
 */
object CostEvaluator {

  /**
   * Coût total (SSE pondérée) de `centers` sur l'ensemble du RDD `points`, calculé en
   * parallèle. Chaque partition appelle `KMeans.cost` (couche 1) sur sa propre part des
   * données avec les centres broadcastés, puis les coûts partiels sont sommés par un
   * `treeReduce` (arbre de sommation, plus robuste qu'une réduction linéaire pour un
   * grand nombre de partitions — même raisonnement que `StreamKMPlusPlus.mergeCoresets`).
   *
   * Contrairement à la fusion de coresets, cette agrégation est une simple somme :
   * aucune composition d'erreur ici (l'addition est associative et exacte à la précision
   * flottante près), donc pas de tolérance à dériver — la valeur ne dépend pas du nombre
   * de partitions choisi (à l'ordre de sommation flottant près).
   */
  def cost(points: RDD[Point], centers: Array[Array[Double]], depth: Int = 2): Double = {
    val bcCenters = points.sparkContext.broadcast(centers)
    points
      .mapPartitions(it => Iterator.single(KMeans.cost(it.toArray, bcCenters.value)))
      .treeReduce(_ + _, depth)
  }

  /** Confort : coût d'un `KMeansModel` déjà ajusté (celui produit par `onModel` de `run`). */
  def cost(points: RDD[Point], model: KMeansModel): Double = cost(points, model.centers)
}
