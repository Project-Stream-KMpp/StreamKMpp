package streamkm.core

import scala.util.Random

/** Résultat d'un clustering : les centres, la SSE pondérée atteinte, le nombre d'itérations. */
final case class KMeansModel(
  centers:    Array[Array[Double]],
  cost:       Double,
  iterations: Int
) extends Serializable {

  def k: Int = centers.length

  override def toString: String =
    s"KMeansModel(k=$k, cost=$cost, iterations=$iterations)"
}

/**
 * Échantillonnage pondéré, factorisé ici parce qu'il sert à trois endroits :
 * le premier centre de k-means++, le premier représentant du coreset tree, et
 * l'échantillonnage D² à l'intérieur d'une feuille.
 */
private[streamkm] object Sampling {

  /**
   * Tire un indice dans [0, n) avec une probabilité proportionnelle à `w(i)`.
   * Si la masse totale est nulle (ou négative par erreur d'arrondi), retombe sur
   * un tirage uniforme plutôt que de lever une exception : ce cas se produit
   * légitimement quand tous les points d'une feuille sont confondus.
   */
  def byWeight(n: Int, w: Int => Double, rng: Random): Int = {
    var total = 0.0
    var i     = 0
    while (i < n) { total += w(i); i += 1 }

    if (!(total > 0.0)) return rng.nextInt(n)

    val target = rng.nextDouble() * total
    var acc    = 0.0
    i = 0
    while (i < n) {
      acc += w(i)
      if (acc >= target) return i
      i += 1
    }
    // Atteignable uniquement par accumulation d'erreurs flottantes.
    n - 1
  }
}

object KMeans {

  /**
   * Procédure de seeding de k-means++ (thèse, algorithme 2.2, lignes 1-3), **version pondérée**.
   *
   * Différence assumée avec la thèse : la ligne 1 de l'algorithme 2.2 choisit le premier
   * centre « uniformly at random ». On tire ici proportionnellement au poids. Sur des points
   * bruts (tous de poids 1) les deux coïncident exactement ; sur un coreset (poids très
   * hétérogènes) le tirage uniforme biaiserait l'initialisation vers les régions peu denses.
   * Or dans StreamKM++, k-means++ est TOUJOURS appliqué à un coreset (§2.3.4, étape 2) —
   * la version pondérée est donc la bonne. À mentionner dans le rapport.
   *
   * Coût : O(n·k·d).
   */
  def seedPlusPlus(points: Array[Point], k: Int, rng: Random): Array[Array[Double]] = {
    require(points.nonEmpty, "seedPlusPlus: ensemble de points vide")
    require(k > 0, s"seedPlusPlus: k doit être > 0 (reçu $k)")

    val n       = points.length
    val kEff    = math.min(k, n)
    val centers = new Array[Array[Double]](kEff)

    centers(0) = points(Sampling.byWeight(n, i => points(i).weight, rng)).coords.clone()

    // closest(i) = distance au carré de points(i) au plus proche centre déjà choisi.
    // Mis à jour incrémentalement : c'est ce qui donne O(n·k·d) et non O(n·k²·d).
    val closest = new Array[Double](n)
    var i       = 0
    while (i < n) { closest(i) = Distance.sqdist(points(i).coords, centers(0)); i += 1 }

    var c = 1
    while (c < kEff) {
      // Tirage D² pondéré : P(i) ∝ w_i · closest(i)
      val idx = Sampling.byWeight(n, j => points(j).weight * closest(j), rng)
      centers(c) = points(idx).coords.clone()

      i = 0
      while (i < n) {
        val d = Distance.sqdist(points(i).coords, centers(c))
        if (d < closest(i)) closest(i) = d
        i += 1
      }
      c += 1
    }
    centers
  }

  /**
   * Algorithme de Lloyd pondéré (thèse, algorithme 2.1).
   *
   * Le centre d'un cluster est la moyenne PONDÉRÉE de ses points :
   *   μ_c = Σ_{p ∈ c} w_p · p  /  Σ_{p ∈ c} w_p
   * Oublier les poids ici est le bug classique de StreamKM++ : le code tourne, ne plante
   * jamais, et produit des centres silencieusement faux dès qu'on l'applique à un coreset.
   *
   * Clusters vides : on conserve le centre précédent. C'est le choix le plus simple et il
   * ne dégrade pas le coût (un cluster vide contribue 0). Alternative classique — ré-amorcer
   * sur le point le plus éloigné — écartée : elle rend le coût non monotone et casse
   * l'assertion de convergence.
   *
   * Arrêt : décroissance relative du coût inférieure à `tol`, ou `maxIter` atteint.
   */
  def lloyd(
    points:  Array[Point],
    init:    Array[Array[Double]],
    maxIter: Int    = 100,
    tol:     Double = 1e-6
  ): KMeansModel = {
    require(points.nonEmpty, "lloyd: ensemble de points vide")
    require(init.nonEmpty, "lloyd: aucun centre initial")

    val k       = init.length
    val d       = points(0).dim
    val centers = Array.tabulate(k)(i => init(i).clone())

    val sums   = Array.ofDim[Double](k, d)
    val masses = new Array[Double](k)

    var prevCost = Double.MaxValue
    var it       = 0
    var done     = false

    while (!done && it < maxIter) {
      var c = 0
      while (c < k) {
        java.util.Arrays.fill(sums(c), 0.0)
        masses(c) = 0.0
        c += 1
      }

      // Étape d'affectation + accumulation des sommes pondérées, en un seul parcours.
      // `currCost` est la SSE des centres COURANTS (avant mise à jour) : c'est bien la
      // quantité dont la décroissance prouve la convergence de Lloyd.
      var currCost = 0.0
      var i        = 0
      while (i < points.length) {
        val p        = points(i)
        val (idx, dd) = Distance.nearest(p.coords, centers)
        currCost += p.weight * dd
        val s = sums(idx)
        val w = p.weight
        var j = 0
        while (j < d) { s(j) += w * p.coords(j); j += 1 }
        masses(idx) += w
        i += 1
      }

      // Étape de mise à jour des centres.
      c = 0
      while (c < k) {
        if (masses(c) > 0.0) {
          val s   = sums(c)
          val m   = masses(c)
          val ctr = centers(c)
          var j   = 0
          while (j < d) { ctr(j) = s(j) / m; j += 1 }
        }
        c += 1
      }

      it += 1
      // Invariant de Lloyd : le coût ne remonte jamais.
      assert(currCost <= prevCost + 1e-9 * math.abs(prevCost),
             s"Lloyd: coût non monotone ($prevCost -> $currCost) — bug d'affectation ou de pondération")

      if (prevCost - currCost <= tol * math.max(math.abs(prevCost), 1e-300)) done = true
      prevCost = currCost
    }

    // Recalcul final : `prevCost` correspond aux centres d'AVANT la dernière mise à jour.
    KMeansModel(centers, cost(points, centers), it)
  }

  /**
   * k-means++ complet : `nRestarts` exécutions indépendantes (seeding + Lloyd), on garde
   * la moins coûteuse. La thèse et le papier original utilisent 5 (§2.3.4, étape 2), mais
   * le §5.3.1 montre qu'une seule application suffit en pratique — c'est un paramètre
   * d'expérience (E3), pas une constante.
   */
  def fit(
    points:    Array[Point],
    k:         Int,
    nRestarts: Int    = 5,
    maxIter:   Int    = 100,
    tol:       Double = 1e-6,
    seed:      Long   = 42L
  ): KMeansModel = {
    require(nRestarts > 0, s"fit: nRestarts doit être > 0 (reçu $nRestarts)")
    val rng  = new Random(seed)
    var best: KMeansModel = null
    var r    = 0
    while (r < nRestarts) {
      val model = lloyd(points, seedPlusPlus(points, k, rng), maxIter, tol)
      if (best == null || model.cost < best.cost) best = model
      r += 1
    }
    best
  }

  /**
   * Fonction objectif : SSE pondérée, Σ_p w_p · min_c ‖p − c‖².
   * C'est la mesure de qualité unique de tout le projet (thèse §4.2) : tous les
   * chapitres expérimentaux comparent des valeurs produites par cette fonction.
   */
  def cost(points: Array[Point], centers: Array[Array[Double]]): Double = {
    var s = 0.0
    var i = 0
    while (i < points.length) {
      val p = points(i)
      s += p.weight * Distance.nearest(p.coords, centers)._2
      i += 1
    }
    s
  }
}
