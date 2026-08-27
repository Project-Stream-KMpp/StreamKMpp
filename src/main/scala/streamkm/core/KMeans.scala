package streamkm.core

import scala.util.Random

/** Résultat d'un clustering : les centres, la SSE pondérée atteinte, le nombre d'itérations. */
final case class KMeansModel(
                              centers:    PointsSoA,
                              cost:       Double,
                              iterations: Int
                            ) {
  def k: Int = centers.n
  override def toString: String = s"KMeansModel(k=$k, cost=$cost, iterations=$iterations)"
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
  def seedPlusPlus(points: PointsSoA, k: Int, rng: Random): PointsSoA = {
    require(points.n > 0, "seedPlusPlus: ensemble de points vide")
    require(k > 0, s"seedPlusPlus: k doit être > 0 (reçu $k)")

    val n    = points.n
    val kEff = math.min(k, n)
    val centers = PointsSoA.allocate(kEff, points.dimension)
    val tmp     = new Array[Double](points.dimension)

    val i0 = Sampling.byWeight(n, points.weight, rng)
    points.copyCoordinatesInto(i0, tmp)
    centers.append(tmp, points.weight(i0))

    // closest(i) = distance au carré de points(i) au plus proche centre déjà choisi.
    val closest = new Array[Double](n)
    var i = 0
    while (i < n) { closest(i) = Distance.squareDistance(points, i, centers, 0); i += 1 }

    var c = 1
    while (c < kEff) {
      val idx = Sampling.byWeight(n, j => points.weight(j) * closest(j), rng)
      points.copyCoordinatesInto(idx, tmp)
      centers.append(tmp, points.weight(idx))

      i = 0
      while (i < n) {
        val d = Distance.squareDistance(points, i, centers, c)
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
             points:  PointsSoA,
             init:    PointsSoA,
             maxIter: Int    = 100,
             tol:     Double = 1e-6
           ): KMeansModel = {
    require(points.n > 0, "lloyd: ensemble de points vide")
    require(init.n > 0, "lloyd: aucun centre initial")

    val k = init.n
    val dimension = points.dimension
    val centers = PointsSoA.allocate(k, dimension)
    val tmp     = new Array[Double](dimension)
    var c = 0
    while (c < k) { init.copyCoordinatesInto(c, tmp); centers.append(tmp, init.weight(c)); c += 1 }
    init.free()   // ← init entièrement copié dans centers, plus jamais utilisé après ce point

    val clusterSums = Array.ofDim[Double](k, dimension)
    val masses      = new Array[Double](k)
    val nearestResult = new NearestResult(0, 0.0)

    var prevCost = Double.MaxValue
    var it       = 0
    var done     = false

    while (!done && it < maxIter) {
      c = 0
      while (c < k) {
        java.util.Arrays.fill(clusterSums(c), 0.0)
        masses(c) = 0.0
        c += 1
      }

      var currCost = 0.0
      var i = 0
      while (i < points.n) {
        Distance.nearest(points, i, centers, nearestResult)
        val idx    = nearestResult.index
        val weight = points.weight(i)
        currCost += weight * nearestResult.distance

        val clusterSum = clusterSums(idx)
        var j = 0
        while (j < dimension) { clusterSum(j) += weight * points.coordinates(i, j); j += 1 }
        masses(idx) += weight
        i += 1
      }

      c = 0
      while (c < k) {
        if (masses(c) > 0.0) {
          val clusterSum = clusterSums(c)
          val mass       = masses(c)
          var j = 0
          while (j < dimension) { tmp(j) = clusterSum(j) / mass; j += 1 }
          centers.set(c, tmp, centers.weight(c))
        }
        c += 1
      }

      it += 1
      assert(currCost <= prevCost + 1e-9 * math.abs(prevCost),
        s"Lloyd: coût non monotone ($prevCost -> $currCost)")

      if (prevCost - currCost <= tol * math.max(math.abs(prevCost), 1e-300)) done = true
      prevCost = currCost
    }

    KMeansModel(centers, cost(points, centers), it)
  }

  /**
   * k-means++ complet : `nRestarts` exécutions indépendantes (seeding + Lloyd), on garde
   * la moins coûteuse. La thèse et le papier original utilisent 5 (§2.3.4, étape 2), mais
   * le §5.3.1 montre qu'une seule application suffit en pratique — c'est un paramètre
   * d'expérience (E3), pas une constante.
   */
  def fit(
           points:    PointsSoA,
           k:         Int,
           nRestarts: Int    = 5,
           maxIter:   Int    = 100,
           tol:       Double = 1e-6,
           seed:      Long   = 42L
         ): KMeansModel = {
    require(nRestarts > 0, s"fit: nRestarts doit être > 0 (reçu $nRestarts)")
    val rng = new Random(seed)
    var best: KMeansModel = null
    var r = 0
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
  def cost(points: PointsSoA, centers: PointsSoA): Double = {
    val nearestResult = new NearestResult(0, 0.0)
    var totalCost = 0.0
    var i = 0
    while (i < points.n) {
      Distance.nearest(points, i, centers, nearestResult)
      totalCost += points.weight(i) * nearestResult.distance
      i += 1
    }
    totalCost
  }
}
