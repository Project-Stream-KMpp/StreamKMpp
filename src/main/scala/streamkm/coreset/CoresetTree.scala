package streamkm.coreset

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import streamkm.core.{Distance, Point, Sampling}

/**
 * Nœud de l'arbre de coreset.
 *
 * NOTE DE NOMMAGE — importante, à reprendre dans le rapport.
 * La thèse appelle `weight(v)` l'attribut servant à la descente dans l'arbre, et le définit
 * pour une feuille comme `cost(P_v, q_v)`, c'est-à-dire une SOMME DE DISTANCES AU CARRÉ.
 * Mais elle appelle aussi « weight » le poids des points du coreset produit, qui vaut le
 * NOMBRE (la masse) de points représentés. Ce sont deux quantités totalement différentes
 * portant le même nom. On les sépare explicitement :
 *   - `cost` : sum_{p in P_v} w_p * ||p - q_v||^2
*              sert a la descente
*
*   - `mass` : sum_{p in P_v} w_p
*              sert au poids du point de coreset en sortie
 *
 * @param points feuille : les points du cluster. Nœud interne : null.
 * @param repr   point représentatif q_v (stocké aussi sur les nœuds internes, il devient
 *               le représentant du fils gauche lors d'une scission).
 */
private[coreset] final class Node(
  var points: Array[Point],
  var repr:   Array[Double],
  var cost:   Double,
  var mass:   Double,
  var left:   Node,
  var right:  Node
) {
  def isLeaf: Boolean = left == null
}

/**
 * Construction du coreset par arbre binaire de clustering divisif hiérarchique
 * (thèse §2.3.3 « Coreset Tree » et algorithme 2.4).
 *
 * Complexité : O(m · n) dans le pire cas, mais O(n · log m) en pratique, contre O(n · m)
 * pour l'AdaptiveCoreset de l'algorithme 2.3 — c'est toute la raison d'être de cette
 * structure.
 */
object CoresetTree {

  /**
   * Construit un coreset d'au plus `m` points pondérés à partir de `points`.
   *
   * GARANTIE STRUCTURELLE : la somme des poids de sortie est exactement égale à la somme
   * des poids d'entrée. C'est l'invariant que testent en priorité les tests de E2 — il
   * attrape la grande majorité des erreurs d'implémentation de cette structure.
   *
   * Si `points.length <= m`, l'ensemble est déjà son propre coreset : on le renvoie tel quel.
   */
  def build(points: Array[Point], m: Int, rng: Random): Array[Point] = {
    require(m > 0, s"build: m doit être > 0 (reçu $m)")
    if (points.length <= m) return points.clone()

    // --- Racine : premier représentant tiré proportionnellement au poids ---
    // (Thèse algo 2.4 ligne 1 : « uniformly at random ». Même remarque que pour
    // KMeans.seedPlusPlus : sur un ensemble pondéré, le tirage proportionnel à la masse
    // est la généralisation correcte, et les deux coïncident sur des points de poids 1.)
    val i0   = Sampling.byWeight(points.length, i => points(i).weight, rng)
    val root = new Node(points, points(i0).coords.clone(), 0.0, 0.0, null, null)
    recomputeLeaf(root)

    var leaves = 1
    val path   = new ArrayBuffer[Node](32)

    // Condition d'arrêt supplémentaire par rapport à l'algorithme 2.4 : `root.cost > 0`.
    // Si le coût global est nul, tous les points sont confondus avec leur représentant :
    // aucune scission ne peut plus rien améliorer, et l'échantillonnage D² serait dégénéré.
    // Ce cas arrive réellement sur des données discrètes très redondantes.
    while (leaves < m && root.cost > 0.0) {
      path.clear()
      val leaf = descend(root, rng, path)
      val q    = sampleD2(leaf, rng)

      if (q == null) return collectLeaves(root) // feuille de coût nul : sécurité
      if (!split(leaf, q)) return collectLeaves(root) // scission dégénérée : sécurité

      // Remontée de la mise à jour jusqu'à la racine (algo 2.4 ligne 9).
      // `leaf` est le dernier élément du chemin et a déjà été mis à jour par `split`.
      var i = path.length - 2
      while (i >= 0) { recomputeInner(path(i)); i -= 1 }

      leaves += 1
    }

    collectLeaves(root)
  }

  // ------------------------------------------------------------------
  // Étapes internes (numérotées comme au §2.3.3)
  // ------------------------------------------------------------------

  /**
   * Étape 1 : choisir une feuille avec une probabilité proportionnelle à son coût.
   *
   * 
   * Le §2.3.3 étape 1 dit « with a probability proportional to cost(P_ℓ, q_ℓ) », et détaille
   * une descente où un fils est choisi avec probabilité cost(fils)/cost(père). La ligne 5 de
   * l'algorithme 2.4 dit en revanche « according to their weights ». Les deux formulations
   * s'accordent si l'on comprend « weight(v) » au sens de la définition donnée juste avant
   * l'algorithme (weight d'une feuille = cost(P_v, q_v)) — ce que confirme le papier original
   * d'Ackermann et al. C'est donc bien le COÛT qui pilote la descente.
   *
   * Le raisonnement le confirme : une feuille déjà bien représentée (coût faible) ne doit
   * plus être scindée ; l'effort de raffinement doit aller là où l'erreur est grande. Utiliser
   * la masse à la place ferait dériver l'algorithme vers un simple échantillonnage
   * proportionnel à la densité, et lui ferait perdre la propriété de coreset.
   *
   * `path` reçoit le chemin racine → feuille, pour la remontée.
   */
  private def descend(root: Node, rng: Random, path: ArrayBuffer[Node]): Node = {
    var node = root
    path += node
    while (!node.isLeaf) {
      val total = node.cost
      node =
        if (!(total > 0.0)) node.left // sécurité : coût nul, choix arbitraire
        else if (rng.nextDouble() * total < node.left.cost) node.left
        else node.right
      path += node
    }
    node
  }

  /**
   * Étape 2 : dans la feuille, tirer le prochain représentant avec une probabilité
   * proportionnelle à sa distance au carré au représentant courant (échantillonnage D²).
   * Renvoie null si la feuille est de coût nul.
   */
  private def sampleD2(leaf: Node, rng: Random): Array[Double] = {
    if (!(leaf.cost > 0.0)) return null
    val pts = leaf.points
    val idx = Sampling.byWeight(
      pts.length,
      i => pts(i).weight * Distance.sqdist(pts(i).coords, leaf.repr),
      rng
    )
    val chosen = pts(idx).coords
    // Si le tirage retombe sur le représentant lui-même (distance nulle), la scission
    // produirait un fils vide. Ne peut arriver que par erreur d'arrondi ; on refuse.
    if (Distance.sqdist(chosen, leaf.repr) <= 0.0) null else chosen.clone()
  }

  /**
   * Étape 3 : scinder la feuille en deux fils selon la proximité à `repr` et à `q`,
   * puis mettre à jour les attributs du nœud devenu interne.
   *
   * Les deux fils sont nécessairement non vides : `repr` est à distance 0 de lui-même
   * (donc à gauche) et `q` est à distance 0 de lui-même et à distance > 0 de `repr`
   * (donc à droite). Le `false` de retour est une sécurité contre une violation de
   * cette propriété par erreur numérique.
   */
  private def split(leaf: Node, q: Array[Double]): Boolean = {
    val pts   = leaf.points
    val left  = new ArrayBuffer[Point](pts.length)
    val right = new ArrayBuffer[Point](pts.length)

    var i = 0
    while (i < pts.length) {
      val p = pts(i)
      if (Distance.sqdist(p.coords, leaf.repr) <= Distance.sqdist(p.coords, q)) left += p
      else right += p
      i += 1
    }

    if (left.isEmpty || right.isEmpty) return false

    val l = new Node(left.toArray, leaf.repr, 0.0, 0.0, null, null)
    val r = new Node(right.toArray, q, 0.0, 0.0, null, null)
    recomputeLeaf(l)
    recomputeLeaf(r)

    leaf.points = null
    leaf.left   = l
    leaf.right  = r
    recomputeInner(leaf)
    true
  }

  private def recomputeLeaf(n: Node): Unit = {
    var cost = 0.0
    var mass = 0.0
    val pts  = n.points
    var i    = 0
    while (i < pts.length) {
      val p = pts(i)
      cost += p.weight * Distance.sqdist(p.coords, n.repr)
      mass += p.weight
      i += 1
    }
    n.cost = cost
    n.mass = mass
  }

  private def recomputeInner(n: Node): Unit = {
    n.cost = n.left.cost + n.right.cost
    n.mass = n.left.mass + n.right.mass
  }

  /**
   * L'union des représentants des feuilles constitue le coreset. Le poids de chacun est
   * la masse totale des points de sa feuille — d'où la conservation de la masse totale.
   */
  private def collectLeaves(root: Node): Array[Point] = {
    val out   = new ArrayBuffer[Point]()
    val stack = new ArrayBuffer[Node]()
    stack += root
    while (stack.nonEmpty) {
      val n = stack.remove(stack.length - 1)
      if (n.isLeaf) out += new Point(n.repr, n.mass)
      else { stack += n.left; stack += n.right }
    }
    out.toArray
  }
}
