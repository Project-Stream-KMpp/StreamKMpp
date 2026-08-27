package streamkm.coreset

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import streamkm.core.{PointsSoA, Distance, Sampling}

/**
 * Nœud de l'arbre de coreset.
 *
 * NOTE DE NOMMAGE — importante, à reprendre dans le rapport.
 * La thèse appelle `weight(v)` l'attribut servant à la descente dans l'arbre, et le définit
 * pour une feuille comme `cost(P_v, q_v)`, c'est-à-dire une SOMME DE DISTANCES AU CARRÉ.
 * Mais elle appelle aussi « weight » le poids des points du coreset produit, qui vaut le
 * NOMBRE (la masse) de points représentés. Ce sont deux quantités totalement différentes
 * portant le même nom. On les sépare explicitement :
 *   - `cost` : Σ_{p ∈ P_v} w_p · ‖p − q_v‖²   → sert à la descente (étape 1 du §2.3.3)
 *   - `mass` : Σ_{p ∈ P_v} w_p                → sert au poids du point de coreset en sortie
 *
 * @param points feuille : les points du cluster. Nœud interne : null.
 * @param repr   point représentatif q_v (stocké aussi sur les nœuds internes, il devient
 *               le représentant du fils gauche lors d'une scission).
 */
private[coreset] final class Node(
  var pointIndices: Array[Int], //indices in set of PointsSoA
  var representativeIndex: Int,
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
   * Construit un coreset d'au plus `m` points à partir de `store` (le store ENTIER
   * est réduit — pour réduire un sous-ensemble, le caller doit d'abord construire
   * un `PointsSoA` de ce sous-ensemble, cf. `PointsSoA.concat` dans BucketManager).
   *
   * Le store `store` passé en entrée est CONSOMMÉ : cette fonction en prend possession
   * et appelle `store.free()` avant de retourner, une fois les représentants copiés
   * dans le store de sortie. Le caller ne doit plus l'utiliser après l'appel.
   */
  def build(setOfPoints: PointsSoA, coresetSize: Int, range: Random): PointsSoA = {
    require(coresetSize > 0, s"build: coresetSize doit être > 0 (reçu $coresetSize)")
    if (setOfPoints.n <= coresetSize){
      return setOfPoints
    }

    val allIndices = Array.range(0, setOfPoints.n)
    val rootRepresentativeIndex = Sampling.byWeight(setOfPoints.n, setOfPoints.weight, range)
    val root = new Node(allIndices, rootRepresentativeIndex, 0.0, 0.0, null, null)
    recomputeLeaf(root, setOfPoints)

    var leafCount = 1
    val pathFromRoot = new ArrayBuffer[Node](32)

    while (leafCount < coresetSize && root.cost > 0.0) {
      pathFromRoot.clear()
      val chosenLeaf = descend(root, range, pathFromRoot)
      val newRepresentativeIndex = sampleD2(chosenLeaf, setOfPoints, range)

      if (newRepresentativeIndex < 0){
        val result = collectLeaves(root, setOfPoints, coresetSize)
        setOfPoints.free()
        return result
      }

      if (!split(chosenLeaf, newRepresentativeIndex, setOfPoints)){
        val result = collectLeaves(root, setOfPoints, coresetSize)
        setOfPoints.free()
        return result
      }
      var pathIndex = pathFromRoot.length - 2
      while (pathIndex >= 0){
        recomputeInner(pathFromRoot(pathIndex))
        pathIndex -= 1
      }
      leafCount += 1
    }

    val result = collectLeaves(root, setOfPoints, coresetSize)
    setOfPoints.free()
    result
  }

  // ------------------------------------------------------------------
  // Étapes internes (numérotées comme au §2.3.3)
  // ------------------------------------------------------------------

  /**
   * Étape 1 : choisir une feuille avec une probabilité proportionnelle à son coût.
   *
   * ⚠️ AMBIGUÏTÉ DE LA SOURCE, tranchée ici et à signaler dans le rapport.
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
  private def descend(root: Node, range: Random, pathFromRoot: ArrayBuffer[Node]): Node = {
    var currentNode = root
    pathFromRoot += currentNode

    while (!currentNode.isLeaf) {
      val totalCost = currentNode.cost
      currentNode =
        if (!(totalCost > 0.0))
          currentNode.left // sécurité : coût nul, choix arbitraire
        else if (range.nextDouble() * totalCost < currentNode.left.cost)
          currentNode.left
        else
          currentNode.right
      pathFromRoot += currentNode
    }
    currentNode
  }

  /**
   * Étape 2 : dans la feuille, tirer le prochain représentant avec une probabilité
   * proportionnelle à sa distance au carré au représentant courant (échantillonnage D²).
   * Renvoie null si la feuille est de coût nul.
   */
  private def sampleD2(leaf: Node, setOfPoints: PointsSoA, rng: Random): Int = {
    if (!(leaf.cost > 0.0)) return -1
    val indices = leaf.pointIndices
    val pickedPosition = Sampling.byWeight(
      indices.length,
      position => setOfPoints.weight(indices(position)) * Distance.squareDistance(setOfPoints, indices(position), leaf.representativeIndex),
      rng
    )
    val chosenIndex = indices(pickedPosition)
    if (Distance.squareDistance(setOfPoints, chosenIndex, leaf.representativeIndex) <= 0.0) -1 else chosenIndex
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
  private def split(leaf: Node, newRepresentativeIndex: Int, store: PointsSoA): Boolean = {
    val indices        = leaf.pointIndices
    val leftIndices  = new ArrayBuffer[Int](indices.length)
    val rightIndices = new ArrayBuffer[Int](indices.length)

    var position = 0
    while (position < indices.length) {
      val pointIndex = indices(position)
      if (Distance.squareDistance(store, pointIndex, leaf.representativeIndex) <= Distance.squareDistance(store, pointIndex, newRepresentativeIndex))
        leftIndices += pointIndex
      else
        rightIndices += pointIndex
      position += 1
    }
    if (leftIndices.isEmpty || rightIndices.isEmpty) return false

    val leftChild  = new Node(leftIndices.toArray,  leaf.representativeIndex, 0.0, 0.0, null, null)
    val rightChild = new Node(rightIndices.toArray, newRepresentativeIndex,   0.0, 0.0, null, null)
    recomputeLeaf(leftChild, store); recomputeLeaf(rightChild, store)

    leaf.pointIndices = null; leaf.left = leftChild; leaf.right = rightChild
    recomputeInner(leaf)
    true
  }

  private def recomputeLeaf(node: Node, setOfPoints: PointsSoA): Unit = {
    var cost = 0.0; var mass = 0.0
    val indices = node.pointIndices
    var position = 0
    while (position < indices.length) {
      val pointIndex = indices(position)
      cost += setOfPoints.weight(pointIndex) * Distance.squareDistance(setOfPoints, pointIndex, node.representativeIndex)
      mass += setOfPoints.weight(pointIndex)
      position += 1
    }
    node.cost = cost; node.mass = mass
  }

  private def recomputeInner(node: Node): Unit = {
    node.cost = node.left.cost + node.right.cost
    node.mass = node.left.mass + node.right.mass
  }

  private def collectLeaves(root: Node, setOfPoints: PointsSoA, coresetSize: Int): PointsSoA = {
    val output    = PointsSoA.allocate(coresetSize, setOfPoints.dimension)
    val tmp       = new Array[Double](setOfPoints.dimension)
    val nodeStack = new ArrayBuffer[Node](); nodeStack += root
    while (nodeStack.nonEmpty) {
      val node = nodeStack.remove(nodeStack.length - 1)
      if (node.isLeaf) { setOfPoints.copyCoordinatesInto(node.representativeIndex, tmp); output.append(tmp, node.mass) }
      else { nodeStack += node.left; nodeStack += node.right }
    }
    output
  }
}
