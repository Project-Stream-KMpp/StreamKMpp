package streamkm.coreset

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import streamkm.core.Point

/**
 * Technique « merge-and-reduce » de StreamKM++ (thèse §2.3.4, algorithme 2.5).
 *
 * INVARIANTS (thèse §2.3.4, propriétés 1 et 2) :
 *   - `bucket(0)` est le tampon d'entrée : entre 0 et m points.
 *   - `bucket(i)` pour i ≥ 1 est soit VIDE, soit exactement de taille m.
 *   - un `bucket(i)` plein pour i ≥ 1 représente exactement 2^(i-1) · m points du flux.
 *   - le nombre de buckets croît en O(log(n/m)) : on n'a pas besoin de connaître n a priori,
 *     les niveaux sont créés à la demande.
 *
 * Ces invariants sont vérifiés par `checkInvariants()`, appelé sous `assert` (donc actif
 * seulement avec -ea, cf. build.sbt).
 *
 * Cette classe est l'état que la thèse maintient dans chaque subtask parallèle de
 * `FlatMapToPartialCoresets` (§4.1). Côté Spark, c'est l'accumulateur d'un `treeAggregate`
 * (E4) : `seqOp = insert`, `combOp = merge`.
 *
 * NON THREAD-SAFE — un `BucketManager` appartient à une seule partition.
 */
final class BucketManager(val m: Int, val seed: Long) extends Serializable {
  require(m > 0, s"BucketManager: m doit être > 0 (reçu $m)")

  private val rng                                     = new Random(seed)
  private val buckets: ArrayBuffer[ArrayBuffer[Point]] = ArrayBuffer(new ArrayBuffer[Point](m))
  private var seen: Double                            = 0.0
  private var collapsed: Boolean                      = false

  /** Masse totale consommée depuis la création (Σ des poids insérés). */
  def numPointsSeen: Double = seen

  /** Nombre de niveaux actuellement alloués (≈ log2(n/m) + 2). */
  def numLevels: Int = buckets.length

  /**
   * Insère un point dans le flux (algorithme 2.5).
   *
   * Déroulé, à partir de la description en prose du §2.3.4 :
   *  - le point va toujours dans B0 ;
   *  - si B0 est plein et B1 vide, on déplace B0 dans B1 (aucun coreset n'est calculé,
   *    B1 contient m points bruts et représente donc bien 2^0 · m points) ;
   *  - si B1 est plein, on construit un coreset de B0 ∪ B1 (2m points → m points) et on
   *    le range dans B2 ; si B2 est plein, on recommence avec B2, et ainsi de suite jusqu'à
   *    trouver un niveau libre.
   */
  def insert(p: Point): Unit = {
    if (collapsed)
      throw new IllegalStateException(
        "BucketManager: insertion interdite après merge(). Un manager fusionné est un " +
        "résultat de combOp, il n'est plus alimenté par le flux."
      )

    buckets(0) += p
    seen += p.weight

    if (buckets(0).length >= m) {
      ensureLevel(1)
      if (buckets(1).isEmpty) {
        // B1 vide : simple déplacement, pas de réduction.
        buckets(1) ++= buckets(0)
        buckets(0).clear()
      } else {
        // B1 plein : cascade de fusions-réductions.
        var union: Array[Point] = (buckets(0) ++ buckets(1)).toArray
        buckets(0).clear()
        buckets(1).clear()

        var level = 2
        var placed = false
        while (!placed) {
          val reduced = CoresetTree.build(union, m, rng)
          ensureLevel(level)
          if (buckets(level).isEmpty) {
            buckets(level) ++= reduced
            placed = true
          } else {
            union = reduced ++ buckets(level)
            buckets(level).clear()
            level += 1
          }
        }
      }
      assert(checkInvariants(), "BucketManager: invariants violés après insert")
    }
  }

  /** Insertion en lot. Renvoie `this` pour pouvoir chaîner dans un `seqOp`. */
  def insertAll(it: Iterator[Point]): this.type = {
    while (it.hasNext) insert(it.next())
    this
  }

  def insertAll(pts: Array[Point]): this.type = insertAll(pts.iterator)

  /**
   * Union de tous les buckets non vides, SANS réduction.
   *
   * C'est le « final coreset » au sens du §2.3.4 : au plus m·(log₂(n/m) + 2) points
   * pondérés. C'est cet ensemble que la thèse passe à k-means++ à l'étape 2 de
   * StreamKM++.
   */
  def union(): Array[Point] = {
    val out = new ArrayBuffer[Point]()
    var i   = 0
    while (i < buckets.length) { out ++= buckets(i); i += 1 }
    out.toArray
  }

  /**
   * Union de tous les buckets, RÉDUITE à m points par le coreset tree.
   *
   * C'est le « partial coreset » du §4.1 : ce que chaque subtask parallèle émet en fin de
   * flux. Ne vide pas l'état — on peut donc interroger le clustering courant sans
   * interrompre la consommation (c'est ce qui rendra possible la variante à requêtes
   * périodiques du §4.3, étape E8).
   */
  def extractCoreset(): Array[Point] = CoresetTree.build(union(), m, rng)

  /**
   * Fusion de deux structures indépendantes, pour le `combOp` d'un `treeAggregate`.
   *
   * Repose sur les deux observations du §2.3.4 :
   *   1. l'union de deux ε-coresets d'ensembles disjoints est un ε-coreset de l'union ;
   *   2. un ε-coreset d'un δ-coreset est un (ε + δ + εδ)-coreset.
   * La seconde explique que l'erreur s'accumule à chaque niveau de fusion — c'est
   * précisément pourquoi on utilise `treeAggregate` avec une profondeur limitée (2 par
   * défaut) plutôt qu'une réduction linéaire : la profondeur de l'arbre de fusion borne
   * le nombre d'empilements. À discuter dans le rapport.
   *
   * Le résultat est « collapsé » : son état n'est plus un empilement de buckets valide
   * au sens des invariants, mais un unique coreset de taille m. Toute insertion ultérieure
   * est refusée. C'est sans conséquence pour `treeAggregate`, dont le `combOp` ne reçoit
   * jamais que des résultats de `seqOp` ou de `combOp`, jamais de points bruts.
   *
   * Ne modifie ni `this` ni `other`.
   */
  def merge(other: BucketManager): BucketManager = {
    require(other.m == m, s"merge: tailles de coreset incompatibles (${m} vs ${other.m})")

    val out = new BucketManager(m, seed ^ other.seed)
    val all = union() ++ other.union()

    out.buckets(0).clear()
    out.ensureLevel(1)
    out.buckets(1) ++= CoresetTree.build(all, m, rng)
    out.seen      = seen + other.seen
    out.collapsed = true
    out
  }

  // ------------------------------------------------------------------

  private def ensureLevel(level: Int): Unit =
    while (buckets.length <= level) buckets += new ArrayBuffer[Point](m)

  /**
   * Vérification des invariants du §2.3.4. Utilisée sous `assert` : coûteuse et
   * désactivée en production.
   */
  private[coreset] def checkInvariants(): Boolean = {
    if (buckets(0).length > m) return false
    var i = 1
    while (i < buckets.length) {
      val s = buckets(i).length
      if (s != 0 && s != m) return false
      i += 1
    }
    true
  }

  override def toString: String =
    s"BucketManager(m=$m, seen=$seen, levels=${buckets.map(_.length).mkString("[", ",", "]")}" +
      (if (collapsed) ", collapsed" else "") + ")"
}

object BucketManager {

  /**
   * Taille de coreset recommandée par la thèse (§2.3.4, clarification 1) et confirmée
   * expérimentalement au §5.2.1 : m = 200·k est un bon compromis temps / qualité.
   * C'est la valeur par défaut de nos expériences, pas une contrainte de l'algorithme.
   */
  def recommendedM(k: Int): Int = 200 * k

  def apply(m: Int, seed: Long = 42L): BucketManager = new BucketManager(m, seed)
}
