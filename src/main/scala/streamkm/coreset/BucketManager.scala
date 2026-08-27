package streamkm.coreset

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import streamkm.core.PointsSoA

final class BucketManager(val coresetSize: Int, val dimension: Int, val seed: Long) {
  private val rng = new Random(seed)
  // buckets(0) : tampon d'entrée, capacité coresetSize, rempli au fil de insert().
  private val buckets: ArrayBuffer[PointsSoA] = ArrayBuffer(PointsSoA.allocate(coresetSize, dimension))
  private var totalWeightSeen: Double = 0.0
  private var collapsed: Boolean = false

  def insert(point: Array[Double], weight: Double): Unit = {
    if (collapsed)
      throw new IllegalStateException(
        "BucketManager: insertion interdite après merge(). Un manager fusionné est un " +
          "résultat de combOp, il n'est plus alimenté par le flux."
      )
    buckets(0).append(point, weight)

    totalWeightSeen += weight

    if (buckets(0).n >= coresetSize) {
      ensureLevel(1)
      if (buckets(1).n == 0) {
        // Déplacement pur : le contenu de buckets(0) devient buckets(1), sans réduction.
        val movedBucket = buckets(0)
        buckets(0) = PointsSoA.allocate(coresetSize, dimension)
        buckets(1) = movedBucket
      } else {
        var pendingUnion: PointsSoA = PointsSoA.concat(buckets(0), buckets(1))
        buckets(0).free(); buckets(0) = PointsSoA.allocate(coresetSize, dimension)
        buckets(1).free(); buckets(1) = PointsSoA.allocate(coresetSize, dimension)

        var level = 2
        var placed = false
        while (!placed) {
          val reducedCoreset = CoresetTree.build(pendingUnion, coresetSize, rng) // consomme et libère `pendingUnion`
          ensureLevel(level)
          if (buckets(level).n == 0) { buckets(level) = reducedCoreset; placed = true }
          else {
            pendingUnion = PointsSoA.concat(reducedCoreset, buckets(level))
            reducedCoreset.free()
            buckets(level).free(); buckets(level) = PointsSoA.allocate(coresetSize, dimension)
            level += 1
          }
        }
      }
    }
  }

  /**
   * Fusion de deux structures indépendantes — combOp pour un usage hors Spark
   * (tests, démonstrateur). Repose sur les mêmes observations que la version AoS
   * (thèse §2.3.4) : union de deux ε-coresets disjoints = ε-coreset de l'union ;
   * composition multiplicative de l'erreur à chaque niveau. Graine combinée par
   * XOR — même principe que StreamKMPlusPlus.mergeCoresets (indépendance de
   * l'aléa à chaque étage de fusion, CLAUDE.md décision 10).
   *
   * Ne modifie ni `this` ni `other` — leurs stores propres restent intacts et
   * libérables indépendamment par leurs appelants respectifs.
   */
  def merge(other: BucketManager): BucketManager = {
    require(other.coresetSize == coresetSize, s"merge: tailles de coreset incompatibles ($coresetSize vs ${other.coresetSize})")
    require(other.dimension == dimension, s"merge: dimensions incompatibles ($dimension vs ${other.dimension})")

    val out = new BucketManager(coresetSize, dimension, seed ^ other.seed)

    val thisUnion  = union()
    val otherUnion = other.union()
    val combined   = PointsSoA.concat(thisUnion, otherUnion)
    thisUnion.free(); otherUnion.free()

    val reduced = CoresetTree.build(combined, coresetSize, out.rng) // consomme et libère `combined`
    out.ensureLevel(1)
    out.buckets(1)        = reduced
    out.totalWeightSeen    = totalWeightSeen + other.totalWeightSeen
    out.collapsed          = true
    out
  }

  /**
   * Union de tous les buckets non vides, SANS réduction. Renvoie TOUJOURS un store
   * fraîchement alloué, jamais une référence directe vers un bucket vivant — même
   * quand un seul bucket est non vide. C'est indispensable : `extractCoreset()` passe
   * ce résultat à `CoresetTree.build`, qui en prend possession (le libère, ou le
   * renvoie tel quel si déjà assez petit). Si `union()` renvoyait directement un
   * bucket vivant dans le cas à un seul élément, l'appelant recevrait soit un store
   * libéré par erreur (use-after-free), soit un alias que `BucketManager` croit
   * toujours posséder alors qu'il a changé de propriétaire.
   */
  def union(): PointsSoA = {
    val nonEmptyBuckets = buckets.filter(_.n > 0)
    require(nonEmptyBuckets.nonEmpty, "union: aucun bucket non vide")

    var accumulated = copyOf(nonEmptyBuckets(0))   // copie dès le départ, jamais d'alias
    var bucketIndex = 1
    while (bucketIndex < nonEmptyBuckets.length) {
      val merged = PointsSoA.concat(accumulated, nonEmptyBuckets(bucketIndex))
      accumulated.free()
      accumulated = merged
      bucketIndex += 1
    }
    accumulated
  }

  /** Copie indépendante d'un store — nécessaire pour que `union()` ne renvoie jamais
   * un alias d'un bucket vivant. */
  private def copyOf(store: PointsSoA): PointsSoA = {
    val output = PointsSoA.allocate(store.n, store.dimension)
    val tmp = new Array[Double](store.dimension)
    var pointIndex = 0
    while (pointIndex < store.n) {
      store.copyCoordinatesInto(pointIndex, tmp)
      output.append(tmp, store.weight(pointIndex))
      pointIndex += 1
    }
    output
  }

  def extractCoreset(): PointsSoA = {
    val unionStore = union()
    CoresetTree.build(unionStore, coresetSize, rng) // consomme et libère `unionStore`
  }

  private def ensureLevel(level: Int): Unit =
    while (buckets.length <= level) buckets += PointsSoA.allocate(coresetSize, dimension)

  def numPointsSeen: Double = totalWeightSeen

  def numLevels: Int = buckets.length

  def free(): Unit = {
    var level = 0
    while (level < buckets.length) { buckets(level).free(); level += 1 }
  }

  /**
   * Insertion en lot depuis un `PointsSoA` déjà construit — évite l'aller-retour
   * PointsSoA → Array[Double] → BucketManager → PointsSoA interne qu'imposerait
   * un simple appel répété à `insert` depuis l'extérieur. Un seul tableau tampon
   * est réutilisé sur toute la boucle : aucune allocation par point.
   */
  def insertAll(store: PointsSoA): Unit = {
    require(store.dimension == dimension, s"insertAll: dimension incompatible (${store.dimension} vs $dimension)")
    val tmp = new Array[Double](dimension)
    var i = 0
    while (i < store.n) {
      store.copyCoordinatesInto(i, tmp)
      insert(tmp, store.weight(i))
      i += 1
    }
  }

  /**
   * Vérification des invariants du §2.3.4. Utilisée sous `assert` : coûteuse et
   * désactivée en production (cf. build.sbt, -ea).
   */
  private[coreset] def checkInvariants(): Boolean = {
    if (buckets(0).n > coresetSize) return false
    var level = 1
    while (level < buckets.length) {
      val size = buckets(level).n
      if (size != 0 && size != coresetSize) return false
      level += 1
    }
    true
  }
}

object BucketManager {
  def recommendedM(k: Int): Int = 200 * k
  def apply(coresetSize: Int, dimension: Int, seed: Long = 42L): BucketManager =
    new BucketManager(coresetSize, dimension, seed)
}