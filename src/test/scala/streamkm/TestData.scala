package streamkm

import scala.util.Random

import streamkm.core.{Distance, Point}

/**
 * Générateur de données synthétiques à vérité terrain.
 *
 * C'est l'outil de l'expérience E1 (correction) : on connaît les vrais centres, donc on
 * peut vérifier que l'algorithme les retrouve, ce qu'aucun jeu réel ne permet. Il servira
 * aussi en E3/E4 pour faire varier la taille des données à volonté sans dépendre d'un
 * téléchargement.
 */
object TestData {

  final case class Mixture(points: Array[Point], trueCenters: Array[Array[Double]])

  /**
   * Mélange de `k` gaussiennes isotropes de dimension `d`, dont les centres sont placés
   * sur une grille de pas `spread` pour garantir la séparabilité.
   */
  def gaussianMixture(
    n:      Int,
    k:      Int,
    d:      Int,
    sigma:  Double = 0.5,
    spread: Double = 20.0,
    seed:   Long   = 12345L
  ): Mixture = {
    val rng = new Random(seed)

    val centers = Array.tabulate(k) { c =>
      Array.tabulate(d)(j => ((c + 1) * (j + 1) % k).toDouble * spread)
    }

    val pts = Array.tabulate(n) { _ =>
      val c   = rng.nextInt(k)
      val ctr = centers(c)
      Point(Array.tabulate(d)(j => ctr(j) + rng.nextGaussian() * sigma))
    }

    Mixture(pts, centers)
  }

  /** Somme des poids d'un ensemble de points. */
  def totalMass(pts: Array[Point]): Double = {
    var s = 0.0
    var i = 0
    while (i < pts.length) { s += pts(i).weight; i += 1 }
    s
  }

  /**
   * Distance maximale entre chaque vrai centre et le centre estimé le plus proche.
   * Sert à vérifier qu'un clustering a bien « retrouvé » la structure sous-jacente,
   * indépendamment de l'ordre des centres.
   */
  def maxCenterDeviation(
    trueCenters: Array[Array[Double]],
    found:       Array[Array[Double]]
  ): Double = {
    var worst = 0.0
    var i     = 0
    while (i < trueCenters.length) {
      val d = math.sqrt(Distance.nearest(trueCenters(i), found)._2)
      if (d > worst) worst = d
      i += 1
    }
    worst
  }
}
