package streamkm.core

/**
 * Un point pondéré de l'espace euclidien de dimension d.
 *
 * `weight` vaut 1.0 pour un point brut du flux, et la masse totale du sous-ensemble
 * représenté pour un point issu d'un coreset (thèse §2.3.3 : « we denote the weight of
 * each q_i as the total number of points in each subset P »).
 *
 * ATTENTION : `coords` n'est pas copié défensivement, ni à la construction ni à la
 * lecture. C'est un choix délibéré — copier à chaque accès tuerait les performances dans
 * les boucles de `Distance` et de `CoresetTree`. La contrepartie est un invariant à
 * respecter partout : **un tableau de coordonnées n'est jamais muté après construction**.
 * Toute mutation potentielle passe par `.clone()` explicite.
 *
 * Cette classe reste volontairement une classe « objet » (Array of Structures) en E1/E2.
 * L'étape E6 introduira une représentation Structure of Arrays derrière la même API
 * publique, et JMH mesurera l'écart.
 */
final class Point(val coords: Array[Double], val weight: Double) extends Serializable {

  def dim: Int = coords.length

  /** Utile uniquement pour les messages d'erreur et le débogage. */
  override def toString: String =
    coords.mkString("Point([", ", ", s"], w=$weight)")
}

object Point {

  /** Point brut du flux : poids 1. */
  def apply(coords: Array[Double]): Point = new Point(coords, 1.0)

  def apply(coords: Array[Double], weight: Double): Point = new Point(coords, weight)

  /** Confort pour les tests : `Point(1.0, 2.0)`. */
  def of(coords: Double*): Point = new Point(coords.toArray, 1.0)

  /**
   * Parsing d'une ligne CSV/SSV en Point.
   * Renvoie None si la ligne est malformée ou n'a pas la dimension attendue — c'est
   * l'équivalent du `FlatMapToPoint` de la thèse (§4.1), qui utilise un flatMap
   * précisément pour pouvoir rejeter les lignes invalides sans faire échouer le job.
   */
  def parse(line: String, dim: Int, sep: Char = ','): Option[Point] = {
    val parts = line.trim.split(sep)
    if (parts.length != dim) None
    else {
      val arr = new Array[Double](dim)
      var i    = 0
      var ok   = true
      while (i < dim && ok) {
        try arr(i) = java.lang.Double.parseDouble(parts(i))
        catch { case _: NumberFormatException => ok = false }
        i += 1
      }
      if (ok) Some(new Point(arr, 1.0)) else None
    }
  }
}

object Distance {

  /**
   * Distance euclidienne AU CARRÉ.
   *
   * Toute la chaîne StreamKM++ (seeding D², coût du coreset tree, fonction objectif
   * k-means) travaille sur des distances au carré. On ne calcule donc jamais de racine :
   * elle serait à la fois inutile et coûteuse. C'est le point le plus chaud du programme.
   */
  def sqdist(a: Array[Double], b: Array[Double]): Double = {
    var s = 0.0
    var i = 0
    val n = a.length
    while (i < n) {
      val d = a(i) - b(i)
      s += d * d
      i += 1
    }
    s
  }

  /**
   * Indice du centre le plus proche et distance au carré correspondante, en un seul
   * parcours.
   *
   * Le tuple alloue (et boxe le Int et le Double). C'est assumé en E1 : la lisibilité
   * prime tant que la correction n'est pas établie. E6 remplacera cet appel par une
   * boucle inline ou un encodage sans allocation, et JMH quantifiera le gain.
   */
  def nearest(p: Array[Double], centers: Array[Array[Double]]): (Int, Double) = {
    var best  = 0
    var bestD = Double.MaxValue
    var c     = 0
    while (c < centers.length) {
      val d = sqdist(p, centers(c))
      if (d < bestD) { bestD = d; best = c }
      c += 1
    }
    (best, bestD)
  }
}
