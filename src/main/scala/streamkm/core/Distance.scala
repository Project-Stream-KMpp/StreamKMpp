package streamkm.core

object Distance {
  def squareDistance (setOfPoints: PointsSoA, i: Int, j: Int): Double = {
    var sumOfSquares = 0.0
    var k = 0
    val dimension = setOfPoints.dimension

    while (k < dimension){
      val delta = setOfPoints.coordinates(i, k) - setOfPoints.coordinates(j, k)
      sumOfSquares += delta * delta
      k += 1
    }
    sumOfSquares
  }

  // Calcule la distance au carrée entre deux points de deux ensembles de points distincts
  // Permet de calculer la distance au carrée entre les points et les centres
  def squareDistance (a: PointsSoA, i: Int, b: PointsSoA, j: Int): Double = {
    require(a.dimension == b.dimension, "sqdist: dimensions incompatibles")
    var sumOfSquares = 0.0
    var k = 0
    val dimension = a.dimension
    while (k < dimension) {
      val delta = a.coordinates(i, k) - b.coordinates(j, k)
      sumOfSquares += delta * delta
      k += 1
    }
    sumOfSquares
  }

  def nearest (points: PointsSoA, i: Int, centers: PointsSoA, output: NearestResult): Unit = {
    var bestIndex = 0
    var bestDistance = Double.MaxValue
    var centerIndex = 0

    while (centerIndex < centers.n){
      val distance = squareDistance(points, i, centers, centerIndex)
      if (distance < bestDistance){
        bestDistance = distance
        bestIndex = centerIndex
      }
      centerIndex += 1
    }
    output.index = bestIndex
    output.distance = bestDistance
  }


  /**
   * Distance euclidienne AU CARRÉ.
   *
   * Toute la chaîne StreamKM++ (seeding D², coût du coreset tree, fonction objectif
   * k-means) travaille sur des distances au carré. On ne calcule donc jamais de racine :
   * elle serait à la fois inutile et coûteuse. C'est le point le plus chaud du programme.
   */
  def squareDistance(a: Array[Double], b: Array[Double]): Double = {
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
      val d = squareDistance(p, centers(c))
      if (d < bestD) { bestD = d; best = c }
      c += 1
    }
    (best, bestD)
  }
}