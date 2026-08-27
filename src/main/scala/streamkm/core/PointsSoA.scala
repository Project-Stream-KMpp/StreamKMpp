package streamkm.core

import java.nio.{ByteBuffer, DoubleBuffer}

final class PointsSoA private(
                               private var coordinatesOwner:  ByteBuffer,   // propriétaire réel de la mémoire native
                               private var coordinatesBuffer: DoubleBuffer, // vue typée sur coordinatesOwner
                               private var weightsOwner:      ByteBuffer,
                               private var weightsBuffer:     DoubleBuffer,
                               val dimension: Int,
                               val capacity:  Int,
                               private var _n: Int
                             ) {
  def n: Int = _n

  @inline def coordinates(i: Int, j: Int): Double = coordinatesBuffer.get(i * dimension + j)
  @inline def weight(i: Int): Double = weightsBuffer.get(i)

  def append(point: Array[Double], weight: Double): Int = {
    require(_n < capacity, s"Capacité $capacity dépassée")
    require(point.length == dimension, s"dimension attendue $dimension, reçue ${point.length}")
    val baseOffset = _n * dimension
    var j = 0
    while (j < dimension) {
      coordinatesBuffer.put(baseOffset + j, point(j))
      j += 1
    }
    weightsBuffer.put(_n, weight)
    val index = _n
    _n += 1
    index
  }

  def set(i: Int, point: Array[Double], weight: Double): Unit = {
    require(i < _n, s"index $i hors de [0, $n)")
    require(point.length == dimension, s"dimension attendue $dimension, reçue ${point.length}")
    val baseOffset = dimension * i
    var j = 0
    while (j < dimension) {
      coordinatesBuffer.put(baseOffset + j, point(j))
      j += 1
    }
    weightsBuffer.put(i, weight)
  }

  def copyCoordinatesInto(i: Int, outputArray: Array[Double]): Unit = {
    val baseOffset = i * dimension
    var j = 0
    while (j < dimension) {
      outputArray(j) = coordinatesBuffer.get(baseOffset + j)
      j += 1
    }
  }

  /**
   * Libère la mémoire native des DEUX ByteBuffer PROPRIÉTAIRES — jamais des vues
   * DoubleBuffer, qui ne possèdent aucune mémoire à libérer (leur .isDirect vaut
   * true, mais elles ne sont pas elles-mêmes des ByteBuffer et n'ont donc pas
   * accès à Unsafe.invokeCleaner, qui n'opère que sur un ByteBuffer direct racine).
   */
  def free(): Unit = {
    PointsSoA.cleanDirectBuffer(coordinatesOwner)
    PointsSoA.cleanDirectBuffer(weightsOwner)
  }
}

object PointsSoA {

  def allocate(capacity: Int, dimension: Int): PointsSoA = {
    val coordinatesOwner  = ByteBuffer.allocateDirect(capacity * dimension * 8)
    val coordinatesBuffer = coordinatesOwner.asDoubleBuffer()
    val weightsOwner       = ByteBuffer.allocateDirect(capacity * 8)
    val weightsBuffer      = weightsOwner.asDoubleBuffer()
    new PointsSoA(coordinatesOwner, coordinatesBuffer, weightsOwner, weightsBuffer, dimension, capacity, 0)
  }

  def fromPoints(points: Array[Point]): PointsSoA = {
    require(points.nonEmpty, "PointsSoA.fromPoints: ensemble vide")
    val dimension = points(0).dim
    val pointsSoA = allocate(points.length, dimension)
    var i = 0
    while (i < points.length) {
      pointsSoA.append(points(i).coords, points(i).weight)
      i += 1
    }
    pointsSoA
  }

  def toPoints(pointsSoA: PointsSoA): Array[Point] = {
    val output = new Array[Point](pointsSoA.n)
    val tmp    = new Array[Double](pointsSoA.dimension)
    var i = 0
    while (i < pointsSoA.n) {
      pointsSoA.copyCoordinatesInto(i, tmp)
      output(i) = new Point(tmp.clone(), pointsSoA.weight(i))
      i += 1
    }
    output
  }

  def concat(a: PointsSoA, b: PointsSoA): PointsSoA = {
    require(a.dimension == b.dimension, "PointsSoA.concat: dimensions incompatibles")
    val output = allocate(a.n + b.n, a.dimension)
    val tmp    = new Array[Double](a.dimension)
    var i = 0
    while (i < a.n) { a.copyCoordinatesInto(i, tmp); output.append(tmp, a.weight(i)); i += 1 }
    var k = 0
    while (k < b.n) { b.copyCoordinatesInto(k, tmp); output.append(tmp, b.weight(k)); k += 1 }
    output
  }

  /**
   * Réflexion sur sun.misc.Unsafe (module jdk.unsupported, exporté par défaut —
   * contrairement à sun.nio.ch.DirectBuffer qui vit dans java.base et n'est PAS
   * exporté). invokeCleaner est la voie stable depuis Java 9 pour libérer un
   * ByteBuffer direct : elle n'opère QUE sur le buffer racine, jamais sur une vue
   * dérivée (asDoubleBuffer, slice, duplicate) — d'où la nécessité de conserver
   * coordinatesOwner/weightsOwner séparément de leurs vues DoubleBuffer.
   */
  private val unsafe: sun.misc.Unsafe = {
    val field = classOf[sun.misc.Unsafe].getDeclaredField("theUnsafe")
    field.setAccessible(true)
    field.get(null).asInstanceOf[sun.misc.Unsafe]
  }

  private def cleanDirectBuffer(buffer: ByteBuffer): Unit =
    if (buffer.isDirect) unsafe.invokeCleaner(buffer)
}