package streamkm.bench

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole
import scala.util.Random
import streamkm.core.{Point, PointsSoA}
import streamkm.coreset.CoresetTree

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.SampleTime, Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 10, time = 3)
@Fork(1)
class CoresetTreeBench {

  @Param(Array("5000", "20000"))
  var n: Int = _

  @Param(Array("500", "2000"))
  var m: Int = _

  val d: Int = 10

  // Fixture AoS stable sur tout le Trial : source pour régénérer une copie PointsSoA
  // fraîche à chaque invocation (voir setupInvocation).
  var rawPoints: Array[Point] = _
  var points:    PointsSoA    = _

  @Setup(Level.Trial)
  def setupTrial(): Unit = {
    val rng   = new Random(42L)
    rawPoints = Array.fill(n)(Point(Array.fill(d)(rng.nextGaussian())))
  }

  // build() CONSOMME (libère) son store d'entrée — un store ne peut donc servir qu'à
  // UN SEUL appel. Le temps de @Setup n'est pas compté dans la mesure JMH (SampleTime/
  // Throughput), donc cette régénération ne biaise pas le résultat CHRONOMÉTRÉ — mais
  // elle ajoute une allocation native réelle avant chaque invocation, qui peut réduire
  // le débit d'itérations/seconde observable en mode Throughput sur les petites tailles.
  // Limite méthodologique assumée, à documenter dans le rapport plutôt qu'ignorée.
  @Setup(Level.Invocation)
  def setupInvocation(): Unit = {
    points = PointsSoA.fromPoints(rawPoints)
  }

  @Benchmark
  def build(bh: Blackhole): Unit = {
    val result = CoresetTree.build(points, m, new Random(43L))
    bh.consume(result)
    result.free()
  }
}