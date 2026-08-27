package streamkm.bench

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole
import scala.util.Random
import streamkm.core.{KMeans, Point, PointsSoA}

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.SampleTime, Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 10, time = 3)
@Fork(1)
class LloydBench {

  @Param(Array("1000", "10000"))
  var n: Int = _

  @Param(Array("10", "50"))
  var k: Int = _

  val d: Int = 10

  var points: PointsSoA = _ // stable sur tout le Trial — lloyd ne le libère jamais
  var init:   PointsSoA = _ // CONSOMMÉ par lloyd — régénéré à CHAQUE invocation

  @Setup(Level.Trial)
  def setupTrial(): Unit = {
    val rng = new Random(42L)
    val raw = Array.fill(n)(Point(Array.fill(d)(rng.nextGaussian())))
    points  = PointsSoA.fromPoints(raw)
  }

  @Setup(Level.Invocation)
  def setupInvocation(): Unit = {
    init = KMeans.seedPlusPlus(points, k, new Random(43L))
  }

  @TearDown(Level.Trial)
  def tearDownTrial(): Unit = points.free()

  @Benchmark
  def lloyd(bh: Blackhole): Unit = {
    val model = KMeans.lloyd(points, init) // consomme et libère `init`
    bh.consume(model)
    model.centers.free()
  }
}