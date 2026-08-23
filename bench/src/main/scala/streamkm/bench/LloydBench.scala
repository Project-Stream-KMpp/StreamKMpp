package streamkm.bench

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole
import scala.util.Random
import streamkm.core.{KMeans, Point}

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

  var points: Array[Point]         = _
  var init:   Array[Array[Double]] = _

  @Setup(Level.Invocation)
  def setup(): Unit = {
    val rng = new Random(42L)
    points  = Array.fill(n)(Point(Array.fill(d)(rng.nextGaussian())))
    init    = KMeans.seedPlusPlus(points, k, new Random(43L))
  }

  @Benchmark
  def lloyd(bh: Blackhole): Unit = bh.consume(KMeans.lloyd(points, init))
}
