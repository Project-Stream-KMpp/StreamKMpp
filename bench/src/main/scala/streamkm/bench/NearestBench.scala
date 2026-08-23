package streamkm.bench

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole
import scala.util.Random
import streamkm.core.Distance

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.SampleTime, Mode.Throughput))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 2)
@Fork(1)
class NearestBench {

  @Param(Array("10", "50"))
  var d: Int = _

  @Param(Array("10", "50"))
  var k: Int = _

  var p:       Array[Double]        = _
  var centers: Array[Array[Double]] = _

  @Setup
  def setup(): Unit = {
    val rng = new Random(42L)
    p       = Array.fill(d)(rng.nextDouble())
    centers = Array.fill(k)(Array.fill(d)(rng.nextDouble()))
  }

  @Benchmark
  def nearest(bh: Blackhole): Unit = bh.consume(Distance.nearest(p, centers))
}
