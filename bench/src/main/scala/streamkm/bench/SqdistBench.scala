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
class SqdistBench {

  @Param(Array("2", "10", "50"))
  var d: Int = _

  var a: Array[Double] = _
  var b: Array[Double] = _

  @Setup
  def setup(): Unit = {
    val rng = new Random(42L)
    a = Array.fill(d)(rng.nextDouble())
    b = Array.fill(d)(rng.nextDouble())
  }

  @Benchmark
  def sqdist(bh: Blackhole): Unit = bh.consume(Distance.sqdist(a, b))
}
