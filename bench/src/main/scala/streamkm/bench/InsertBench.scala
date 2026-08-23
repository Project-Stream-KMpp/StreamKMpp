package streamkm.bench

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole
import scala.util.Random
import streamkm.core.Point
import streamkm.coreset.BucketManager

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.SampleTime, Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 10, time = 3)
@Fork(1)
class InsertBench {

  @Param(Array("10000", "100000"))
  var n: Int = _

  @Param(Array("1000", "5000"))
  var m: Int = _

  val d: Int = 10

  var points: Array[Point] = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    val rng = new Random(42L)
    points  = Array.fill(n)(Point(Array.fill(d)(rng.nextGaussian())))
  }

  @Benchmark
  def insertAll(bh: Blackhole): Unit = {
    val bm = new BucketManager(m, seed = 42L)
    bm.insertAll(points)
    bh.consume(bm)
  }
}
