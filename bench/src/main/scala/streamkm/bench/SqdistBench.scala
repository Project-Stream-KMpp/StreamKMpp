package streamkm.bench

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole
import scala.util.Random
import streamkm.core.{Distance, PointsSoA}

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.SampleTime, Mode.Throughput))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 2)
@Fork(1)
class SqdistBench {

  @Param(Array("2", "10", "50"))
  var d: Int = _

  var storeA: PointsSoA = _
  var storeB: PointsSoA = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    val rng = new Random(42L)
    storeA  = PointsSoA.allocate(1, d)
    storeA.append(Array.fill(d)(rng.nextDouble()), 1.0)
    storeB  = PointsSoA.allocate(1, d)
    storeB.append(Array.fill(d)(rng.nextDouble()), 1.0)
  }

  @TearDown(Level.Trial)
  def tearDown(): Unit = { storeA.free(); storeB.free() }

  // Distance.sqdist ne consomme ni ne modifie ses arguments — un seul store par Trial suffit.
  @Benchmark
  def sqdist(bh: Blackhole): Unit = bh.consume(Distance.squareDistance(storeA, 0, storeB, 0))
}