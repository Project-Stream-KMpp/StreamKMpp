package streamkm.bench

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole
import scala.util.Random
import streamkm.core.{Distance, NearestResult, PointsSoA}

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

  var point:   PointsSoA     = _
  var centers: PointsSoA     = _
  // Réutilisé à travers toutes les invocations — c'est exactement ce que ce type existe
  // pour permettre : zéro allocation par appel, cf. décision "sortie par paramètre".
  var out: NearestResult = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    val rng = new Random(42L)
    point   = PointsSoA.allocate(1, d)
    point.append(Array.fill(d)(rng.nextDouble()), 1.0)

    centers = PointsSoA.allocate(k, d)
    var i = 0
    while (i < k) { centers.append(Array.fill(d)(rng.nextDouble()), 1.0); i += 1 }

    out = new NearestResult(0, 0.0)
  }

  @TearDown(Level.Trial)
  def tearDown(): Unit = { point.free(); centers.free() }

  @Benchmark
  def nearest(bh: Blackhole): Unit = {
    Distance.nearest(point, 0, centers, out)
    bh.consume(out)
  }
}