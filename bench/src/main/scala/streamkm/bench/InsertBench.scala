package streamkm.bench

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole
import scala.util.Random
import streamkm.core.{Point, PointsSoA}
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

  // BucketManager.insertAll LIT le store (copyCoordinatesInto/weight) sans le libérer —
  // réutilisable tel quel à chaque invocation, contrairement à CoresetTree.build.
  var points: PointsSoA = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    val rng = new Random(42L)
    val raw = Array.fill(n)(Point(Array.fill(d)(rng.nextGaussian())))
    points  = PointsSoA.fromPoints(raw)
  }

  @TearDown(Level.Trial)
  def tearDown(): Unit = points.free()

  @Benchmark
  def insertAll(bh: Blackhole): Unit = {
    val bm = BucketManager(m, dimension = d, seed = 42L)
    bm.insertAll(points)
    bh.consume(bm)
    bm.free() // sans quoi chaque invocation fuit les buckets natifs alloués
  }
}