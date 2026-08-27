package streamkm.bench

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole
import scala.util.Random
import streamkm.core.{KMeans, Point, PointsSoA}
import streamkm.coreset.BucketManager

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.SampleTime, Mode.Throughput))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 10, time = 3)
@Fork(1)
class PipelineBench {

  @Param(Array("10000", "50000"))
  var n: Int = _

  @Param(Array("10", "25"))
  var k: Int = _

  val d: Int = 10

  var points: PointsSoA = _
  var m: Int            = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    val rng = new Random(42L)
    val raw = Array.fill(n)(Point(Array.fill(d)(rng.nextGaussian())))
    points  = PointsSoA.fromPoints(raw)
    m       = BucketManager.recommendedM(k)
  }

  @TearDown(Level.Trial)
  def tearDown(): Unit = points.free()

  @Benchmark
  def fullPipeline(bh: Blackhole): Unit = {
    val bm      = BucketManager(m, dimension = d, seed = 42L)
    bm.insertAll(points)
    val coreset = bm.extractCoreset()
    val model   = KMeans.fit(coreset, k, nRestarts = 5, seed = 42L)
    coreset.free()
    bh.consume(model)
    model.centers.free()
    bm.free()
  }

  @Benchmark
  def ingestOnly(bh: Blackhole): Unit = {
    val bm      = BucketManager(m, dimension = d, seed = 42L)
    bm.insertAll(points)
    val coreset = bm.extractCoreset()
    bh.consume(coreset)
    coreset.free()
    bm.free()
  }
}