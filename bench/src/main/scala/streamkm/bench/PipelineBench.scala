package streamkm.bench

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole
import scala.util.Random
import streamkm.core.{KMeans, Point}
import streamkm.coreset.BucketManager

/**
 * Benchmark bout-en-bout de la couche kernel (sans Spark) :
 *   BucketManager.insertAll → extractCoreset → KMeans.fit
 *
 * Représente le pipeline séquentiel de référence de la thèse (§5.2.1) et isole
 * le goulot `KMeans.fit` que §5.3.1 identifie comme non-parallélisable.
 *
 * Paramètres intentionnellement restreints (n ≤ 50k) : KMeans.fit avec
 * nRestarts=5 peut prendre plusieurs secondes à grande échelle. Pour les
 * profils à grande échelle, utiliser E7Runner.
 */
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

  var points: Array[Point] = _
  var m: Int               = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    val rng = new Random(42L)
    points  = Array.fill(n)(Point(Array.fill(d)(rng.nextGaussian())))
    m       = BucketManager.recommendedM(k)
  }

  /** Pipeline complet : ingest + coreset + clustering. */
  @Benchmark
  def fullPipeline(bh: Blackhole): Unit = {
    val bm = new BucketManager(m, seed = 42L)
    bm.insertAll(points)
    val coreset = bm.extractCoreset()
    bh.consume(KMeans.fit(coreset, k, nRestarts = 5, seed = 42L))
  }

  /** Isole BucketManager seul (ingest + coreset) pour quantifier la part de KMeans.fit. */
  @Benchmark
  def ingestOnly(bh: Blackhole): Unit = {
    val bm = new BucketManager(m, seed = 42L)
    bm.insertAll(points)
    bh.consume(bm.extractCoreset())
  }
}
