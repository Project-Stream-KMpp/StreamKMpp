package streamkm.experiments

import java.io.{File, PrintWriter}
import scala.collection.mutable

import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.{Vector => MLlibVector, Vectors => MLlibVectors}
import org.apache.spark.ml.clustering.{KMeans => MLKMeans}
import org.apache.spark.ml.linalg.{Vectors => MLVectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import streamkm.core.{KMeans, Point}
import streamkm.spark.{CostEvaluator, StreamKMParams, StreamKMPlusPlus}

/**
 * E6 — Comparaison de référence (SPEC §5, « cadeau » : à la place du code C
 * original de la thèse, non disponible, on compare à `spark.ml.KMeans`
 * (batch) et `StreamingKMeans` (`org.apache.spark.mllib.clustering`), sur
 * Covertype (mêmes données que E2, cf. `CovertypeLoader`).
 *
 * Les TROIS méthodes sont évaluées par la MÊME fonction de coût
 * (`CostEvaluator.cost`, couche 1 `KMeans.cost` distribuée), jamais par le
 * coût interne de chaque bibliothèque (qui peut différer par construction :
 * `ml.KMeans` expose un `trainingCost` sur le sous-échantillon vu à
 * l'entraînement, pas nécessairement sur l'ensemble complet) — condition pour
 * que la comparaison soit honnête.
 *
 * - **StreamKM++** : `mergeCoresets` (E4) puis `KMeans.fit` (E1) sur le
 *   coreset — un seul passage sur les données, garantie de coreset.
 * - **spark.ml.KMeans** : Lloyd batch classique, autant d'itérations que
 *   `core.KMeans.lloyd` (`maxIter=100`) pour une comparaison à effort égal.
 *   Voit l'intégralité des données à chaque itération (pas de garantie de
 *   passage unique).
 * - **StreamingKMeans (mllib)** : le seul des trois qui est réellement
 *   incrémental comme StreamKM++, mais sans garantie de coreset et sensible à
 *   l'initialisation (SPEC §5) — pas de fenêtre "batch", juste un facteur
 *   d'oubli (`decayFactor=1.0` ici : pas d'oubli, tous les points comptent
 *   également, condition la plus proche de StreamKM++ pour la comparaison).
 *   Simulé en rejouant le RDD statique par tranches dans un
 *   `StreamingContext.queueStream`, faute d'un vrai flux disponible.
 *
 * OBSERVATION (smoke test sur `data/small/covtype_10k.data`, m=1000, k=5) :
 * `StreamingKMeans` produit un coût plusieurs ORDRES DE GRANDEUR plus mauvais
 * que StreamKM++ et `ml.KMeans` (~1e14 contre ~1e10). Cause probable, pas un
 * bug de ce fichier : `setRandomCenters` tire les centres initiaux suivant
 * une loi normale centrée en 0, sans rapport avec l'échelle réelle des
 * features de Covertype (non standardisées, certaines colonnes vont jusqu'à
 * ~7000) — avec seulement quelques micro-batches, l'algorithme n'a pas le
 * temps de corriger un mauvais tirage initial. On NE standardise PAS les
 * features pour autant (cf. `CovertypeLoader`) : le faire ici et pas pour les
 * deux autres méthodes fausserait la comparaison. C'est un résultat à
 * signaler tel quel dans la section « points forts/faibles » du rapport :
 * StreamKM++ garantit une propriété de coreset, `StreamingKMeans` non, et ce
 * run le montre concrètement (SPEC §5, remarque sur E6).
 */
object E6Baseline {

  final case class Row(method: String, k: Int, timeMs: Double, cost: Double)

  private def parseInts(s: String): Seq[Int] = s.split(',').map(_.trim.toInt).toSeq

  def main(args: Array[String]): Unit = {
    val argMap = args.grouped(2).collect { case Array(k, v) => k -> v }.toMap

    val dataPath  = argMap.getOrElse("--data", "data/small/covtype_10k.data")
    val outPath   = argMap.getOrElse("--out", "results/e6_baseline.csv")
    val kValues   = parseInts(argMap.getOrElse("--k", "25"))
    val m         = argMap.getOrElse("--m", "5000").toInt
    val numParts  = argMap.getOrElse("--partitions", "4").toInt
    val seed      = argMap.getOrElse("--seed", "42").toLong
    val numChunks = argMap.getOrElse("--stream-chunks", "10").toInt

    val spark = SparkSession.builder().appName("StreamKM++ E6 baseline").master("local[*]").getOrCreate()
    val sc    = spark.sparkContext

    try {
      val points = CovertypeLoader.load(sc, dataPath).repartition(numParts).cache()
      val n       = points.count()
      println(s"[E6] $n points chargés depuis $dataPath, $numParts partitions, m=$m")

      val rows = kValues.flatMap { k =>
        Seq(
          runStreamKM(points, k, m, seed),
          runMlKMeans(spark, points, k, seed),
          runStreamingKMeans(sc, points, k, numChunks, seed)
        )
      }

      rows.foreach(r => println(f"[E6] ${r.method}%-14s k=${r.k} temps=${r.timeMs}%.1fms coût=${r.cost}%.4g"))
      writeCsv(outPath, rows)
      println(s"[E6] ${rows.length} lignes écrites dans $outPath")
    } finally {
      spark.stop()
    }
  }

  private def runStreamKM(points: RDD[Point], k: Int, m: Int, seed: Long): Row = {
    val params = StreamKMParams(k, m, nRestarts = 5, seed = seed)
    val t0      = System.nanoTime()
    val coreset = StreamKMPlusPlus.mergeCoresets(points, params)
    val model   = KMeans.fit(coreset, k, params.nRestarts, seed = seed)
    val timeMs  = (System.nanoTime() - t0) / 1e6
    Row("StreamKM++", k, timeMs, CostEvaluator.cost(points, model.centers))
  }

  private def runMlKMeans(spark: SparkSession, points: RDD[Point], k: Int, seed: Long): Row = {
    import spark.implicits._
    val df = points.map(p => Tuple1(MLVectors.dense(p.coords))).toDF("features")

    val t0    = System.nanoTime()
    val model = new MLKMeans().setK(k).setMaxIter(100).setSeed(seed).fit(df)
    val timeMs = (System.nanoTime() - t0) / 1e6

    val centers = model.clusterCenters.map(_.toArray)
    Row("ml.KMeans", k, timeMs, CostEvaluator.cost(points, centers))
  }

  /**
   * `StreamingKMeans` exige un `DStream`, pas un `RDD` statique : on rejoue
   * `points` en `numChunks` tranches via `queueStream`, un intervalle de 1s
   * par tranche (assez large pour laisser le micro-batch précédent finir sur
   * un poste de développement).
   */
  private def runStreamingKMeans(
    sc: org.apache.spark.SparkContext, points: RDD[Point], k: Int, numChunks: Int, seed: Long
  ): Row = {
    val vectors: RDD[MLlibVector] = points.map(p => MLlibVectors.dense(p.coords))
    val dim    = vectors.first().size
    val chunks = vectors.randomSplit(Array.fill(numChunks)(1.0 / numChunks), seed)

    val ssc   = new StreamingContext(sc, Seconds(1))
    val queue = new mutable.Queue[RDD[MLlibVector]]()
    queue ++= chunks
    val dstream = ssc.queueStream(queue, oneAtATime = true)

    val model = new StreamingKMeans()
      .setK(k)
      .setDecayFactor(1.0) // pas d'oubli : comparable au coreset, qui ne périme jamais les points vus
      .setRandomCenters(dim, 0.0, seed)
    model.trainOn(dstream)

    val t0 = System.nanoTime()
    ssc.start()
    ssc.awaitTerminationOrTimeout(numChunks * 1200L + 5000L)
    val timeMs = (System.nanoTime() - t0) / 1e6
    ssc.stop(stopSparkContext = false, stopGracefully = false)

    val centers = model.latestModel().clusterCenters.map(_.toArray)
    Row("StreamingKMeans", k, timeMs, CostEvaluator.cost(points, centers))
  }

  private def writeCsv(path: String, rows: Seq[Row]): Unit = {
    val file = new File(path)
    Option(file.getParentFile).foreach(_.mkdirs())
    val pw = new PrintWriter(file)
    try {
      pw.println("method,k,time_ms,cost")
      rows.foreach(r => pw.println(s"${r.method},${r.k},${r.timeMs},${r.cost}"))
    } finally pw.close()
  }
}
