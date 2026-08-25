package streamkm.experiments

import java.io.{File, FileOutputStream, OutputStreamWriter, PrintWriter}
import java.nio.charset.StandardCharsets

import scala.io.Source

import org.apache.spark.ml.clustering.{KMeans => MLKMeans}
import org.apache.spark.ml.linalg.{SQLDataTypes, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row => SqlRow, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}

import streamkm.core.{KMeans, Point}
import streamkm.coreset.BucketManager
import streamkm.spark.{CostEvaluator, StreamKMParams, StreamKMPlusPlus}

/**
 * Harnais d'expériences de QUALITÉ du clustering (coût SSE), en miroir de la thèse
 * §5.2 (Bitsakis 2018, "Non-Parallel Experiments" / "Comparison with original
 * StreamKM++"). Mesure UNIQUEMENT le coût — PAS le temps d'exécution, couvert
 * séparément par les benchmarks JMH (E6).
 *
 * Objet séparé de la production (SPEC §1, règle d'or) : ne modifie et ne duplique
 * aucune logique de `streamkm.core`, `streamkm.coreset` ou `streamkm.spark`. Toute
 * la logique de coût vient de `KMeans.cost` / `CostEvaluator.cost`, tout le calcul
 * du coreset vient de `BucketManager` / `StreamKMPlusPlus.mergeCoresets`.
 *
 * Sortie : CSV écrits localement (pas via `df.write.csv`, qui produirait un
 * répertoire de part-files plutôt qu'un fichier unique) — voir CLAUDE.md pour la
 * justification de ce choix et l'absence volontaire de génération de graphes ici.
 */
object QualityMetrics {

  /** Une ligne de résultat brut : un (dataset, méthode, k, m) → coût. */
  final case class ResultRow(dataset: String, method: String, k: Int, m: Int, cost: Double)

  // ======================================================================
  // Chargement des données
  // ======================================================================

  /**
   * Charge un fichier Covertype (UCI ML Repository — même source que la thèse,
   * §5.2, table 5.2 : "581,012 points in 54 dimensions"). Le fichier brut a 55
   * colonnes (54 attributs + 1 label de classe à la fin) ; on ne garde que les 54
   * premières.
   *
   * Réutilise `Point.parse` (couche core, déjà testé par `KMeansSpec`) plutôt que
   * de redévelopper une validation numérique : on tronque juste chaque ligne aux
   * `dim` premières colonnes avant de la lui passer.
   *
   * `maxPoints` permet de charger un sous-échantillon — utilisé pour un test de
   * fumée rapide avant de lancer le protocole complet sur les 581k points.
   */
  def loadCovertype(path: String, dim: Int = 54, maxPoints: Option[Int] = None): Array[Point] = {
    val src = Source.fromFile(path)
    try {
      val lines = maxPoints match {
        case Some(n) => src.getLines().take(n)
        case None    => src.getLines()
      }
      lines.flatMap { line =>
        val cols = line.trim.split(",")
        if (cols.length < dim) None
        else Point.parse(cols.take(dim).mkString(","), dim, ',')
      }.toArray
    } finally src.close()
  }

  // ======================================================================
  // Les trois méthodes comparées — aucune logique de coût réimplémentée ici
  // ======================================================================

  /**
   * Référence séquentielle (E2 / `demo.Main`) : un seul `BucketManager`, tout le
   * flux d'un coup, sur un seul thread. C'est la "vérité de référence" à laquelle
   * on compare la version distribuée (miroir des figures 5.3-5.7 de la thèse).
   */
  def sequentialCost(points: Array[Point], k: Int, m: Int, seed: Long): Double = {
    val bm = new BucketManager(m, seed)
    bm.insertAll(points)
    val model = KMeans.fit(bm.extractCoreset(), k, nRestarts = 5, seed = seed)
    KMeans.cost(points, model.centers)
  }

  /**
   * Notre implémentation distribuée : `mergeCoresets` (E4) pour construire le
   * coreset, `KMeans.fit` (E1) pour le clustering, puis `CostEvaluator.cost` (E5,
   * réutilisé tel quel — pas de recalcul du coût "à la main") pour évaluer le
   * résultat sur l'ensemble des données, en parallèle.
   */
  def distributedCost(rdd: RDD[Point], k: Int, m: Int, seed: Long, numPartitions: Int): Double = {
    // Partitions toujours fixées explicitement (METHODO §4.0), jamais laissées à
    // Spark — condition nécessaire pour que les futures expériences de
    // scalabilité isolent bien l'effet du partitionnement.
    val partitioned = rdd.repartition(numPartitions)
    val coreset = StreamKMPlusPlus.mergeCoresets(partitioned, StreamKMParams(k = k, m = m, seed = seed))
    val model   = KMeans.fit(coreset, k, nRestarts = 5, seed = seed)
    CostEvaluator.cost(partitioned, model.centers)
  }

  /**
   * Référence EXTERNE : Spark MLlib `KMeans`, entraîné directement sur le dataset
   * complet (pas sur un coreset — c'est tout l'intérêt de la comparaison : montrer
   * ce que coûte, ou ce que fait gagner, le passage par un coreset). Le DataFrame
   * est construit explicitement via un schéma (`SQLDataTypes.VectorType`) plutôt
   * que via `Seq(...).toDF()`, pour garder le contrôle du nombre de partitions —
   * même exigence que pour `distributedCost`.
   */
  def mllibCost(spark: SparkSession, points: Array[Point], k: Int, seed: Long, numPartitions: Int): Double = {
    val schema = StructType(Seq(StructField("features", SQLDataTypes.VectorType, nullable = false)))
    val rowRdd = spark.sparkContext
      .parallelize(points, numPartitions)
      .map(p => SqlRow(Vectors.dense(p.coords)))
    val df = spark.createDataFrame(rowRdd, schema)

    val model = new MLKMeans().setK(k).setSeed(seed).setFeaturesCol("features").fit(df)
    model.summary.trainingCost // WSSSE — même définition que KMeans.cost (SSE non pondérée)
  }

  // ======================================================================
  // Expérience 1+2 : non-régression (vs séquentiel) + comparaison MLlib, à k variable
  // Miroir des figures 5.3-5.7 de la thèse (un graphique par dataset, une barre par méthode).
  // ======================================================================

  def runKSweep(
    spark:         SparkSession,
    datasetName:   String,
    points:        Array[Point],
    ks:            Seq[Int],
    numPartitions: Int,
    seed:          Long
  ): Seq[ResultRow] = {
    val rdd = spark.sparkContext.parallelize(points, numPartitions)
    ks.flatMap { k =>
      val m = BucketManager.recommendedM(k) // 200·k, convention du projet (thèse §2.3.4)
      println(s"[QualityMetrics] k-sweep : k=$k, m=$m ...")

      val seq  = sequentialCost(points, k, m, seed)
      val dist = distributedCost(rdd, k, m, seed, numPartitions)
      val mll  = mllibCost(spark, points, k, seed, numPartitions)

      Seq(
        ResultRow(datasetName, "Séquentiel (E2)",                       k, m, seq),
        ResultRow(datasetName, "Spark distribué (notre implémentation)", k, m, dist),
        ResultRow(datasetName, "MLlib KMeans (batch, sans coreset)",     k, m, mll)
      )
    }
  }

  // ======================================================================
  // Expérience 3 : trade-off qualité vs taille de coreset m, à k fixe
  // Miroir de l'esprit des figures 5.1-5.2 de la thèse (axe coût uniquement —
  // l'axe temps est couvert par les benchmarks JMH d'un autre membre du groupe).
  // ======================================================================

  def runMSweep(
    spark:         SparkSession,
    datasetName:   String,
    points:        Array[Point],
    k:             Int,
    ms:            Seq[Int],
    numPartitions: Int,
    seed:          Long
  ): Seq[ResultRow] = {
    val rdd = spark.sparkContext.parallelize(points, numPartitions)
    ms.flatMap { m =>
      println(s"[QualityMetrics] m-sweep : m=$m (k=$k) ...")
      val seq  = sequentialCost(points, k, m, seed)
      val dist = distributedCost(rdd, k, m, seed, numPartitions)
      Seq(
        ResultRow(datasetName, "Séquentiel (E2)",                       k, m, seq),
        ResultRow(datasetName, "Spark distribué (notre implémentation)", k, m, dist)
      )
    }
  }

  // ======================================================================
  // Écriture CSV
  // ======================================================================

  def writeCsv(path: String, rows: Seq[ResultRow]): Unit = {
    val file = new File(path)
    Option(file.getParentFile).foreach(_.mkdirs())
    // UTF-8 explicite : PrintWriter(File) utilise sinon l'encodage par défaut de la
    // plateforme (Cp1252 sous Windows), qui corrompt les accents ("Séquentiel" devient
    // "S?quentiel"). Constaté sur le test de fumée du 2026-08-22.
    val pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8))
    try {
      pw.println("dataset,method,k,m,cost")
      rows.foreach(r => pw.println(s"${r.dataset},${r.method},${r.k},${r.m},${r.cost}"))
    } finally pw.close()
  }

  // ======================================================================
  // Point d'entrée
  // ======================================================================

  /**
   * Usage : `sbt "runMain streamkm.experiments.QualityMetrics [chemin] [maxPoints]"`
   *   - `chemin`    : fichier Covertype (défaut : data/covertype/covtype.data)
   *   - `maxPoints` : sous-échantillon pour un test rapide (défaut : tout charger)
   */
  def main(args: Array[String]): Unit = {
    val path          = if (args.length > 0) args(0) else "data/covertype/covtype.data"
    val maxPoints     = if (args.length > 1 && args(1).toInt > 0) Some(args(1).toInt) else None
    val seed          = 2026L
    val numPartitions = 8

    val spark = SparkSession.builder()
      .appName("QualityMetrics")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      println(s"[QualityMetrics] chargement de $path (maxPoints=$maxPoints)...")
      val points = loadCovertype(path, dim = 54, maxPoints = maxPoints)
      println(s"[QualityMetrics] ${points.length} points chargés.")

      // k = [10,20,30,40,50] : reproduit exactement la grille de la thèse pour
      // Covertype (§5.2.2, figure 5.5).
      val kSweepRows = runKSweep(spark, "Covertype", points, Seq(10, 20, 30, 40, 50), numPartitions, seed)
      writeCsv("results/quality_k_sweep_covertype.csv", kSweepRows)

      // k=25 fixe (comme la courbe médiane de la thèse fig. 5.1), m variable.
      val mSweepRows = runMSweep(spark, "Covertype", points, k = 25, Seq(1000, 5000, 10000, 20000, 50000), numPartitions, seed)
      writeCsv("results/quality_m_sweep_covertype.csv", mSweepRows)

      println("[QualityMetrics] terminé.")
      println("  -> results/quality_k_sweep_covertype.csv")
      println("  -> results/quality_m_sweep_covertype.csv")
    } finally {
      spark.stop()
    }
  }
}
