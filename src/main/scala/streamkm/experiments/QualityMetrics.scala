package streamkm.experiments

import java.io.{File, FileOutputStream, OutputStreamWriter, PrintWriter}
import java.nio.charset.StandardCharsets

import scala.io.Source
import scala.util.Try

import org.apache.spark.ml.clustering.{KMeans => MLKMeans}
import org.apache.spark.ml.linalg.{SQLDataTypes, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row => SqlRow, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}

import streamkm.core.{KMeans, Point}
import streamkm.coreset.BucketManager
import streamkm.spark.{CostEvaluator, StreamKMParams, StreamKMPlusPlus}

/**
 * Experiments de qualite pour StreamKM++ sur le dataset Covertype.
 *
 * Trois approches sont comparees :
 *   1. StreamKM++ sequentiel ;
 *   2. StreamKM++ distribue avec Spark ;
 *   3. K-Means de Spark MLlib utilise comme reference externe.
 *
 * La mesure de qualite est la SSE :
 *   sum_x min_c ||x - c||^2
 *
 * Les couts sont toujours evalues sur les donnees originales,
 * et non uniquement sur le coreset.
 */
object QualityMetrics {

  private val Dim = 54
  private val DefaultSeed = 2026L
  private val DefaultPartitions = 8

  private val NRestarts = 5
  private val MaxIter = 100
  private val Tol = 1e-6

  private val KValues = Seq(10, 20, 30, 40, 50)

  // Valeurs coherentes avec l'experience Covertype de la these.
  private val MValues = Seq(1000, 5000, 10000, 20000)

  final case class ResultRow(
    dataset: String,
    method: String,
    k: Int,
    m: Int,
    run: Int,
    seed: Long,
    cost: Double
  )

  // ---------------------------------------------------------------------------
  // Chargement de Covertype
  // ---------------------------------------------------------------------------

  /**
   * Charge Covertype.
   *
   * Le fichier contient 55 colonnes :
   *   - 54 attributs ;
   *   - 1 label de classe.
   *
   * Le label n'est pas utilise pour le clustering.
   *
   * maxPoints permet de limiter le nombre de lignes pour les tests rapides.
   */
  def loadCovertype(
    path: String,
    dim: Int = Dim,
    maxPoints: Option[Int] = None
  ): Array[Point] = {

    val source = Source.fromFile(path)

    try {
      val lines = maxPoints match {
        case Some(n) => source.getLines().take(n)
        case None    => source.getLines()
      }

      lines.flatMap { line =>
        val columns = line.trim.split(",")

        if (columns.length < dim) {
          None
        } else {
          Point.parse(columns.take(dim).mkString(","), dim)
        }
      }.toArray

    } finally {
      source.close()
    }
  }

  // ---------------------------------------------------------------------------
  // StreamKM++ sequentiel
  // ---------------------------------------------------------------------------

  /**
   * Execute StreamKM++ sur un seul thread.
   *
   * Le clustering est construit sur le coreset, puis le cout final est
   * calcule sur toutes les donnees originales.
   */
  def sequentialCost(
    points: Array[Point],
    k: Int,
    m: Int,
    seed: Long
  ): Double = {

    val manager = new BucketManager(m, seed)
    manager.insertAll(points)

    val coreset = manager.extractCoreset()

    val model = KMeans.fit(
      coreset,
      k,
      nRestarts = NRestarts,
      maxIter = MaxIter,
      tol = Tol,
      seed = seed
    )

    KMeans.cost(points, model.centers)
  }

  // ---------------------------------------------------------------------------
  // StreamKM++ Spark
  // ---------------------------------------------------------------------------

  /**
   * Execute la version distribuee de StreamKM++.
   */
  def distributedCost(
    rdd: RDD[Point],
    k: Int,
    m: Int,
    seed: Long,
    numPartitions: Int
  ): Double = {

    // Evite un shuffle inutile si le RDD possede deja
    // le nombre de partitions souhaite.
    val partitioned =
      if (rdd.getNumPartitions == numPartitions) rdd
      else rdd.repartition(numPartitions)

    val params = StreamKMParams(
      k = k,
      m = m,
      nRestarts = NRestarts,
      seed = seed
    )

    val coreset =
      StreamKMPlusPlus.mergeCoresets(partitioned, params)

    val model = KMeans.fit(
      coreset,
      k,
      nRestarts = NRestarts,
      maxIter = MaxIter,
      tol = Tol,
      seed = seed
    )

    CostEvaluator.cost(partitioned, model.centers)
  }

  // ---------------------------------------------------------------------------
  // Spark MLlib
  // ---------------------------------------------------------------------------

  /**
   * Reference externe utilisant Spark MLlib.
   *
   * Pour rendre la comparaison plus equitable, MLlib utilise :
   *   - le meme nombre de redemarrages ;
   *   - le meme maximum d'iterations ;
   *   - la meme tolerance.
   *
   * On conserve le meilleur cout parmi les redemarrages.
   */
  def mllibCost(
    spark: SparkSession,
    points: Array[Point],
    k: Int,
    seed: Long,
    numPartitions: Int
  ): Double = {

    val schema = StructType(
      Seq(
        StructField(
          "features",
          SQLDataTypes.VectorType,
          nullable = false
        )
      )
    )

    val rows = spark.sparkContext
      .parallelize(points, numPartitions)
      .map(point => SqlRow(Vectors.dense(point.coords)))

    val df = spark.createDataFrame(rows, schema).cache()

    try {
      (0 until NRestarts).map { restart =>

        val model = new MLKMeans()
          .setK(k)
          .setSeed(seed + restart)
          .setFeaturesCol("features")
          .setMaxIter(MaxIter)
          .setTol(Tol)
          .fit(df)

        model.summary.trainingCost

      }.min

    } finally {
      df.unpersist()
    }
  }

  // ---------------------------------------------------------------------------
  // Experience 1 : variation de k
  // ---------------------------------------------------------------------------

  def runKSweep(
    spark: SparkSession,
    datasetName: String,
    points: Array[Point],
    ks: Seq[Int],
    numPartitions: Int,
    baseSeed: Long,
    nRepeats: Int = 1
  ): Seq[ResultRow] = {

    val rdd =
      spark.sparkContext.parallelize(points, numPartitions).cache()

    try {
      ks.flatMap { k =>

        val m = BucketManager.recommendedM(k)

        (1 to nRepeats).flatMap { run =>

          val seed = baseSeed + run - 1

          println(
            s"[QualityMetrics] k-sweep: " +
              s"k=$k m=$m run=$run/$nRepeats seed=$seed"
          )

          val sequential =
            sequentialCost(points, k, m, seed)

          val distributed =
            distributedCost(rdd, k, m, seed, numPartitions)

          val mllib =
            mllibCost(spark, points, k, seed, numPartitions)

          Seq(
            ResultRow(
              datasetName,
              "Sequential StreamKM++",
              k,
              m,
              run,
              seed,
              sequential
            ),
            ResultRow(
              datasetName,
              "Spark StreamKM++",
              k,
              m,
              run,
              seed,
              distributed
            ),
            ResultRow(
              datasetName,
              "Spark MLlib KMeans",
              k,
              m,
              run,
              seed,
              mllib
            )
          )
        }
      }

    } finally {
      rdd.unpersist()
    }
  }

  // ---------------------------------------------------------------------------
  // Experience 2 : variation de la taille du coreset
  // ---------------------------------------------------------------------------

  def runMSweep(
    spark: SparkSession,
    datasetName: String,
    points: Array[Point],
    k: Int,
    ms: Seq[Int],
    numPartitions: Int,
    baseSeed: Long,
    nRepeats: Int = 1
  ): Seq[ResultRow] = {

    val rdd =
      spark.sparkContext.parallelize(points, numPartitions).cache()

    try {
      ms.flatMap { m =>

        (1 to nRepeats).flatMap { run =>

          val seed = baseSeed + run - 1

          println(
            s"[QualityMetrics] m-sweep: " +
              s"k=$k m=$m run=$run/$nRepeats seed=$seed"
          )

          val sequential =
            sequentialCost(points, k, m, seed)

          val distributed =
            distributedCost(rdd, k, m, seed, numPartitions)

          Seq(
            ResultRow(
              datasetName,
              "Sequential StreamKM++",
              k,
              m,
              run,
              seed,
              sequential
            ),
            ResultRow(
              datasetName,
              "Spark StreamKM++",
              k,
              m,
              run,
              seed,
              distributed
            )
          )
        }
      }

    } finally {
      rdd.unpersist()
    }
  }

  // ---------------------------------------------------------------------------
  // CSV
  // ---------------------------------------------------------------------------

  private def csvField(value: String): String = {
    if (
      value.exists(c =>
        c == ',' || c == '"' || c == '\n' || c == '\r'
      )
    ) {
      "\"" + value.replace("\"", "\"\"") + "\""
    } else {
      value
    }
  }

  def writeCsv(
    path: String,
    rows: Seq[ResultRow]
  ): Unit = {

    val file = new File(path)

    Option(file.getParentFile).foreach(_.mkdirs())

    val writer = new PrintWriter(
      new OutputStreamWriter(
        new FileOutputStream(file),
        StandardCharsets.UTF_8
      )
    )

    try {
      writer.println(
        "dataset,method,k,m,run,seed,cost"
      )

      rows.foreach { row =>
        writer.println(
          Seq(
            csvField(row.dataset),
            csvField(row.method),
            row.k.toString,
            row.m.toString,
            row.run.toString,
            row.seed.toString,
            row.cost.toString
          ).mkString(",")
        )
      }

    } finally {
      writer.close()
    }
  }

  // ---------------------------------------------------------------------------
  // Main
  // ---------------------------------------------------------------------------

  private def positiveInt(
    args: Array[String],
    index: Int
  ): Option[Int] = {
    args
      .lift(index)
      .flatMap(value => Try(value.toInt).toOption)
      .filter(_ > 0)
  }

  /**
   * Usage :
   *
   * sbt "runMain streamkm.experiments.QualityMetrics \
   * data/covertype/covtype.data 20000 1"
   *
   * args(0) : chemin du dataset
   * args(1) : nombre maximal de points
   * args(2) : nombre de repetitions
   */
  def main(args: Array[String]): Unit = {

    val path =
      args.headOption.getOrElse(
        "data/covertype/covtype.data"
      )

    val maxPoints =
      positiveInt(args, 1)

    val nRepeats =
      positiveInt(args, 2).getOrElse(1)

    val spark = SparkSession
      .builder()
      .appName("StreamKM++ Quality Experiments")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      println(
        s"[QualityMetrics] Loading $path " +
          s"(maxPoints=$maxPoints)"
      )

      val points =
        loadCovertype(
          path,
          dim = Dim,
          maxPoints = maxPoints
        )

      require(
        points.nonEmpty,
        s"No valid point loaded from $path"
      )

      println(
        s"[QualityMetrics] ${points.length} points loaded."
      )

      // ------------------------------------------------------------
      // k-sweep
      // ------------------------------------------------------------

      val kSweepRows =
        runKSweep(
          spark = spark,
          datasetName = "Covertype",
          points = points,
          ks = KValues,
          numPartitions = DefaultPartitions,
          baseSeed = DefaultSeed,
          nRepeats = nRepeats
        )

      writeCsv(
        "results/quality_k_sweep_covertype.csv",
        kSweepRows
      )

      // ------------------------------------------------------------
      // m-sweep
      // ------------------------------------------------------------

      val mSweepRows =
        runMSweep(
          spark = spark,
          datasetName = "Covertype",
          points = points,
          k = 25,
          ms = MValues,
          numPartitions = DefaultPartitions,
          baseSeed = DefaultSeed,
          nRepeats = nRepeats
        )

      writeCsv(
        "results/quality_m_sweep_covertype.csv",
        mSweepRows
      )

      println("[QualityMetrics] Done.")
      println(
        "  -> results/quality_k_sweep_covertype.csv"
      )
      println(
        "  -> results/quality_m_sweep_covertype.csv"
      )

    } finally {
      spark.stop()
    }
  }
}