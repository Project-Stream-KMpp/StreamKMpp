package streamkm.experiments

import java.io.{File, FileOutputStream, OutputStreamWriter, PrintWriter}
import java.nio.charset.StandardCharsets

import scala.io.Source
import scala.util.Try

import org.apache.spark.ml.clustering.{KMeans => MLKMeans}
import org.apache.spark.ml.linalg.{SQLDataTypes, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row => SqlRow, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.storage.StorageLevel

import streamkm.core.{KMeans, Point}
import streamkm.coreset.BucketManager
import streamkm.spark.{CostEvaluator, StreamKMParams, StreamKMPlusPlus}


/**
 * Experiments de qualite de StreamKM++ sur Covertype.
 *
 * Trois approches sont comparees :
 *
 *   1. StreamKM++ sequentiel ;
 *   2. StreamKM++ distribue avec Spark ;
 *   3. Spark MLlib KMeans comme reference externe.
 *
 * La mesure de qualite est toujours la SSE evaluee sur les donnees
 * originales :
 *
 *   sum_x min_c ||x - c||^2
 *
 * Le coreset sert uniquement a calculer les centres.
 */
object QualityMetrics {

  // ---------------------------------------------------------------------------
  // Configuration commune des experiences
  // ---------------------------------------------------------------------------

  private val Dim = 54

  private val DefaultSeed = 2026L
  private val DefaultPartitions = 8

  private val NRestarts = 5
  private val MaxIter = 100
  private val Tol = 1e-6

  // Valeurs utilisees pour l'etude de l'effet du nombre de clusters.
  private val KValues =
    Seq(10, 20, 30, 40, 50)

  // Valeurs utilisees pour l'etude de la taille du coreset.
  private val MValues =
    Seq(1000, 5000, 10000, 20000)


  final case class ResultRow(
    dataset: String,
    method: String,
    k: Int,
    m: Int,
    run: Int,
    seed: Long,
    cost: Double
  )


  // ===========================================================================
  // Parsing de Covertype
  // ===========================================================================

  /**
   * Parse une ligne du dataset Covertype.
   *
   * Le fichier UCI contient 55 colonnes :
   *
   *   - 54 features ;
   *   - 1 label de classe.
   *
   * Comme nous faisons du clustering non supervise, le label n'est pas utilise.
   */
  private def parseCovertypeLine(
  line: String,
  dim: Int
): Option[Point] = {

    val columns = line.trim.split(",")

    if (columns.length < dim) {
      None
    } else {
      Point.parse(
        columns.take(dim).mkString(","),
        dim
      )
    }
  }


  // ===========================================================================
  // Lecture locale / sequentielle
  // ===========================================================================

  /**
   * Ouvre Covertype et fournit un Iterator[Point].
   *
   * Contrairement a l'ancienne implementation, l'ensemble du dataset
   * n'est PAS charge dans un Array[Point].
   *
   * Les points sont lus progressivement depuis le fichier.
   *
   * maxPoints est principalement utilise pour les smoke tests :
   *
   *   Some(20000) -> premiers 20 000 points
   *   None        -> dataset complet
   */
  private def withPointsFromFile[A](
    path: String,
    dim: Int,
    maxPoints: Option[Int]
  )(
    f: Iterator[Point] => A
  ): A = {

    val source = Source.fromFile(path)

    try {

      val allLines = source.getLines()

      val selectedLines =
        maxPoints match {
          case Some(n) => allLines.take(n)
          case None    => allLines
        }

      val points =
        selectedLines.flatMap { line =>
          parseCovertypeLine(line, dim)
        }

      f(points)

    } finally {
      source.close()
    }
  }

/**
 * Charge une petite partie de Covertype dans un Array.
 *
 * Cette methode est conservee principalement pour les tests de validation
 * et les petits jeux de donnees.
 *
 * Pour les experiences Spark sur le dataset complet, on utilise
 * loadCovertypeRDD afin d'eviter de charger toutes les donnees sur le driver.
 */
def loadCovertype(
  path: String,
  dim: Int = Dim,
  maxPoints: Option[Int] = None
): Array[Point] = {

  withPointsFromFile(
    path,
    dim,
    maxPoints
  ) { points =>
    points.toArray
  }
}

  /**
   * Calcule la SSE d'un ensemble de centres sur un fichier Covertype
   * sans charger tout le dataset en memoire.
   *
   * Les points sont lus par blocs.
   *
   * On reutilise KMeans.cost afin de ne pas dupliquer la formule
   * mathematique de la SSE.
   */
  private def costFromFile(
    path: String,
    centers: Array[Array[Double]],
    dim: Int,
    maxPoints: Option[Int],
    chunkSize: Int = 4096
  ): Double = {

    withPointsFromFile(
      path,
      dim,
      maxPoints
    ) { points =>

      points
        .grouped(chunkSize)
        .map { chunk =>
          KMeans.cost(
            chunk.toArray,
            centers
          )
        }
        .sum
    }
  }


  // ===========================================================================
  // Lecture distribuee Spark
  // ===========================================================================

  /**
   * Charge directement Covertype avec Spark.
   *
   * C'est la difference principale avec l'ancienne version :
   *
   * AVANT :
   *
   *   Source.fromFile
   *       -> Array[Point]
   *       -> sc.parallelize(...)
   *
   * MAINTENANT :
   *
   *   sc.textFile(...)
   *       -> RDD[String]
   *       -> RDD[Point]
   *
   * Les donnees ne transitent donc plus par un gros Array sur le driver.
   */
  def loadCovertypeRDD(
    spark: SparkSession,
    path: String,
    dim: Int = Dim,
    numPartitions: Int = DefaultPartitions,
    maxPoints: Option[Int] = None
  ): RDD[Point] = {

    val lines =
      spark.sparkContext.textFile(
        path,
        numPartitions
      )

    /*
     * Pour le dataset complet, aucune operation supplementaire
     * n'est necessaire.
     *
     * Pour un smoke test avec maxPoints, zipWithIndex permet de
     * conserver les N premieres lignes sans les collecter sur le driver.
     */
    val selectedLines =
      maxPoints match {

        case Some(n) =>
          lines
            .zipWithIndex()
            .filter {
              case (_, index) =>
                index < n.toLong
            }
            .keys

        case None =>
          lines
      }

    val parsed =
      selectedLines.flatMap { line =>
        parseCovertypeLine(line, dim)
      }

    /*
     * textFile utilise numPartitions comme minimum.
     * On garantit ici explicitement le nombre de partitions utilise
     * par notre protocole experimental.
     *
     * Cette repartition n'est faite qu'UNE SEULE FOIS au chargement,
     * et non a chaque valeur de k ou m.
     */
    if (parsed.getNumPartitions == numPartitions) {
      parsed
    } else {
      parsed.repartition(numPartitions)
    }
  }


  // ===========================================================================
  // 1. StreamKM++ sequentiel
  // ===========================================================================

  /**
   * Version de reference sequentielle.
   *
   * Etape 1 :
   * lecture du fichier point par point et construction du coreset.
   *
   * Etape 2 :
   * k-means++ est applique au coreset.
   *
   * Etape 3 :
   * le fichier est relu afin de calculer la SSE sur les donnees originales.
   *
   * La memoire utilisee ne depend donc pas directement du nombre total
   * de points du dataset.
   */
  def sequentialCostFromFile(
    path: String,
    k: Int,
    m: Int,
    seed: Long,
    dim: Int = Dim,
    maxPoints: Option[Int] = None
  ): Double = {

    val manager =
      new BucketManager(
        m,
        seed
      )

    // Premier passage : construction du coreset.
    withPointsFromFile(
      path,
      dim,
      maxPoints
    ) { points =>

      manager.insertAll(points)
    }

    require(
      manager.numPointsSeen > 0.0,
      s"No valid point found in $path"
    )

    val coreset =
      manager.extractCoreset()

    val model =
      KMeans.fit(
        coreset,
        k,
        nRestarts = NRestarts,
        maxIter = MaxIter,
        tol = Tol,
        seed = seed
      )

    // Deuxieme passage : cout sur les donnees completes.
    costFromFile(
      path,
      model.centers,
      dim,
      maxPoints
    )
  }


  // ===========================================================================
  // 2. StreamKM++ distribue Spark
  // ===========================================================================

  /**
   * Version Spark de StreamKM++.
   *
   * Le RDD est deja partitionne et mis en cache dans main.
   * Il n'est donc pas repartitionne a chaque experience.
   */
  def distributedCost(
    points: RDD[Point],
    k: Int,
    m: Int,
    seed: Long
  ): Double = {

    val params =
      StreamKMParams(
        k = k,
        m = m,
        nRestarts = NRestarts,
        seed = seed
      )

    val coreset =
      StreamKMPlusPlus.mergeCoresets(
        points,
        params
      )

    val model =
      KMeans.fit(
        coreset,
        k,
        nRestarts = NRestarts,
        maxIter = MaxIter,
        tol = Tol,
        seed = seed
      )

    /*
     * Important :
     * on evalue les centres sur toutes les donnees du RDD,
     * pas seulement sur le coreset.
     */
    CostEvaluator.cost(
      points,
      model.centers
    )
  }


  // ===========================================================================
  // 3. Spark MLlib KMeans
  // ===========================================================================

  /**
   * Convertit une seule fois le RDD[Point] en DataFrame MLlib.
   *
   * Ce DataFrame sera reutilise pour toutes les valeurs de k.
   */
  private def toFeaturesDataFrame(
    spark: SparkSession,
    points: RDD[Point]
  ): DataFrame = {

    val schema =
      StructType(
        Seq(
          StructField(
            "features",
            SQLDataTypes.VectorType,
            nullable = false
          )
        )
      )

    val rows =
      points.map { point =>
        SqlRow(
          Vectors.dense(point.coords)
        )
      }

    spark.createDataFrame(
      rows,
      schema
    )
  }


  /**
   * Reference externe avec Spark MLlib.
   *
   * MLlib n'utilise pas notre coreset :
   * KMeans est entraine directement sur les donnees completes.
   *
   * Pour rendre la comparaison plus stable, plusieurs initialisations
   * independantes sont testees et le meilleur cout est conserve.
   *
   * Remarque :
   * MLlib utilise son propre mecanisme d'initialisation.
   * Il s'agit donc d'une reference externe de qualite et non d'une
   * reproduction exacte de KMeans.fit.
   */
  def mllibCost(
    features: DataFrame,
    k: Int,
    seed: Long
  ): Double = {

    (0 until NRestarts)
      .map { restart =>

        /*
         * On espace volontairement les graines entre redemarrages
         * afin d'eviter que deux repetitions successives utilisent
         * presque exactement la meme serie de graines.
         */
        val restartSeed =
          seed + restart.toLong * 1000003L

        val model =
          new MLKMeans()
            .setK(k)
            .setSeed(restartSeed)
            .setFeaturesCol("features")
            .setMaxIter(MaxIter)
            .setTol(Tol)
            .fit(features)

        model.summary.trainingCost
      }
      .min
  }


  // ===========================================================================
  // Experience A : effet de k
  // ===========================================================================

  /**
   * Fait varier le nombre de clusters k.
   *
   * Pour chaque valeur :
   *
   *   - m = 200 * k ;
   *   - StreamKM++ sequentiel ;
   *   - StreamKM++ Spark ;
   *   - Spark MLlib.
   */
  def runKSweep(
    spark: SparkSession,
    path: String,
    datasetName: String,
    pointsRDD: RDD[Point],
    featuresDF: DataFrame,
    ks: Seq[Int],
    baseSeed: Long,
    nRepeats: Int,
    maxPoints: Option[Int]
  ): Seq[ResultRow] = {

    ks.flatMap { k =>

      val m =
        BucketManager.recommendedM(k)

      (1 to nRepeats).flatMap { run =>

        val seed =
          baseSeed + run - 1

        println(
          s"[QualityMetrics] k-sweep: " +
            s"k=$k m=$m run=$run/$nRepeats seed=$seed"
        )

        val sequential =
          sequentialCostFromFile(
            path = path,
            k = k,
            m = m,
            seed = seed,
            maxPoints = maxPoints
          )

        val distributed =
          distributedCost(
            pointsRDD,
            k,
            m,
            seed
          )

        val mllib =
          mllibCost(
            featuresDF,
            k,
            seed
          )

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
  }


  // ===========================================================================
  // Experience B : effet de m
  // ===========================================================================

  /**
   * Fait varier la taille du coreset m avec k fixe.
   */
  def runMSweep(
    path: String,
    datasetName: String,
    pointsRDD: RDD[Point],
    k: Int,
    ms: Seq[Int],
    baseSeed: Long,
    nRepeats: Int,
    maxPoints: Option[Int]
  ): Seq[ResultRow] = {

    ms.flatMap { m =>

      (1 to nRepeats).flatMap { run =>

        val seed =
          baseSeed + run - 1

        println(
          s"[QualityMetrics] m-sweep: " +
            s"k=$k m=$m run=$run/$nRepeats seed=$seed"
        )

        val sequential =
          sequentialCostFromFile(
            path = path,
            k = k,
            m = m,
            seed = seed,
            maxPoints = maxPoints
          )

        val distributed =
          distributedCost(
            pointsRDD,
            k,
            m,
            seed
          )

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
  }


  // ===========================================================================
  // CSV
  // ===========================================================================

  /**
   * Echappe correctement les champs CSV contenant des virgules,
   * des guillemets ou des retours a la ligne.
   */
  private def csvField(
    value: String
  ): String = {

    if (
      value.exists { c =>
        c == ',' ||
        c == '"' ||
        c == '\n' ||
        c == '\r'
      }
    ) {

      "\"" +
        value.replace("\"", "\"\"") +
        "\""

    } else {
      value
    }
  }


  /**
   * Ecrit les resultats dans un fichier CSV unique.
   */
  def writeCsv(
    path: String,
    rows: Seq[ResultRow]
  ): Unit = {

    val file =
      new File(path)

    Option(
      file.getParentFile
    ).foreach(
      _.mkdirs()
    )

    val writer =
      new PrintWriter(
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


  // ===========================================================================
  // Arguments
  // ===========================================================================

  /**
   * Lit un entier positif depuis les arguments.
   *
   * Une valeur <= 0 est consideree comme absente.
   */
  private def positiveInt(
    args: Array[String],
    index: Int
  ): Option[Int] = {

    args
      .lift(index)
      .flatMap { value =>
        Try(value.toInt).toOption
      }
      .filter(_ > 0)
  }


  // ===========================================================================
  // Main
  // ===========================================================================

  /**
   * Utilisation :
   *
   * Smoke test 20 000 points :
   *
   * sbt "runMain streamkm.experiments.QualityMetrics \
   * data/covertype/covtype.data 20000 1"
   *
   *
   * Dataset complet, une repetition :
   *
   * sbt "runMain streamkm.experiments.QualityMetrics \
   * data/covertype/covtype.data 0 1"
   *
   *
   * Arguments :
   *
   * args(0) : chemin du fichier
   *
   * args(1) : nombre maximal de points
   *           0 = dataset complet
   *
   * args(2) : nombre de repetitions
   */
  def main(
    args: Array[String]
  ): Unit = {

    val path =
      args.headOption.getOrElse(
        "data/covertype/covtype.data"
      )

    val maxPoints =
      positiveInt(
        args,
        1
      )

    val nRepeats =
      positiveInt(
        args,
        2
      ).getOrElse(1)


    val spark =
      SparkSession
        .builder()
        .appName(
          "StreamKM++ Quality Experiments"
        )
        .master("local[*]")
        .config(
          "spark.ui.enabled",
          "false"
        )
        .config(
          "spark.sql.shuffle.partitions",
          DefaultPartitions.toString
        )
        .getOrCreate()


    // Les logs Spark restent tres bavards.
    // WARN suffit pour nos experiences locales.
    spark.sparkContext.setLogLevel("WARN")


    try {

      println(
        s"[QualityMetrics] Loading $path"
      )

      println(
        s"[QualityMetrics] maxPoints=$maxPoints"
      )

      println(
        s"[QualityMetrics] partitions=$DefaultPartitions"
      )


      // -----------------------------------------------------------------------
      // Chargement distribue direct du fichier
      // -----------------------------------------------------------------------

      val pointsRDD =
        loadCovertypeRDD(
          spark = spark,
          path = path,
          dim = Dim,
          numPartitions = DefaultPartitions,
          maxPoints = maxPoints
        )
          .persist(
            StorageLevel.MEMORY_AND_DISK
          )


      try {

        /*
         * count() force le chargement du RDD.
         *
         * A partir de ce moment, les experiences reutilisent les donnees
         * mises en cache plutot que de relire le fichier a chaque operation Spark.
         */
        val numberOfPoints =
          pointsRDD.count()


        require(
          numberOfPoints > 0,
          s"No valid point loaded from $path"
        )


        println(
          s"[QualityMetrics] $numberOfPoints points loaded."
        )

        println(
          s"[QualityMetrics] ${pointsRDD.getNumPartitions} Spark partitions."
        )


        // ---------------------------------------------------------------------
        // DataFrame MLlib construit une seule fois
        // ---------------------------------------------------------------------

        val featuresDF =
          toFeaturesDataFrame(
            spark,
            pointsRDD
          )
            .persist(
              StorageLevel.MEMORY_AND_DISK
            )


        try {

          /*
           * Force le cache du DataFrame avant les differents appels MLlib.
           */
          featuresDF.count()


          // ===================================================================
          // Experience k-sweep
          // ===================================================================

          val kSweepRows =
            runKSweep(
              spark = spark,
              path = path,
              datasetName = "Covertype",
              pointsRDD = pointsRDD,
              featuresDF = featuresDF,
              ks = KValues,
              baseSeed = DefaultSeed,
              nRepeats = nRepeats,
              maxPoints = maxPoints
            )


          writeCsv(
            "results/quality_k_sweep_covertype.csv",
            kSweepRows
          )


          // ===================================================================
          // Experience m-sweep
          // ===================================================================

          val mSweepRows =
            runMSweep(
              path = path,
              datasetName = "Covertype",
              pointsRDD = pointsRDD,
              k = 25,
              ms = MValues,
              baseSeed = DefaultSeed,
              nRepeats = nRepeats,
              maxPoints = maxPoints
            )


          writeCsv(
            "results/quality_m_sweep_covertype.csv",
            mSweepRows
          )


          println(
            "[QualityMetrics] Done."
          )

          println(
            "  -> results/quality_k_sweep_covertype.csv"
          )

          println(
            "  -> results/quality_m_sweep_covertype.csv"
          )


        } finally {

          // Libere le cache du DataFrame MLlib.
          featuresDF.unpersist()
        }


      } finally {

        // Libere le RDD Covertype du cache Spark.
        pointsRDD.unpersist()
      }


    } finally {

      spark.stop()
    }
  }
}