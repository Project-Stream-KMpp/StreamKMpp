package streamkm.spark

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import streamkm.TestData
import streamkm.core.{KMeans, KMeansModel}
import streamkm.coreset.BucketManager

/**
 * Modèle de composition d'erreur pour `treeReduce` (thèse §2.3.4, observation 2 :
 * « un ε-coreset d'un δ-coreset est un (ε+δ+εδ)-coreset », c.-à-d. (1+ε)(1+δ)-1 —
 * l'erreur se COMPOSE multiplicativement, pas additivement).
 *
 * IMPORTANT — limite honnête : la thèse ne donne aucune formule fermée pour l'ε d'un
 * coreset produit par `CoresetTree` (§2.3.4, point 3 : « the authors do not prove that
 * the coreset tree structure is indeed a (k,ε)-coreset »). Le seul ε chiffré de la thèse
 * concerne `AdaptiveCoreset` (théorème 2.1), avec une constante O(d) non précisée — donc
 * inutilisable numériquement. Ce qu'on PEUT dériver rigoureusement, c'est la loi de
 * composition (multiplicative) et le nombre de niveaux de composition qu'introduit
 * `treeReduce`. Pour la magnitude d'un niveau isolé, on réutilise la seule valeur dont on
 * dispose : l'ε empirique déjà validé par `CoresetSpec` ("le coreset préserve la qualité
 * du clustering", 1 seul niveau de réduction, tolérance choisie à 10 % dans ce repo).
 */
private[spark] object CoresetErrorModel {

  /**
   * Facteur de regroupement utilisé par `RDD.treeReduce`/`treeAggregate` en interne
   * (implémentation Spark : `scale = max(2, ceil(numPartitions^(1/depth)))`).
   */
  def scaleFactor(numPartitions: Int, depth: Int): Int =
    math.max(2, math.ceil(math.pow(numPartitions.toDouble, 1.0 / depth)).toInt)

  /**
   * Nombre MAXIMUM de fusions `BucketManager.merge` séquentielles que peut subir la
   * contribution d'un point donné, dans le pire cas, à travers un `treeReduce` de
   * profondeur `depth` sur `numPartitions` partitions.
   *
   * Dérivation : à chaque palier (il y en a au plus `depth`), Spark regroupe environ
   * `scale` éléments par clé via `reduceByKey`, ce qui replie ce groupe par une CHAÎNE de
   * `scale - 1` applications séquentielles de `combOp` (chaque application composant à
   * nouveau l'erreur, cf. observation 2 ci-dessus). D'où la borne `depth · (scale - 1)`.
   *
   * Ce n'est PAS une garantie publiée par Spark (treeReduce ne documente pas de borne de
   * composition d'erreur, pour cause : ses `U` ne sont pas des coresets en général) — c'est
   * une estimation dérivée de sa stratégie de regroupement interne, pour un pire cas.
   */
  def worstCaseCompositionLevels(numPartitions: Int, depth: Int): Int = {
    val scale = scaleFactor(numPartitions, depth)
    depth * (scale - 1)
  }

  /** (1+ε)^levels - 1 : composition multiplicative d'une erreur ε sur `levels` niveaux. */
  def compoundedTolerance(epsPerLevel: Double, levels: Int): Double =
    math.pow(1.0 + epsPerLevel, levels) - 1.0

  /**
   * Tolérance relative attendue pour comparer le coût d'un `mergeCoresets` distribué
   * (sur `numPartitions` partitions, profondeur `depth`) à une référence séquentielle à un
   * seul niveau de réduction.
   *
   * `epsPerLevel = 0.10` : repris tel quel de `CoresetSpec` (E2), qui valide déjà qu'UNE
   * réduction CoresetTree isolée dégrade le coût de moins de 10 % en pratique sur des
   * mélanges gaussiens comparables. On ne réinvente pas cette calibration ici, on la
   * compose.
   */
  def treeReduceTolerance(numPartitions: Int, depth: Int, epsPerLevel: Double = 0.10): Double =
    compoundedTolerance(epsPerLevel, worstCaseCompositionLevels(numPartitions, depth))
}

/**
 * Critères de sortie de l'étape E4.
 *
 * Le test central (« non-régression distribué vs séquentiel ») est plus
 * important ici que la conservation stricte de la masse (déjà couverte par
 * E2) : il vérifie que distribuer StreamKM++ sur Spark ne dégrade pas la
 * qualité du clustering par rapport à l'exécution séquentielle de référence
 * (`demo.Main`, ou le test "StreamKM++ séquentiel" de `CoresetSpec`).
 *
 * Les tests "état streaming" (fin de fichier) couvrent le risque le plus récent de la
 * couche Spark : le double-comptage ou la perte de masse entre micro-batches successifs
 * dans l'état `global` maintenu par `StreamKMPlusPlus.run`.
 */
class StreamKMSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("StreamKMSpec")
      .master("local[4]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  // ==================================================================
  // partialCoresets
  // ==================================================================

  test("partialCoresets : un coreset par partition, masse totale conservée") {
    val mix = TestData.gaussianMixture(n = 8000, k = 4, d = 2, seed = 31L)
    val rdd = spark.sparkContext.parallelize(mix.points, numSlices = 4)

    val params   = StreamKMParams(k = 4, m = 200)
    val partials = StreamKMPlusPlus.partialCoresets(rdd, params).collect()

    partials.length shouldBe 4
    partials.foreach(_.length should be <= 200)

    val totalMass = partials.iterator.flatten.map(_.weight).sum
    totalMass shouldBe 8000.0 +- 1e-6
  }

  // ==================================================================
  // mergeCoresets
  // ==================================================================

  test("mergeCoresets : conserve la masse totale, quel que soit le nombre de partitions") {
    val mix    = TestData.gaussianMixture(n = 10000, k = 5, d = 2, seed = 32L)
    val params = StreamKMParams(k = 5, m = BucketManager.recommendedM(5))

    for (nParts <- Seq(1, 2, 8)) {
      val rdd     = spark.sparkContext.parallelize(mix.points, numSlices = nParts)
      val coreset = StreamKMPlusPlus.mergeCoresets(rdd, params)
      withClue(s"nParts=$nParts : ") {
        coreset.map(_.weight).sum shouldBe 10000.0 +- 1e-6
        coreset.length should be <= params.m
      }
    }
  }

  // ==================================================================
  // LE TEST LE PLUS IMPORTANT : non-régression distribué vs séquentiel
  // ==================================================================

  test("mergeCoresets sur 8 partitions produit un clustering de qualité comparable au séquentiel") {
    val k          = 5
    val m          = BucketManager.recommendedM(k)
    val mix        = TestData.gaussianMixture(n = 40000, k = k, d = 2, sigma = 0.5, seed = 33L)
    val numParts   = 8
    val treeDepth  = 2 // valeur par défaut de mergeCoresets, non surchargée ici

    // Référence séquentielle : un seul BucketManager, tout le flux d'un coup
    // (même logique que le test "StreamKM++ séquentiel" de CoresetSpec, E2,
    // et que demo.Main).
    val seqBm    = new BucketManager(m, seed = 40L)
    seqBm.insertAll(mix.points)
    val seqModel = KMeans.fit(seqBm.extractCoreset(), k, nRestarts = 5, seed = 41L)
    val seqCost  = KMeans.cost(mix.points, seqModel.centers)

    // Version distribuée : mêmes données, réparties sur `numParts` partitions Spark.
    val rdd         = spark.sparkContext.parallelize(mix.points, numSlices = numParts)
    val distCoreset = StreamKMPlusPlus.mergeCoresets(rdd, StreamKMParams(k = k, m = m, seed = 40L), depth = treeDepth)
    val distModel   = KMeans.fit(distCoreset, k, nRestarts = 5, seed = 41L)
    val distCost    = KMeans.cost(mix.points, distModel.centers)

    // Tolérance DÉRIVÉE (pas codée en dur) de la loi de composition multiplicative des
    // coresets (thèse §2.3.4, observation 2) appliquée au nombre de niveaux de fusion
    // que `treeReduce` introduit réellement pour numParts=8, depth=2 (CoresetErrorModel).
    // Voir CLAUDE.md, décision 11, pour le calcul détaillé et la comparaison à
    // l'ancienne valeur codée en dur (25 %).
    val tolerance = CoresetErrorModel.treeReduceTolerance(numParts, treeDepth)

    withClue(
      s"séquentiel=$seqCost, distribué=$distCost, tolérance théorique=${tolerance * 100}%% : "
    ) {
      distCost should be <= seqCost * (1.0 + tolerance)
    }
    TestData.maxCenterDeviation(mix.trueCenters, distModel.centers) should be < 2.0
  }

  test("mergeCoresets : le coût ne se dégrade pas franchement entre 1 et 8 partitions") {
    val k   = 4
    val m   = BucketManager.recommendedM(k)
    val mix = TestData.gaussianMixture(n = 20000, k = k, d = 2, sigma = 0.5, seed = 34L)

    val params = StreamKMParams(k = k, m = m, seed = 50L)

    def costWith(nParts: Int): Double = {
      val rdd     = spark.sparkContext.parallelize(mix.points, numSlices = nParts)
      val coreset = StreamKMPlusPlus.mergeCoresets(rdd, params)
      KMeans.cost(mix.points, KMeans.fit(coreset, k, nRestarts = 5, seed = 51L).centers)
    }

    val cost1 = costWith(1)
    val cost8 = costWith(8)

    withClue(s"1 partition=$cost1, 8 partitions=$cost8 : ") {
      // La thèse observe (§5.3.1, fig. 5.9) que le coût varie peu avec le
      // parallélisme, et peut même s'améliorer légèrement (plus de listes de
      // buckets = coresets plus fins). On vérifie ici l'absence de
      // dégradation franche — même observation que E3bis du METHODO, en
      // version test de non-régression plutôt qu'expérience chiffrée.
      math.abs(cost1 - cost8) should be <= 0.3 * math.max(cost1, cost8)
    }
  }

  test("partialCoresets et mergeCoresets utilisent des graines distinctes par partition") {
    // Les deux fonctions dérivent la graine de chaque partition comme
    // `p.seed + idx` (cf. commentaire de StreamKMPlusPlus.partialCoresets).
    // On vérifie ici que deux partitions contenant les MÊMES points ne
    // produisent pas des coresets identiques point à point (ce qui trahirait
    // une graine partagée) : c'est une garantie de non-corrélation, pas une
    // propriété algorithmique de la thèse, mais elle conditionne la validité
    // de l'expérience E3bis (isoler l'effet du nombre de partitions de tout
    // effet de graine).
    val pts    = TestData.gaussianMixture(n = 2000, k = 3, d = 2, seed = 60L).points
    val rdd    = spark.sparkContext.parallelize(pts ++ pts, numSlices = 2) // 2 copies identiques
    val params = StreamKMParams(k = 3, m = 50)

    val partials = StreamKMPlusPlus.partialCoresets(rdd, params).collect()
    partials.length shouldBe 2
    // Si les deux partitions recevaient exactement les mêmes points originaux
    // avec la même graine, leurs coresets seraient identiques coordonnée à
    // coordonnée ; avec des graines distinctes, ce n'est presque sûrement pas
    // le cas dès qu'un tirage aléatoire intervient (m=50 << n/2=2000, donc le
    // coreset tree tire bien des échantillons aléatoires).
    partials(0).map(_.coords.toSeq).toSet should not equal partials(1).map(_.coords.toSeq).toSet
  }

  // ==================================================================
  // ÉTAT STREAMING DANS LE TEMPS : le point le plus récent et le plus à risque
  // (double-comptage / perte de masse entre micro-batches successifs)
  // ==================================================================

  test(
    "état streaming : l'insertion multi micro-batches conserve exactement la masse, " +
    "sans double-comptage ni perte"
  ) {
    // Reproduit exactement la boucle interne de StreamKMPlusPlus.run (mergeCoresets par
    // batch, puis global.insert des représentants), mais avec accès direct à
    // `global.numPointsSeen` — ce que le callback `onModel: KMeansModel => Unit` de `run`
    // n'expose pas. C'est le test le plus précis pour la classe de bug visée : si un point
    // était compté deux fois (ou perdu) entre deux micro-batches, `numPointsSeen`
    // décrocherait de la somme cumulée AU MOMENT MÊME où ça se produit — vérifié après
    // CHAQUE batch, pas seulement à la fin (une compensation entre un batch qui perd de la
    // masse et un autre qui en gagne resterait invisible si on ne vérifiait qu'à la fin).
    val k   = 4
    val m   = BucketManager.recommendedM(k)
    val mix = TestData.gaussianMixture(n = 30000, k = k, d = 2, sigma = 0.5, seed = 70L)

    val chunks = mix.points.grouped(10000).toArray
    chunks.length shouldBe 3

    val global = new BucketManager(m, seed = 71L)
    var cumulativeExpected = 0.0

    chunks.zipWithIndex.foreach { case (chunk, batchId) =>
      val rdd     = spark.sparkContext.parallelize(chunk, numSlices = 4)
      val partial = StreamKMPlusPlus.mergeCoresets(rdd, StreamKMParams(k = k, m = m, seed = 71L + batchId))
      partial.foreach(global.insert)

      cumulativeExpected += chunk.length.toDouble
      withClue(s"après le micro-batch $batchId : ") {
        global.numPointsSeen shouldBe cumulativeExpected +- 1e-6
      }
    }

    TestData.totalMass(global.extractCoreset()) shouldBe 30000.0 +- 1e-6
  }

  // IGNORÉ sous Windows sans winutils.exe/HADOOP_HOME : Structured Streaming a besoin
  // d'écrire les métadonnées de checkpoint sur le système de fichiers local, ce qui
  // passe par RawLocalFileSystem.setPermission → Shell.getWinUtilsPath, absent de cette
  // machine (limitation d'environnement Windows connue de Hadoop, sans rapport avec le
  // code de ce projet). Voir CLAUDE.md, décision 13. Fonctionnera tel quel, sans aucune
  // modification, sur Linux/macOS ou dans un conteneur Docker — à réactiver (`test` au
  // lieu d'`ignore`) dès que l'un de ces environnements est disponible pour lancer la
  // suite.
  ignore("run() sur plusieurs micro-batches produit une qualité comparable à un traitement en un seul bloc") {
    // Test d'intégration bout-en-bout de `run`, via un MemoryStream Spark piloté
    // manuellement (addData + processAllAvailable) pour contrôler précisément les
    // frontières de micro-batch — c'est le seul test qui exerce réellement
    // `StreamKMPlusPlus.run` (les autres tests appellent `mergeCoresets` directement).
    val k   = 4
    val m   = BucketManager.recommendedM(k)
    val mix = TestData.gaussianMixture(n = 30000, k = k, d = 2, sigma = 0.5, seed = 72L)
    val chunks = mix.points.grouped(10000).toArray
    chunks.length shouldBe 3

    val sparkSession: SparkSession = spark // alias stable : `spark` est un `var`, inutilisable dans un import
    implicit val sqlCtx: SQLContext = sparkSession.sqlContext
    import sparkSession.implicits._
    val source = MemoryStream[Array[Double]]
    val df     = source.toDF().withColumnRenamed("value", "features")

    val models = scala.collection.mutable.ArrayBuffer.empty[KMeansModel]
    val query  = StreamKMPlusPlus.run(df, StreamKMParams(k = k, m = m, seed = 73L), models += _)

    try {
      chunks.foreach { chunk =>
        source.addData(chunk.map(_.coords))
        query.processAllAvailable()
      }
    } finally {
      query.stop()
    }

    // Un micro-batch par appel à addData+processAllAvailable : exactement 3 modèles.
    // Si un batch avait été sauté (perte de masse) ou rejoué (double-comptage), ce
    // nombre resterait 3 mais la qualité ci-dessous décrocherait fortement.
    models should have length 3

    val streamCost = KMeans.cost(mix.points, models.last.centers)
    TestData.maxCenterDeviation(mix.trueCenters, models.last.centers) should be < 2.0

    // Référence : les mêmes points, en un seul bloc (pas de multi-batch), même graine
    // de driver que le test de masse ci-dessus pour rester comparable.
    val refBm = new BucketManager(m, seed = 71L)
    val rdd   = spark.sparkContext.parallelize(mix.points, numSlices = 4)
    StreamKMPlusPlus.mergeCoresets(rdd, StreamKMParams(k = k, m = m, seed = 71L)).foreach(refBm.insert)
    val refModel = KMeans.fit(refBm.extractCoreset(), k, nRestarts = 5, seed = 73L)
    val refCost  = KMeans.cost(mix.points, refModel.centers)

    // Tolérance : réutilise le même modèle de composition que le test principal
    // (CoresetErrorModel), en reconnaissant explicitement qu'il est réemployé par
    // commodité plutôt que re-dérivé pour cette composition précise (batch ->
    // global.insert, différente du treeReduce interne à chaque batch) — voir
    // CLAUDE.md, décision 12.
    val tolerance = CoresetErrorModel.treeReduceTolerance(numPartitions = 4, depth = 2)
    withClue(s"streaming (3 micro-batches)=$streamCost, référence (1 bloc)=$refCost : ") {
      streamCost should be <= refCost * (1.0 + tolerance)
    }
  }
}
