package streamkm.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

import streamkm.core.{KMeans, KMeansModel, Point}
import streamkm.coreset.BucketManager

/**
 * Paramètres de StreamKM++ (SPEC §4).
 *
 * `m` est la taille de coreset — la thèse recommande m = 200·k (§2.3.4,
 * `BucketManager.recommendedM`). `nRestarts` est le nombre d'applications de
 * k-means++ sur le coreset final (thèse §2.3.4 étape 2 : 5 par défaut ; le
 * §5.3.1 montre qu'une seule suffit en pratique — paramètre d'expérience E3,
 * pas une constante algorithmique).
 */
final case class StreamKMParams(
  k:         Int,
  m:         Int,
  nRestarts: Int = 5,
  seed:      Long = 42L
)

/**
 * Couche 3 (SPEC §4) : orchestration Spark de StreamKM++, portage du dataflow
 * Flink §4.1 de la thèse.
 *
 * RÈGLE D'OR (SPEC §1, rappelée ici) : ce fichier ne contient AUCUNE logique
 * algorithmique. Tout le calcul vient de `streamkm.core` (KMeans, déjà testé
 * E1) et `streamkm.coreset` (BucketManager/CoresetTree, déjà testé E2). Cette
 * couche se contente de distribuer des appels à ces fonctions sur des
 * partitions RDD et d'orchestrer le micro-batch Structured Streaming. Aucune
 * modification n'a été nécessaire dans `core`/`coreset` pour ça — voir
 * CLAUDE.md, décision « E3-1 ».
 */
object StreamKMPlusPlus {

  /**
   * Un coreset partiel par PARTITION — portage direct de l'opérateur
   * `FlatMapToPartialCoresets` de la thèse (§4.1) : « each one of the parallel
   * subtasks... consumes the input points according to algorithm 2.5 ». Une
   * partition Spark joue exactement le rôle d'un subtask Flink.
   *
   * Graine dérivée de l'indice de partition (`p.seed + idx`) plutôt que
   * partagée : nécessaire pour que les expériences de scalabilité (METHODO
   * §4.0, E3bis) restent reproductibles indépendamment du nombre de
   * partitions choisi. Avec une graine unique partagée entre partitions,
   * changer `numPartitions` changerait aussi la suite aléatoire consommée par
   * chaque partition, et confondrait l'effet mesuré (le parallélisme) avec un
   * simple changement de graine — biais qu'on évite ici.
   */
  def partialCoresets(rdd: RDD[Point], p: StreamKMParams): RDD[Array[Point]] =
    rdd.mapPartitionsWithIndex { (idx, it) =>
      val bm = new BucketManager(p.m, p.seed + idx)
      bm.insertAll(it)
      Iterator.single(bm.extractCoreset())
    }

  /**
   * Coreset final unique pour tout le RDD — remplace l'opérateur
   * `FlatMapToFinalCoreset` de la thèse (§4.1), qui y est **non-parallèle**
   * (fusion séquentielle des coresets partiels). C'est l'apport principal
   * identifié par METHODO §3 : la thèse elle-même repère cet opérateur comme
   * le goulot qui plafonne le speedup au-delà de 8 cœurs (§5.3.1). Ici, la
   * fusion se fait en arbre via `RDD.treeReduce`.
   *
   * Implémentation : un `BucketManager` par partition (équivalent d'un
   * `seqOp` qui insère tous les points de la partition, déjà couvert par les
   * tests E2 de `BucketManager.insert`/`insertAll`), fusionnés deux à deux par
   * `BucketManager.merge` (le `combOp`, réutilisé tel quel — voir CLAUDE.md
   * décision « E3-1 ») via `treeReduce(f, depth)`.
   *
   * Pourquoi pas `RDD.treeAggregate` directement, alors que la SPEC (§4,
   * exemple de pipeline) l'utilise avec un `zeroValue` unique partagé ? Parce
   * que ce `zeroValue` unique porte UNE SEULE graine pour toutes les
   * partitions (Spark clone l'objet par tâche, mais avec le même état RNG
   * initial), ce qui casse la recommandation ci-dessus de graines
   * indépendantes par partition. `mapPartitionsWithIndex` + `treeReduce`
   * produit exactement la même opération (un accumulateur par partition,
   * fusionnés en arbre de profondeur `depth`) tout en gardant des graines
   * distinctes. Écart documenté par rapport à l'exemple de code de la SPEC,
   * mais pas à ses signatures ni à ses invariants.
   */
  def mergeCoresets(rdd: RDD[Point], p: StreamKMParams, depth: Int = 2): Array[Point] = {
    val perPartition: RDD[BucketManager] = rdd.mapPartitionsWithIndex { (idx, it) =>
      val bm = new BucketManager(p.m, p.seed + idx)
      bm.insertAll(it)
      Iterator.single(bm)
    }
    perPartition.treeReduce((a, b) => a.merge(b), depth).extractCoreset()
  }

  /**
   * Boucle Structured Streaming complète — portage du dataflow entier §4.1 de
   * la thèse (HdfsSource → FlatMapToPoint → FlatMapToPartialCoresets →
   * FlatMapToFinalCoreset → FlatMapToKmeansPP → HdfsSink).
   *
   * Contrat d'entrée : `source` doit exposer une colonne `featuresCol` de type
   * `array<double>` (un point brut du flux, poids implicite 1.0 — le poids
   * n'apparaît jamais dans les données d'entrée, seulement dans les coresets
   * produits en interne, cf. `Point.scala`). C'est l'équivalent du
   * `FlatMapToPoint` de la thèse, qui parse une ligne en `Point` ; ici la
   * conversion est un simple `Row => Point`, volontairement sans passer par un
   * `Encoder[Point]` Spark (Point n'est pas un case class, lui en donner un
   * ajouterait de la complexité sans bénéfice : on repasse en RDD dès la
   * lecture de la colonne, comme le fait `mergeCoresets`).
   *
   * État global : un unique `BucketManager` maintenu côté driver entre les
   * micro-batches (mémoire bornée par construction, O(m·log(n/m)), thèse
   * §2.3.4). Chaque micro-batch produit son propre coreset distribué via
   * `mergeCoresets`, qui est ensuite intégré (`insert`) dans cet état global —
   * exactement la même opération de fusion que `FlatMapToFinalCoreset`,
   * appliquée cette fois entre micro-batches plutôt qu'entre partitions.
   *
   * Instrumentation : une ligne de log par micro-batch (partitions, m, k,
   * masse vue, temps de construction du coreset, coût du modèle) — le
   * minimum nécessaire pour l'analyse de scalabilité du ch. 5 du rapport,
   * sans dépendance à un framework de métriques. `numPartitions` et `m` sont
   * déjà des leviers explicites (le premier via `source.repartition(N)` avant
   * l'appel, le second via `StreamKMParams.m`) : rien à changer ici pour les
   * faire varier dans les expériences.
   */
  def run(
    source:      DataFrame,
    p:           StreamKMParams,
    onModel:     KMeansModel => Unit,
    featuresCol: String = "features"
  ): StreamingQuery = {

    val global = new BucketManager(p.m, p.seed)

    source.writeStream.foreachBatch { (batch: DataFrame, batchId: Long) =>
      val t0 = System.nanoTime()

      val points: RDD[Point] = batch.rdd.map { row =>
        val coords = row.getAs[Seq[Double]](featuresCol).toArray
        Point(coords)
      }
      val numPartitions = points.getNumPartitions

      val partial = mergeCoresets(points, p.copy(seed = p.seed + batchId))
      partial.foreach(global.insert)

      val model     = KMeans.fit(global.extractCoreset(), p.k, p.nRestarts, seed = p.seed)
      val elapsedMs = (System.nanoTime() - t0) / 1e6

      // scalastyle:off println
      println(
        s"[StreamKM++] batch=$batchId partitions=$numPartitions m=${p.m} k=${p.k} " +
        f"pointsSeen=${global.numPointsSeen}%.0f buildTimeMs=$elapsedMs%.1f cost=${model.cost}%.4g"
      )
      // scalastyle:on println

      onModel(model)
    }.start()
  }
}
