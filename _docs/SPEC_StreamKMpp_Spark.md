# StreamKM++ sur Spark — Spécification commune

> Document à valider par tout le groupe avant d'écrire une ligne de code.
> Référence : Bitsakis (2018), *Clustering Big Data Streams in Apache Flink* — chapitres 2 et 4.
> Papier d'origine : Ackermann et al. (2012), *StreamKM++: A clustering algorithm for data streams*, JEA 17.

---

## 0. Contraintes de l'énoncé (à ne pas perdre de vue)

| Exigence | Points | Conséquence sur l'archi |
|---|---|---|
| Description de la solution | 3 | Chapitre "algorithme + mapping Flink→Spark" |
| Algorithmes + description globale | 3 | Pseudo-code repris de la thèse, annoté |
| Commentaires sur les fragments de code principaux | 3 | Scaladoc systématique sur les fonctions publiques |
| **Analyse expérimentale, en particulier la scalabilité** | 3 | **Il faut faire varier le parallélisme Spark** |
| Points forts / points faibles | 3 | Section critique honnête (goulot driver, micro-batch…) |
| Annexe : tout le code + **notebook exécutable sur petites données** | 4 | Prévoir un notebook Almond/Databricks dès le début |

**La thèse est en Flink, le rendu doit être en Spark Streaming.** Tout le travail intéressant du rapport est dans ce portage : ce qui se traduit directement, ce qui ne se traduit pas, et pourquoi.

---

## 1. Architecture en 3 couches

```
┌─────────────────────────────────────────────────────────┐
│  couche 3 : streamkm.spark                              │
│  Structured Streaming, foreachBatch, treeAggregate      │
│  → aucune logique algorithmique ici, que de l'orchestration│
├─────────────────────────────────────────────────────────┤
│  couche 2 : streamkm.coreset                            │
│  CoresetTree (algo 2.4) + BucketManager (algo 2.5)      │
│  → pur Scala, sérialisable, testable sans Spark         │
├─────────────────────────────────────────────────────────┤
│  couche 1 : streamkm.core                               │
│  Point, distances, k-means++ (algo 2.2), Lloyd (algo 2.1)│
│  → pur Scala, zéro dépendance                           │
└─────────────────────────────────────────────────────────┘
```

**Règle d'or : les couches 1 et 2 ne connaissent pas Spark.** C'est ce qui permet
(a) de les tester en local avec ScalaTest, (b) de les micro-benchmarker avec JMH,
(c) de les appeler indifféremment depuis `mapPartitions`, un test unitaire ou le notebook.

---

## 2. Couche 1 — `streamkm.core`

```scala
package streamkm.core

/**
 * Point pondéré en dimension d.
 * `coords` n'est jamais copié défensivement : ne JAMAIS le muter après construction.
 * weight = 1.0 pour un point du flux ; > 1.0 pour un point de coreset.
 */
final class Point(val coords: Array[Double], val weight: Double) extends Serializable {
  def dim: Int = coords.length
}

object Distance {
  /** Distance euclidienne AU CARRÉ. Pas de sqrt : inutile et coûteux dans les boucles chaudes. */
  def sqdist(a: Array[Double], b: Array[Double]): Double

  /** Renvoie (indice du centre le plus proche, distance au carré). Un seul parcours. */
  def nearest(p: Array[Double], centers: Array[Array[Double]]): (Int, Double)
}

final case class KMeansModel(
  centers:    Array[Array[Double]],
  cost:       Double,   // SSE pondérée
  iterations: Int
)

object KMeans {
  /** Seeding D² — algo 2.2 lignes 1-3. Complexité O(n·k·d). */
  def seedPlusPlus(points: Array[Point], k: Int, rng: scala.util.Random): Array[Array[Double]]

  /** Lloyd pondéré — algo 2.1. Arrêt si |Δcost| < tol ou maxIter atteint. */
  def lloyd(points: Array[Point],
            init: Array[Array[Double]],
            maxIter: Int = 100,
            tol: Double = 1e-6): KMeansModel

  /** k-means++ complet : nRestarts × (seeding + Lloyd), on garde le coût minimal.
    * nRestarts = 5 dans la thèse (§4.1) — c'est un paramètre d'expérience, pas une constante. */
  def fit(points: Array[Point], k: Int,
          nRestarts: Int = 5, maxIter: Int = 100, seed: Long = 42L): KMeansModel

  /** SSE pondérée : Σ_p w_p · min_c ‖p − c‖². Sert à mesurer la qualité (§4.2 de la thèse). */
  def cost(points: Array[Point], centers: Array[Array[Double]]): Double
}
```

### Tests de validation obligatoires (couche 1)
1. `cost` sur 3 points colinéaires avec k=1 → variance analytique connue.
2. Lloyd sur deux gaussiennes bien séparées (σ=0.1, centres à distance 10) → retrouve les vrais centres à 1e-2.
3. Le coût est **monotone décroissant** à chaque itération de Lloyd (assertion dans la boucle en mode debug).
4. `fit(points, k)` avec poids tous à 1 doit donner le même résultat que MLlib `KMeans` à ±2 % de coût.

---

## 3. Couche 2 — `streamkm.coreset`

### 3.1 Coreset Tree (algo 2.4)

```scala
package streamkm.coreset

private[coreset] final class Node(
  var points: Array[Point],    // non-null ⟺ feuille
  var repr:   Array[Double],   // point représentatif
  var cost:   Double,          // Σ_{p ∈ points} w_p · ‖p − repr‖²  (feuille) ; somme des fils (interne)
  var weight: Double,          // Σ_{p ∈ points} w_p                (masse totale du sous-arbre)
  var left:   Node,
  var right:  Node
) { def isLeaf: Boolean = left == null }

object CoresetTree {
  /** Algo 2.4. Renvoie exactement min(m, |points|) points pondérés.
    * Le poids d'un point de sortie = somme des poids des points de sa feuille. */
  def build(points: Array[Point], m: Int, rng: scala.util.Random): Array[Point]

  /** Descente depuis la racine, choix d'un fils proportionnellement à son COÛT. */
  private def chooseLeaf(root: Node, rng: scala.util.Random): Node

  /** Échantillonnage D² dans la feuille, relativement à leaf.repr. */
  private def sampleD2(leaf: Node, rng: scala.util.Random): Array[Double]

  /** Scission d'une feuille en deux, puis remontée des attributs jusqu'à la racine. */
  private def split(leaf: Node, newRepr: Array[Double]): Unit
  private def propagateUp(n: Node): Unit
}
```

> ⚠️ **Ambiguïté à trancher collectivement (et à mentionner dans le rapport).**
> La thèse §2.3.3 dit « probabilité proportionnelle au **coût** », mais l'algo 2.4 ligne 5
> dit « selon leurs **poids** ». Le papier d'origine utilise le coût — c'est ce qui a du sens
> (une feuille bien clusterisée ne doit plus être scindée). **On implémente le coût**, et on
> signale l'incohérence dans le rapport. C'est exactement le genre de « difficulté rencontrée »
> qui rapporte des points en section 4.

### 3.2 Merge-and-Reduce (algo 2.5)

```scala
/**
 * Invariants (thèse §2.3.4) :
 *  - bucket(0) : buffer d'entrée, contient entre 0 et m points de poids 1
 *  - bucket(i>0) : soit VIDE, soit EXACTEMENT m points, et représente alors
 *    2^(i-1) · m points du flux
 *  - nombre de buckets : O(log(n/m)) — croissance à la demande, pas besoin de n a priori
 */
final class BucketManager(val m: Int, val seed: Long) extends Serializable {
  def insert(p: Point): Unit
  def insertAll(it: Iterator[Point]): this.type
  def numPointsSeen: Long

  /** Union de tous les buckets non vides, puis CoresetTree.build(_, m).
    * NE VIDE PAS l'état : on peut interroger le clustering en cours de flux. */
  def extractCoreset(): Array[Point]

  /** Fusion de deux structures indépendantes (observation 1 de §2.3.4).
    * Utilisée par treeAggregate. Ne modifie ni this ni other. */
  def merge(other: BucketManager): BucketManager
}
```

### Tests de validation obligatoires (couche 2)
1. Après `insertAll` de n points, `numPointsSeen == n` et les invariants de taille tiennent (assertion explicite).
2. `extractCoreset()` renvoie ≤ m points et **la somme des poids == n**. ← test le plus important, il attrape 90 % des bugs.
3. Sur un mélange de 5 gaussiennes, `KMeans.fit(coreset, 5)` a un coût ≤ 1.1 × celui de `KMeans.fit(tous_les_points, 5)`.
4. `a.merge(b).extractCoreset()` a une somme de poids == `a.numPointsSeen + b.numPointsSeen`.

---

## 4. Couche 3 — `streamkm.spark`

```scala
package streamkm.spark

final case class StreamKMParams(
  k:         Int,
  m:         Int,          // taille de coreset ; la thèse recommande m = 200·k (§2.3.4)
  nRestarts: Int = 5,
  seed:      Long = 42L
)

object StreamKMPlusPlus {
  /** Un micro-batch → un coreset partiel par partition (opérateur FlatMapToPartialCoresets). */
  def partialCoresets(rdd: RDD[Point], p: StreamKMParams): RDD[Array[Point]]

  /** Fusion arborescente des coresets partiels — cf. le cours sur treeAggregate.
    * Remplace l'opérateur non-parallèle de la thèse (§4.1) : c'est NOTRE apport vs Flink. */
  def mergeCoresets(rdd: RDD[Point], p: StreamKMParams, depth: Int = 2): Array[Point]

  /** Boucle streaming complète, état global maintenu entre micro-batches. */
  def run(source: DataFrame, p: StreamKMParams, onModel: KMeansModel => Unit): StreamingQuery
}
```

### Variante retenue : Structured Streaming + `foreachBatch` + `treeAggregate`

```scala
val global = new BucketManager(p.m, p.seed)   // état driver, taille O(m·log(n/m)) → quelques Mo

source.writeStream.foreachBatch { (batch: DataFrame, batchId: Long) =>
  // 1) réduction locale, en parallèle, une passe par partition
  val partial: Array[Point] = batch.as[Point].rdd
    .treeAggregate(new BucketManager(p.m, p.seed + batchId))(
      seqOp   = (bm, pt)   => { bm.insert(pt); bm },
      combOp  = (bm1, bm2) => bm1.merge(bm2),
      depth   = 2
    ).extractCoreset()

  // 2) intégration dans l'état global (merge-and-reduce au niveau driver)
  partial.foreach(global.insert)

  // 3) clustering à la demande
  onModel(KMeans.fit(global.extractCoreset(), p.k, p.nRestarts))
}.start()
```

**Pourquoi cette variante :** elle mappe 1-pour-1 sur le dataflow Flink de la thèse (§4.1,
figures 4.1-4.2), elle réutilise `treeAggregate` vu en cours, et elle est débuggable.

**Variantes à mentionner dans le rapport (essayées ou au moins discutées) :**
- `flatMapGroupsWithState` : le vrai équivalent du *keyed state* Flink, mais impose des
  `Encoder` sur `BucketManager` → sérialisation pénible. À citer comme piste écartée.
- DStreams + `mapWithState` : littéralement « Spark Streaming » historique, API en voie
  d'abandon. Bon paragraphe de comparaison.
- Queryable State (§4.4 de la thèse) : **n'a pas d'équivalent Spark**. C'est un point
  fort de Flink à assumer dans la section « points faibles ». Contournement : sink mémoire
  + requête SQL sur la table des centres.

---

## 5. Plan d'expériences (la partie qui rapporte le plus de points)

| Exp. | Ce qu'on fait varier | Ce qu'on mesure | Figure attendue |
|---|---|---|---|
| **E1 — Correction** | k ∈ {5,10,20} sur données synthétiques à vérité terrain | coût vs MLlib KMeans | table de coûts, écart < 5 % |
| **E2 — Trade-off m** | m ∈ {20k, 50k, 100k, 200k} | temps + coût | courbe coût/temps (= fig. 5.1-5.2 thèse) |
| **E3 — Scalabilité forte** | `local[N]`, N ∈ {1,2,4,8} à données fixes | temps, speedup, efficacité | speedup vs idéal (= fig. 5.8) |
| **E4 — Scalabilité faible** | N et taille données × N | temps ≈ constant ? | courbe plate ou non |
| **E5 — Débit** | taille du flux × {1,2,4,8} | points/s | fig. 5.10-5.11 thèse |
| **E6 — Baseline** | vs `StreamingKMeans` de MLlib | coût + temps | table comparative |

**E6 est un cadeau** : Spark MLlib contient déjà `StreamingKMeans` (mini-batch avec facteur
d'oubli). Le comparer à StreamKM++ donne gratuitement une section « points forts/faibles »
solide : StreamKM++ garantit un ε-coreset et ne dérive pas, `StreamingKMeans` est plus rapide
mais sans garantie et sensible à l'initialisation.

**Jeux de données.** HIGGS (11 M × 28, 7,76 Go) est hors de portée d'un portable. Prendre :
- un générateur synthétique de mélange gaussien (vérité terrain, taille pilotable) → E2-E5 ;
- **Covertype** (581 k × 54) ou **KDD Cup 99**, tous deux sur UCI, tailles raisonnables → E1, E6 ;
- garder un extrait de 10 k lignes pour le notebook de l'annexe.

**Points forts / faibles à préparer d'avance (section 4 du barème) :**
- ➕ un seul passage sur les données, mémoire O(m·log n), qualité proche du k-means++ batch
- ➕ `treeAggregate` évite le goulot du réducteur non-parallèle de la thèse
- ➖ le `KMeans.fit` final tourne sur le driver → temps constant qui plafonne le speedup
  (exactement ce qu'observe la thèse au-delà de 8 : §5.3.1)
- ➖ le modèle micro-batch de Spark ajoute de la latence vs le vrai streaming de Flink
- ➖ pas de gestion du *concept drift* (les vieux buckets ne sont jamais oubliés)
- ➖ non-déterminisme : le résultat dépend du partitionnement → fixer les seeds par partition

---

## 6. Répartition et calendrier (deadline : 31 août)

| Jour | Qui | Quoi | Livrable |
|---|---|---|---|
| J1 | tous | valider CE document | spec figée, repo GitHub créé |
| J1 | 1 pers. | **envoyer le mail de composition du groupe** | — |
| J2-J4 | 2 pers. | couche 1 + tests | `core` vert, poussé sur GitHub |
| J3-J6 | 2 pers. | couche 2 + tests (dépend de la couche 1) | `coreset` vert |
| J5-J8 | 2 pers. | couche 3 Spark + notebook squelette | job qui tourne en `local[*]` |
| J7-J10 | 1-2 pers. | optimisations SoA/primitifs + JMH sur `CoresetTree.build` | table avant/après |
| J9-J12 | 2 pers. | expériences E1-E6, collecte des mesures | CSV + graphes |
| J2-J14 | tous | rapport LaTeX en parallèle | PDF |
| J13-J14 | tous | notebook final petites données, relecture | rendu |

**Note sur les optimisations perf.** Elles ont toute leur place, mais sur la **couche 2**
(le kernel `CoresetTree.build` exécuté dans chaque executor), pas sur du code Spark : JMH
mesure une JVM isolée, il ne peut rien dire d'un job distribué. Le récit du rapport devient
alors cohérent : « on a optimisé le kernel local (JMH, ×N), puis mesuré la scalabilité
end-to-end du job Spark (E3-E5) ». Les deux mesures répondent à deux questions différentes,
et le dire explicitement est un bon point.

---

## 7. Conventions de repo

```
streamkm-spark/
├── build.sbt                      # Scala 2.12.18, Spark 3.5.x (2.12 obligatoire pour Spark)
├── src/main/scala/streamkm/
│   ├── core/     Point.scala  Distance.scala  KMeans.scala
│   ├── coreset/  CoresetTree.scala  BucketManager.scala
│   └── spark/    StreamKMPlusPlus.scala  Main.scala
├── src/test/scala/streamkm/       # ScalaTest, un fichier par module
├── bench/                         # sbt-jmh, isolé du build principal
├── notebooks/demo.ipynb           # Almond ou Databricks Community
├── data/small/                    # extrait 10k lignes pour le notebook
└── report/                        # LaTeX
```

Branches : `main` protégée, une branche par couche, PR obligatoire avec relecture croisée
(ça sert aussi à répartir la connaissance avant la soutenance/le rapport).
