# StreamKM++ sur Spark — projet M2 Dauphine (déploiement ML à grande échelle)

Portage en **Apache Spark** de StreamKM++, dont la référence d'implémentation
(Bitsakis, 2018, *Clustering Big Data Streams in Apache Flink*) est en **Apache
Flink**. Papier d'origine : Ackermann et al. (2012), *StreamKM++: A clustering
algorithm for data streams*, JEA 17.

Deadline : 31 août 2026. Barème sur 19 points (voir `_docs/indicationsSITNapp2026.key.pdf`).

## Documents qui font autorité

Ne pas s'en écarter sans le signaler explicitement dans une réponse.

- `_docs/indicationsSITNapp2026.key.pdf` — énoncé et barème.
- `_docs/METHODO_StreamKMpp_Spark.md` — méthodologie (étapes E0→E9) et table de
  correspondance opérateur par opérateur Flink → Spark.
- `_docs/SPEC_StreamKMpp_Spark.md` — spécification validée par le groupe
  (signatures, invariants, tests obligatoires).
- `_docs/Bitsakis_Theodoros_Dip_2018 copy.pdf` — thèse de référence. §2.3.3
  (coreset tree), §2.3.4 (merge-and-reduce) et §4.1 (dataflow Flink) ont été lus
  intégralement ; le reste est résumé dans le METHODO.

## Architecture en 3 couches

```
streamkm.core     Point, Distance, KMeans (seedPlusPlus, lloyd, fit, cost)   — E1
streamkm.coreset  CoresetTree (algo 2.4), BucketManager (algo 2.5)          — E2
streamkm.spark    StreamKMParams, StreamKMPlusPlus (partialCoresets,
                  mergeCoresets, run) ; CostEvaluator (cost)                — E3/E4/E5
```

**Règle d'or (SPEC §1) : les couches `core` et `coreset` ne connaissent pas
Spark.** Pur Scala, sérialisable, testable sans JVM Spark, benchmarkable au JMH.
`streamkm.demo.Main` (E1/E2 seulement, pas de Spark) sert de démonstrateur en
attendant la couche `spark`.

## Les décisions de conception (tranchées, ne pas « simplifier »)

### E1/E2 — vérifiées contre la thèse (§2.3.3, §2.3.4, algo 2.4) le 2026-08-18

1. **`cost` et `mass` sont deux champs distincts dans `CoresetTree.Node`.**
   La thèse appelle « weight » les deux : `weight(v)` défini juste avant
   l'algo 2.4 = `cost(P_v, q_v)` (une somme de distances au carré, sert à la
   descente) ; §2.3.4 réutilise ensuite « weight » pour désigner la masse
   (nombre de points) du coreset de sortie. Deux quantités incompatibles, un
   seul mot dans la thèse. Les confondre donne un algorithme qui tourne sans
   planter et produit des coresets faux.
2. **La descente dans l'arbre (`CoresetTree.descend`) est proportionnelle au
   COÛT, pas à la masse.** §2.3.3 étape 1 le dit en prose ; l'algo 2.4 ligne 5
   dit « according to their weights », mais au sens de la définition de
   `weight(v)` donnée juste au-dessus (= cost). Confirmé par le papier
   original d'Ackermann et al.
3. **Le premier centre de `KMeans.seedPlusPlus` et le premier représentant de
   `CoresetTree.build` sont tirés proportionnellement au poids**, alors que la
   thèse écrit « uniformly at random » (algo 2.2 ligne 1, algo 2.4 ligne 1).
   Correct sur des points bruts (poids uniforme ⇒ les deux tirages
   coïncident) ; biaisé sur un coreset (poids très hétérogènes) — et dans le
   pipeline complet de StreamKM++, k-means++ et le coreset tree ne s'appliquent
   jamais qu'à des ensembles déjà pondérés.
4. **`BucketManager.merge` renvoie un manager « collapsé »** dont toute
   insertion ultérieure lève `IllegalStateException`. Voulu et sans risque
   pour un `combOp` de `treeAggregate` (Spark ne lui passe jamais que des
   résultats de `seqOp`/`combOp`, jamais de points bruts).

### E3/E4 — couche `streamkm.spark`, décidées le 2026-08-22

5. **`BucketManager.merge` existait déjà et a été réutilisée telle quelle,
   sans aucune modification.** Avant d'écrire la couche Spark, vérification
   explicite (demandée en amont) : la thèse a deux moments de fusion de
   coresets — interne au merge-and-reduce (algo 2.5, deux buckets pleins de
   même niveau) et au niveau du pipeline distribué (`FlatMapToFinalCoreset`,
   §4.1, fusion des coresets partiels de chaque subtask). Ce sont
   algorithmiquement la même opération (union pondérée + réduction via coreset
   tree), et `BucketManager.merge(other)` la couvrait déjà entièrement :
   signature `(BucketManager, BucketManager) => BucketManager`, sans état
   partagé, exactement le contrat d'un `combOp`. Aucune extension nécessaire,
   donc aucune modification de `streamkm.coreset` : `partialCoresets` et
   `mergeCoresets` (dans `streamkm.spark.StreamKMPlusPlus`) l'appellent
   directement.
6. **`mergeCoresets` n'utilise pas `RDD.treeAggregate` malgré l'exemple de
   pipeline de la SPEC (§4), mais `mapPartitionsWithIndex` + `RDD.treeReduce`.**
   `treeAggregate(zeroValue)(seqOp, combOp, depth)` clone un `zeroValue`
   UNIQUE (même graine) pour chaque partition. Or METHODO §4.0 exige des
   graines distinctes par partition, condition nécessaire pour que
   l'expérience E3bis (faire varier `numPartitions` à données fixes) mesure
   bien l'effet du partitionnement et non un simple changement de suite
   aléatoire. `mapPartitionsWithIndex` (graine = `p.seed + idx`) suivi de
   `treeReduce((a, b) => a.merge(b), depth)` obtient la même opération
   (accumulateur par partition, fusion en arbre) sans ce biais. Écart
   documenté par rapport à l'exemple de code de la SPEC, pas par rapport à ses
   signatures (`partialCoresets`, `mergeCoresets` sont conformes au caractère
   près) ni à ses invariants (masse totale conservée — testé).
7. **Contrat d'entrée de `run` : une colonne `featuresCol: array<double>`,
   poids implicite 1.0.** Non spécifié dans la SPEC (qui laisse `run` avec une
   `DataFrame` générique). Choix le plus simple cohérent avec `Point.scala`
   (le poids n'existe que dans les coresets produits en interne, jamais en
   entrée du flux) ; conversion `Row => Point` faite en RDD brut plutôt que via
   un `Encoder[Point]` Spark, pour éviter d'ajouter un Encoder à une classe qui
   n'est pas un case class — cohérent avec la « règle d'or » (pas de logique
   Spark qui fuite dans `core`).
8. **`sbt test` sous Java 17 exige les `--add-opens` du JDK** (ajoutés à
   `Test / javaOptions` dans `build.sbt`) : Spark/Hadoop font de la réflexion
   sur des modules internes du JDK (java.lang, java.nio, sun.nio.ch…) que le
   module system de Java 17 bloque par défaut. Sans ce flag,
   `SparkSession.builder().getOrCreate()` lève une
   `InaccessibleObjectException` dès l'initialisation. Spark ajoute les mêmes
   flags dans ses propres scripts de lancement depuis la 3.3 ; on les
   reproduit ici pour que `sbt test` reste utilisable en local sans passer par
   `spark-submit`.
9. **Instrumentation minimale plutôt qu'un framework de métriques.** `run` logue
   une ligne par micro-batch (partitions, m, k, masse vue, temps de
   construction du coreset, coût) via `println`, sans changer la signature de
   `run` prévue par la SPEC. `numPartitions` (via `repartition(N)` en amont) et
   `m` (`StreamKMParams.m`) sont déjà des paramètres explicites côté appelant :
   rien à ajouter au code pour les faire varier dans les expériences du ch. 5 ;
   il suffira d'un script qui appelle `mergeCoresets`/`run` en boucle sur une
   grille de `(numPartitions, m)` et parse ces lignes de log (ou capture le
   `KMeansModel` via `onModel`).

### Audit ciblé du 2026-08-22 (post-implémentation E4) — 3 points vérifiés à la demande

10. **Bug réel trouvé et corrigé : `BucketManager.merge` construisait le coreset
    fusionné avec la mauvaise graine.** La ligne `CoresetTree.build(all, m, rng)`
    utilisait `rng` non qualifié, c'est-à-dire `this.rng` — le générateur de
    l'opérande GAUCHE de l'appel (`a` dans `a.merge(b)`) — et non `out.rng`, le
    générateur fraîchement construit avec la graine combinée
    `seed ^ other.seed` (ligne juste au-dessus, calculée mais jamais utilisée).
    Conséquence concrète : dans un `treeReduce` à plusieurs niveaux, seul le
    tout premier niveau (partition → coreset partiel, dans
    `StreamKMPlusPlus.partialCoresets`/`mergeCoresets`) avait un aléa réellement
    indépendant par construction (`new BucketManager(m, p.seed + idx)`) ; à
    chaque niveau de fusion SUIVANT, l'aléa ne dépendait plus que d'un seul des
    deux opérandes fusionnés, en ignorant totalement la graine de l'autre — pas
    un aléa « fixe », mais un aléa hérité et donc moins indépendant que voulu à
    partir du deuxième niveau. **Correction : `CoresetTree.build(all, m,
    out.rng)`** (un seul mot changé). Aucun test existant n'était sensible à ce
    bug (aucun ne comparait le comportement à plusieurs niveaux de fusion à une
    référence sans biais de graine) — voir décision 12 qui comble ce trou.
11. **La tolérance de 25 % du test « 8 partitions vs séquentiel » était une
    valeur choisie à vue, pas dérivée — remplacée par un calcul explicite**
    (objet `CoresetErrorModel` dans `StreamKMSpec.scala`). Dérivation :
    - La thèse ne donne AUCUNE formule fermée pour l'ε d'un coreset produit par
      `CoresetTree` (§2.3.4, point 3 : « the authors do not prove that the
      coreset tree structure is indeed a (k,ε)-coreset »). Le seul ε chiffré
      (théorème 2.1) concerne `AdaptiveCoreset`, avec une constante `O(d)` non
      précisée — inutilisable numériquement. Une « tolérance théorique
      exacte » au sens strict n'existe donc pas dans la thèse ; ce qui EST
      dérivable rigoureusement, c'est la LOI de composition (observation 2,
      §2.3.4 : `(1+ε)(1+δ)-1`, multiplicative, pas additive) et le nombre de
      niveaux de composition qu'introduit `treeReduce`.
    - Nombre de niveaux : Spark regroupe les résultats par paquets de
      `scale = max(2, ⌈numPartitions^(1/depth)⌉)` à chaque palier (au plus
      `depth` paliers), chaque paquet étant replié par une chaîne de
      `scale - 1` fusions séquentielles. Pire cas :
      `levels = depth · (scale - 1)`. Pour `numPartitions=8, depth=2` (valeurs
      du test) : `scale = ⌈√8⌉ = 3`, `levels = 2·(3-1) = 4`.
    - Magnitude par niveau : on réutilise l'ε empirique déjà validé par
      `CoresetSpec` ("le coreset préserve la qualité du clustering", **un
      seul** niveau de réduction) = 10 %, faute d'une valeur théorique
      exploitable.
    - Résultat : `(1.10)^4 - 1 ≈ 46,4 %`, contre 25 % codé en dur auparavant.
      **La valeur théorique est donc plus LARGE que l'ancienne valeur
      empirique**, pas plus stricte : le test passait déjà largement en
      dessous des deux bornes. On utilise directement la valeur calculée
      (46,4 %) plutôt que de la tronquer arbitrairement — un bug de régression
      réel (par ex. l'oubli de pondération dans Lloyd) produirait un écart de
      qualité bien supérieur à cette borne, qui reste donc discriminante.
    - Limite assumée : `depth · (scale - 1)` est une estimation dérivée de la
      stratégie de regroupement interne de `treeReduce`/`treeAggregate`
      (`RDD.scala`), pas une garantie publiée par Spark. À documenter comme tel
      dans le rapport (section « points faibles » : notre borne d'erreur est
      elle-même une extrapolation, pas un résultat prouvé de bout en bout).
12. **Trou de test comblé : aucun test ne comparait un traitement en plusieurs
    micro-batches successifs (état maintenu) à un traitement en un seul bloc.**
    Deux tests ajoutés à `StreamKMSpec` :
    - un test **précis** qui reproduit la boucle interne de `run` à la main
      (accès direct à `global.numPointsSeen`, vérifié après CHAQUE micro-batch,
      pas seulement à la fin — une perte de masse sur un batch compensée par un
      double-comptage sur un autre resterait invisible sinon) ;
    - un test **d'intégration** qui appelle réellement `StreamKMPlusPlus.run`
      via un `MemoryStream` Spark piloté par `addData`/`processAllAvailable`
      (c'était le seul chemin de code de toute la couche Spark qu'aucun test
      n'exerçait jusqu'ici), comparant la qualité obtenue après 3 micro-batches
      à un traitement en un seul bloc, avec la même tolérance dérivée qu'en
      décision 11 (réemployée par commodité pour `numPartitions=4`, pas
      re-dérivée indépendamment pour cette composition légèrement différente
      `batch → global.insert` — limite signalée explicitement dans le
      commentaire du test).
13. **Le test d'intégration `run()` (décision 12, second test) est marqué
    `ignore` sur cette machine Windows** — pas une régression algorithmique,
    mais une limitation d'environnement : Spark Structured Streaming écrit des
    métadonnées de checkpoint sur le système de fichiers local via
    `RawLocalFileSystem.setPermission`, qui exige `winutils.exe`/`HADOOP_HOME`
    sous Windows (absent ici). Confirmé par la pile d'erreur exacte
    (`Shell.checkHadoopHomeInner : HADOOP_HOME and hadoop.home.dir are
    unset`), reproductible et sans rapport avec `StreamKMPlusPlus`. Décision :
    ne pas installer `winutils.exe` (binaire tiers précompilé, non publié par
    Apache, qui serait ensuite EXÉCUTÉ par Hadoop) pour corriger un problème
    d'environnement local ; le test reste dans le code, prêt à être réactivé
    (`test` au lieu d'`ignore`) sans aucune modification dès qu'il tourne sous
    Linux/macOS ou dans un conteneur Docker — pertinent de toute façon pour les
    futures expériences de scalabilité (E7) sur une base reproductible.

### E5 — instrument de mesure du coût (2026-08-22)

14. **`CostEvaluator.cost` délègue entièrement à `KMeans.cost` (couche 1),
    par partition, sommé par `treeReduce`.** Pas de nouvelle formule : chaque
    partition appelle `KMeans.cost(it.toArray, centers)` (déjà testé par
    `KMeansSpec`) avec les centres reçus par `broadcast`, puis les résultats
    partiels sont sommés. Différence de nature avec `mergeCoresets` : une somme
    est associative et exacte (à l'ordre de sommation flottant près) — aucune
    composition d'erreur multiplicative ici, donc les tests de `CostEvaluator`
    utilisent une égalité stricte (tolérance ~1e-9) avec la référence
    séquentielle, pas une tolérance dérivée façon décision 11. Un écart au-delà
    de cette précision signalerait un vrai bug de comptage (points dupliqués ou
    perdus par le partitionnement), pas une variance d'échantillonnage.

### E7 — métriques de qualité sur Covertype (2026-08-25), `streamkm.experiments`

Choix du groupe : Covertype (581 012 × 54, UCI) plutôt que les gaussiennes
synthétiques pour les expériences de qualité — c'est en haute dimension que
le coreset est vraiment mis à l'épreuve, et la thèse rapporte des résultats
dessus (comparaison possible). Fichier brut NON versionné (~72 Mo) :
`data/covertype/download.sh` le régénère ; `data/small/covtype_10k.data`
(10 000 lignes, tirage aléatoire seed=42, versionné) sert aux tests et aux
runs rapides.

15. **`CovertypeLoader.parseLine` rejette la 55ᵉ colonne (`Cover_Type`).**
    54 colonnes de features numériques (10 mesures continues + 4+40 colonnes
    one-hot), aucune standardisation appliquée — features brutes, comme le
    papier d'origine. À documenter comme limite dans le rapport : les colonnes
    one-hot (échelle 0/1) et les mesures brutes (échelle jusqu'à ~7000) pèsent
    très différemment dans une distance euclidienne au carré.
16. **`E2TradeoffM` (trade-off m, fig. 5.1-5.2 thèse) tourne avec une grille
    réduite par défaut** (3 runs au lieu de 10) : la grille complète de la
    SPEC (m×k×10 runs = 210 combinaisons) devient coûteuse en `local[*]` dès
    que m et k sont grands (Lloyd sur un coreset de 20 000 points en
    dimension 54, 5 redémarrages, jusqu'à 100 itérations). Grille et nombre de
    runs réglables en CLI (`--m`, `--k`, `--runs`, `--partitions`) — la grille
    complète est à lancer séparément pour les chiffres définitifs du rapport,
    sur une machine plus patiente ou un cluster.
17. **`E6Baseline` évalue StreamKM++, `spark.ml.KMeans` et
    `mllib.StreamingKMeans` avec LA MÊME fonction de coût** (`CostEvaluator`,
    jamais le coût interne de chaque bibliothèque, qui peut être défini
    différemment). `ml.KMeans` reçoit `maxIter=100` pour un effort comparable
    à `core.KMeans.lloyd`. `StreamingKMeans` (API DStream, dépréciée depuis
    Spark 3.4 mais sans équivalent Structured Streaming) est nourri via un
    `StreamingContext.queueStream` qui rejoue le RDD statique par tranches,
    faute d'un vrai flux.
    **Résultat observé** (smoke test 10k points, m=1000, k=5) :
    `StreamingKMeans` a un coût ~4 ordres de grandeur pire que StreamKM++ et
    `ml.KMeans` (~1e14 contre ~1e10). Cause probable : `setRandomCenters` tire
    les centres initiaux suivant une loi normale centrée en 0, sans rapport
    avec l'échelle réelle de Covertype (non standardisée), et le petit nombre
    de micro-batches ne laisse pas le temps de corriger un mauvais tirage.
    **Décision : ne pas corriger** (par exemple en standardisant les features)
    — ce serait avantager `StreamingKMeans` seul et fausser la comparaison à
    trois. Ce résultat illustre concrètement ce que la SPEC §5 anticipait déjà
    pour la section « points forts/faibles » : StreamKM++ garantit une
    propriété de coreset, `StreamingKMeans` non, et y est sensible à
    l'initialisation. À confirmer sur la grille complète (581 k points) avant
    de l'écrire dans le rapport — un seul smoke test n'est pas une preuve.

## État d'avancement

| Étape | Contenu | État |
|---|---|---|
| E0 | Appropriation théorique | fait (groupe) |
| E1 | `streamkm.core` | **fait, compile, 7 tests verts** |
| E2 | `streamkm.coreset` | **fait, compile, 14 tests verts** |
| E3 | Rédaction ch. 3 (rapport) | à faire (le code de la couche Spark existe, pas encore sa description en prose) |
| E4 | `streamkm.spark`, job borné §4.1 | **fait : `partialCoresets`, `mergeCoresets`, `run` (Structured Streaming), 5 tests dont le test de non-régression distribué/séquentiel** |
| E5 | Job de mesure du coût (§4.2) | **fait : `CostEvaluator.cost`, 4 tests** — instrument de mesure prêt pour E1/E2/E6 |
| E6 | Optimisation kernel local (JMH) | **fait sur la branche `feat/benchmark` (non fusionnée)** : `bench/`, résultats dans `results/jmh_results.csv` |
| E7 | Expériences ch. 5 | **qualité en cours** : `streamkm.experiments` (`CovertypeLoader`, `E2TradeoffM`, `E6Baseline`) sur Covertype, vérifié par smoke test sur l'échantillon 10k — grille complète (581k, 210 combinaisons E2) pas encore lancée. Scalabilité (E3, E3bis, E4, E5) pas commencée. |
| E8 | Variante à requêtes (§4.3) | à faire, ou décrite si le temps manque |
| E9 | Notebook + rapport | à faire |

`sbt test` est passé au vert le 2026-08-18 avec une seule correction : une
erreur d'inférence de type dans `BucketManager.insert` (`toArray` sans
contexte de type explicite), aucune correction algorithmique nécessaire.

`sbt test` re-vérifié le 2026-08-22 après l'ajout de `streamkm.spark` (26
tests au total : 21 E1/E2 + 5 E4, tous verts — voir la sortie exacte reportée
dans la réponse qui accompagne ce commit). Aucun fichier de `streamkm.core` ou
`streamkm.coreset` n'a été modifié pour E4 : `BucketManager`/`CoresetTree`/
`KMeans` sont utilisés tels quels.

## Commandes

```
sbt test    # compile + fait tourner les tests (core + coreset + spark + experiments)
sbt run     # streamkm.demo.Main — démonstration E1/E2 sur un mélange gaussien

# E7 (qualité, sur Covertype) : dépendances Provided invisibles pour `sbt run`
# (cf. note ci-dessous) → passer par Test/runMain.
data/covertype/download.sh   # une fois, télécharge covtype.data (~72 Mo, non versionné)

sbt "Test/runMain streamkm.experiments.E2TradeoffM --data data/covertype/covtype.data --out results/e2_tradeoff_m.csv"
sbt "Test/runMain streamkm.experiments.E6Baseline  --data data/covertype/covtype.data --out results/e6_baseline.csv --k 25,50,75"
# --data data/small/covtype_10k.data pour un smoke test rapide avant de lancer sur les 581k.
```

Scala 2.12.18 (imposé par Spark 3.5, cf. `build.sbt`). Dépendances : `scalatest`
(Test), `spark-core`/`spark-sql` 3.5.3 (Provided — sur le classpath de compile
et de test, absentes du jar assemblé). Les assertions d'invariants
(`BucketManager.checkInvariants`, monotonie du coût de Lloyd) ne sont actives
qu'avec `-ea`, déjà configuré pour `Test` dans `build.sbt`, avec les
`--add-opens` nécessaires à Spark sous Java 17 (décision 8 ci-dessus).

**Note pour la suite (`sbt run` sur un futur `Main` Spark, si besoin) :** les
dépendances `Provided` sont visibles par `sbt test` mais PAS par `sbt run` par
défaut (limitation connue de sbt : `Compile/run` n'inclut pas les
dépendances `Provided` sur son classpath runtime). Si un point d'entrée Spark
exécutable en local est ajouté plus tard, il faudra soit lancer via
`sbt "Test/runMain ..."`, soit ajouter la configuration
`Compile / run := Defaults.runTask(Compile / fullClasspath, ...).evaluated`
dans `build.sbt`. Non nécessaire pour l'instant : E4 n'expose que des
fonctions testées via ScalaTest, pas de `Main`.
