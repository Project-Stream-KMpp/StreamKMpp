# Méthodologie d'implémentation — StreamKM++ sur Spark

> Document compagnon de `SPEC_StreamKMpp_Spark.md`.
> La spec dit **quoi** coder (structures, signatures) ; ce document dit **dans quel ordre**
> et **pourquoi**, en calquant la démarche de Bitsakis (2018).

---

## 0. Pourquoi reprendre le plan de la thèse

Le §1.1 de la thèse (*Thesis Outline*) n'est pas un simple sommaire : c'est une **méthode
de travail**, qui va du plus stable au plus incertain :

1. **Théorie d'abord** (ch. 2) — on ne code rien tant que l'algorithme n'est pas compris,
   parce que StreamKM++ est un empilement de trois briques (Lloyd → k-means++ → coreset tree
   → merge-and-reduce) où chacune est le *seeding* de la suivante. Coder la brique 3 sans
   maîtriser la 2, c'est déboguer à l'aveugle.
2. **Framework ensuite** (ch. 3) — la thèse décrit Flink *avant* l'implémentation, parce que
   le dataflow qu'elle propose au ch. 4 est directement contraint par ce que Flink sait faire
   (état par subtask, watermarks, broadcast). Même logique pour nous avec Spark.
3. **Implémentation opérateur par opérateur** (ch. 4), avec une version simple d'abord
   (flux borné, §4.1), puis les raffinements (évaluation §4.2, requêtes périodiques §4.3,
   requêtes temps réel §4.3.1).
4. **Validation avant performance** (ch. 5) — la thèse mesure d'abord la **qualité**
   en non-parallèle (§5.2 : « our implementation produces accurate clusterings »), et
   **seulement ensuite** la scalabilité (§5.3). L'ordre est crucial : mesurer le speedup
   d'un algorithme faux ne veut rien dire.

**On reprend cette structure telle quelle.** Elle donne à la fois notre plan de travail
et le plan du rapport, et elle couvre exactement le barème de l'énoncé.

---

## 1. Correspondance thèse → notre projet

| Thèse | Contenu | Chez nous | Barème visé |
|---|---|---|---|
| Ch. 2 | k-means, k-means++, coreset tree, merge-and-reduce | Ch. 2 identique (+ nos notations) | Algorithmes (3 pts) |
| Ch. 3 | Apache Flink : dataflow, runtime, DataStream API | Ch. 3 : **Spark** — RDD/DataFrame, Structured Streaming, micro-batch, `broadcast`, `treeAggregate` | Description solution (3 pts) |
| §4.1 | Dataflow distribué, flux borné | §4.1 : job Spark équivalent, opérateur par opérateur | Solution + commentaires code (6 pts) |
| §4.2 | Méthodologie d'évaluation (coût parallèle) | §4.2 : `broadcast` + `treeAggregate` | Analyse expé (3 pts) |
| §4.3 | Ré-évaluation périodique par requêtes | §4.3 : trigger Structured Streaming, k multiples | Solution (3 pts) |
| §4.3.1 | Queryable State (requêtes temps réel) | §4.3.1 : **pas d'équivalent Spark** → sink mémoire + SQL, discuté honnêtement | Points forts/faibles (3 pts) |
| §5.2 | Qualité : trade-off m, comparaison à l'original | §5.2 : E1, E2, E6 | Analyse expé (3 pts) |
| §5.3 | Scalabilité : runtime, coût, débit | §5.3 : E3, E4, E5 | **Scalabilité (3 pts)** |
| Ch. 6 | Conclusions, travaux futurs | Ch. 6 + annexe code & notebook | Points forts/faibles + annexe (7 pts) |

---

## 2. Les étapes, dans l'ordre

Chaque étape a un **critère de sortie** vérifiable. On ne passe pas à la suivante sans lui.

### E0 — Appropriation théorique (tout le groupe, 1 réunion)
- Lire ch. 2 de la thèse. Chacun explique une brique aux autres à l'oral :
  Lloyd (algo 2.1), seeding D² (algo 2.2), coreset tree (algos 2.3-2.4), merge-and-reduce (algo 2.5).
- Trancher l'ambiguïté « coût vs poids » du §2.3.3 étape 1 (cf. spec).
- **Sortie :** figure 2.2/2.3 de la thèse redessinée à la main par le groupe sur un exemple
  à 10 points. Si on sait la refaire, on a compris. Cette figure ira dans le rapport.

### E1 — Couche noyau (`streamkm.core`)
- k-means++ (seeding + Lloyd) en Scala, **pondéré dès le départ** — c'est le piège classique :
  le coreset produit des points de poids ≠ 1, un Lloyd non pondéré donnera des centres faux
  sans planter.
- **Sortie :** tests 1-4 de la spec au vert. Le plus rapide pousse sur GitHub, les autres relisent.

### E2 — Couche coreset (`streamkm.coreset`)
- `CoresetTree.build` (algo 2.4), puis `BucketManager` (algo 2.5).
- Assertions d'invariants activées en mode debug : tailles de buckets, **somme des poids == n**.
- **Sortie :** tests 1-4 de la spec au vert + un test de non-régression comparant
  `KMeans.fit(coreset, k)` à `KMeans.fit(données_complètes, k)` sur 5 gaussiennes.

### E3 — Description du framework (rédaction, en parallèle de E1-E2)
- Écrire le ch. 3 du rapport : modèle Spark, RDD vs DataFrame, micro-batch vs vrai streaming,
  `broadcast`, `treeAggregate`, partitions vs cœurs.
- **Sortie :** ch. 3 rédigé. Il sert de référence pour justifier les choix du ch. 4.

### E4 — Job Spark, version bornée (§4.1)
Traduction opérateur par opérateur (voir §3 ci-dessous). Version « tout le fichier, puis
clustering ». C'est le cœur du rendu.
- **Sortie :** le job tourne en `local[*]` sur Covertype et produit des centres dont le coût
  est à moins de 5 % de `KMeans.fit` batch sur les mêmes données.

### E5 — Méthodologie d'évaluation (§4.2)
Job séparé qui recalcule le coût sur le fichier complet, en parallèle, à partir d'un fichier
de centres. C'est l'instrument de mesure de tout le ch. 5 — il doit exister **avant** les
expériences.
- **Sortie :** le coût calculé par ce job coïncide (à 1e-9 relatif) avec celui calculé
  en mémoire par `KMeans.cost` sur un petit jeu.

### E6 — Optimisation du kernel local (JMH, en parallèle de E5)
Structure of Arrays, tableaux primitifs, suppression du boxing, moins de branches dans
`sqdist` et `chooseLeaf`. **Uniquement sur les couches 1-2**, mesuré au JMH.
- **Sortie :** table avant/après (ns/op) + preuve que les tests de E1/E2 passent toujours.

### E7 — Expériences (ch. 5)
Protocole détaillé en §4 ci-dessous. Ordre imposé : **qualité (§5.2) avant scalabilité (§5.3)**.
- **Sortie :** un CSV brut par expérience + les figures générées.

### E8 — Variante avec requêtes (§4.3) — *si le temps le permet*
Ré-évaluation périodique + plusieurs valeurs de k simultanément.
- **Sortie :** captures montrant l'évolution des centres au fil des micro-batches.
- **Si le temps manque : on ne l'implémente pas, mais on la décrit** dans le rapport comme
  extension, avec le dataflow Spark correspondant. La thèse elle-même ne l'évalue pas
  expérimentalement (cf. son ch. 6, « in future work it is important to evaluate… »).

### E9 — Notebook et rapport
Notebook sur `data/small/` (10 k lignes), commenté entrée/sortie comme l'exige l'énoncé.
Rapport LaTeX suivant le plan du §1 ci-dessus.

---

## 3. Traduction du dataflow, opérateur par opérateur

C'est **le contenu technique central du rapport** : c'est là qu'on montre qu'on a compris
la thèse *et* Spark. Chaque ligne est un paragraphe du ch. 4.

### §4.1 — StreamKM++ distribué

| Opérateur Flink (thèse) | Rôle | Équivalent Spark | Commentaire pour le rapport |
|---|---|---|---|
| `Custom File Source` (par. 1) | surveillance du répertoire | `readStream.format("csv").option("maxFilesPerTrigger", 1)` | Spark gère nativement, pas d'opérateur dédié |
| `HdfsSource` (par. 32) | lecture parallèle des lignes | source fichier, partitionnée par splits | idem |
| `HandleWatermark` | détecter la fin de fichier, émettre un point marqué « EOF » | **aucun** | Simplification nette : `foreachBatch` est appelé une fois par micro-batch, la frontière est explicite. Tout le bricolage watermark/EOF de la thèse disparaît. **À souligner.** |
| `FlatMapToPoint` | parsing `String` → `Point`, rejet des lignes invalides | `mapPartitions` + `filter` | traduction directe |
| `FlatMapToPartialCoresets` (par. 32) | merge-and-reduce par subtask, état en mémoire | `treeAggregate(seqOp = insert)` ou `mapPartitions` + `BucketManager` | l'état par subtask Flink devient l'accumulateur par partition |
| `FlatMapToFinalCoreset` (**par. 1**) | fusion **séquentielle** des coresets partiels | `treeAggregate(combOp = merge, depth = 2)` — **arborescente** | ⚠️ **Notre apport principal.** La thèse identifie elle-même cet opérateur non-parallèle comme le goulot qui plafonne le speedup au-delà de 8 (§5.3.1). On le parallélise. |
| `FlatMapToKmeansPP` (par. 1) | 5× k-means++ sur le coreset final | `KMeans.fit` sur le driver | reste non-parallèle : c'est le résidu d'Amdahl, à mesurer |
| `HdfsSink` | écriture des centres | `write.csv` dans `foreachBatch` | traduction directe |

### §4.2 — Méthodologie d'évaluation

| Opérateur Flink | Rôle | Équivalent Spark |
|---|---|---|
| `FlatMapToCentroid` + `.broadcast()` | diffuser les centres à tous les subtasks | `sc.broadcast(centers)` |
| `CoFlatMapToPartialCost` (connected stream + buffering) | sommer les coûts en parallèle | `treeAggregate` avec `seqOp = (acc, p) => acc + w*d²` |
| `CoFlatMapToFinalCost` (par. 1) | somme des coûts partiels | `combOp = _ + _` |

Ce qui demande à Flink un *connected stream*, un buffer d'attente et trois opérateurs tient
en Spark en trois lignes de `broadcast` + `treeAggregate`. **C'est le meilleur exemple de
« point fort de Spark » du rapport**, et il mobilise directement le cours sur broadcast et
treeAggregate.

### §4.3 — Variante avec requêtes

| Opérateur Flink | Rôle | Équivalent Spark |
|---|---|---|
| `SourceRequests_R` + broadcast | générer des requêtes périodiques | **le trigger est la requête** : `Trigger.ProcessingTime("30 seconds")` |
| `CoFlatMapToPartialCoresets_R` | état + requêtes dans le même opérateur | état global + `foreachBatch` |
| `FlatMapToFinalCoreset_R` | dupliquer le coreset pour plusieurs k | `Seq(25, 50, 75).map(k => KMeans.fit(coreset, k))` |
| `KeyedProcessFuncToKmeansPP_R` + `ValueState` | stocker le meilleur clustering par k | `Map[Int, KMeansModel]` ou table Delta |

### §4.3.1 — Requêtes temps réel

| Flink | Spark |
|---|---|
| `QueryableStateClient` / `Proxy` / `Server` | **pas d'équivalent.** Contournement : sink `memory` + `spark.sql("SELECT * FROM centers")`, ou écriture des centres dans un store externe |

À assumer franchement : c'est un avantage architectural réel de Flink (état keyed adressable
de l'extérieur) que Spark n'a pas, parce que Spark n'expose pas d'état inter-micro-batch
interrogeable. Section « points faibles ».

---

## 4. Protocole expérimental (ch. 5)

### 4.0 Un point de méthode que la thèse ne pose pas

Dans Flink, « parallelism » est **un seul nombre** : le nombre de task slots, qui fixe à la
fois le nombre de subtasks et le nombre de listes de buckets. Dans Spark, ces deux choses
sont **découplées** :

- le **nombre de partitions** détermine le nombre de `BucketManager` indépendants → influence
  la **qualité** (plus de listes = plus de buckets = coresets plus fins) ;
- le **nombre de cœurs** détermine le débit → influence le **temps**.

La thèse observe que le coût *diminue* quand le parallélisme augmente (§5.3.1, fig. 5.9) et
l'explique justement par la multiplication des listes de buckets. **On peut tester cette
explication directement**, ce que Flink ne permet pas : fixer les cœurs et faire varier les
partitions seules. Si le coût baisse quand on augmente les partitions à cœurs constants,
l'explication de la thèse est confirmée. C'est une contribution propre, peu coûteuse, et
c'est exactement le genre de chose qui fait la différence en section 4 du barème.

→ Fixer explicitement `spark.sql.shuffle.partitions` et utiliser `repartition(P)`, ne jamais
laisser Spark décider.

### 4.1 Setup (§5.1 de la thèse)
Documenter : machine(s), CPU, RAM, version Spark/Scala/JVM, mode de déploiement
(`local[N]` ou cluster), options mémoire. Sans ça, aucune mesure n'est interprétable.

### 4.2 Qualité, avant tout (§5.2)

**E1 — Correction.** Données synthétiques à vérité terrain (mélange de gaussiennes),
k ∈ {5, 10, 20}. Vérifier que les centres retrouvés correspondent.

**E2 — Trade-off taille de coreset (reproduit fig. 5.1-5.2).**
Datasets : Covertype (581 k × 54) et Tower (4,9 M × 3) — ceux de la thèse, tailles raisonnables.
m ∈ {200, 1 000, 2 000, 4 000, 6 000, 10 000, 20 000}, k ∈ {25, 50, 75}, 10 runs, moyenne.
Attendu : le coût plafonne au-delà de m ≈ 10 000, le temps croît linéairement en m
→ on doit retrouver que **m = 200k** est un bon compromis.

**E6 — Comparaison de référence.** À la place de la comparaison au code C original de la thèse
(§5.2.2, non disponible), comparer à `spark.ml.KMeans` (batch) et à `StreamingKMeans` (MLlib)
sur les mêmes données. Trois lignes dans une table : coût et temps.

### 4.3 Scalabilité, ensuite (§5.3)

**E3 — Temps & coût vs parallélisme (reproduit fig. 5.8-5.9).**
k ∈ {25, 50, 75, 100}, cœurs ∈ {1, 2, 4, 8}, 4 runs par point, moyenne + écart-type.
Reprendre les **deux réglages de la thèse** : 5 applications de k-means++ sur le coreset
final vs 1 seule (§5.3.1). La thèse conclut que 1 seule suffit (le coût ne perd que deux
ordres de grandeur en dessous du total) — c'est une conclusion facile à re-vérifier et
qui montre qu'on a lu le ch. 5.
Reporter **speedup et efficacité**, pas seulement le temps brut.

**E3bis — Partitions à cœurs constants.** Cf. §4.0. Cœurs fixés à 4, partitions ∈ {4, 8, 16, 32}.
Mesurer le coût. Confirme ou infirme l'explication de la thèse.

**E4 — Scalabilité faible.** Données × N et cœurs × N simultanément. Temps constant = bon.

**E5 — Débit (reproduit fig. 5.16-5.19).** Répliquer le dataset ×1, ×2, ×4, ×8 comme le fait
la thèse avec HIGGS, mesurer en points/s. Attendu : le débit **augmente** avec la taille,
parce que la partie non-parallèle (k-means++ final) est à temps constant et s'amortit.

**E7 — Fusion séquentielle vs arborescente.** Notre variante `treeAggregate` contre la
fusion séquentielle de la thèse (§4.1, `FlatMapToFinalCoreset` en parallélisme 1), à
parallélisme croissant. Doit montrer que le plafond observé par la thèse au-delà de 8
recule. C'est **la mesure qui justifie notre apport** ; si elle ne montre rien, on le dit,
c'est aussi un résultat.

### 4.4 Hygiène de mesure (à écrire dans le rapport)
- JVM : jeter le premier run (JIT), garder les 4 suivants.
- Seeds fixées et **reportées**, une par partition pour la reproductibilité.
- Moyenne **et** écart-type ; sans dispersion, une courbe de speedup ne prouve rien.
- Séparer temps de lecture I/O et temps de calcul (sinon on mesure le disque).

---

## 5. Plan du rapport (à ouvrir dans le dépôt LaTeX dès maintenant)

```
1. Introduction — problème du clustering en flux, pourquoi les coresets
2. Algorithmes — k-means, k-means++, coreset tree, merge-and-reduce   [3 pts]
3. Apache Spark — modèle, micro-batch vs vrai streaming, broadcast, treeAggregate
4. Implémentation
   4.1 StreamKM++ distribué : dataflow Spark, opérateur par opérateur  [3+3 pts]
   4.2 Méthodologie d'évaluation du coût
   4.3 Variante à ré-évaluation périodique
   4.4 Portage depuis Flink : ce qui se traduit, ce qui ne se traduit pas
5. Évaluation expérimentale
   5.1 Setup — 5.2 Qualité (E1, E2, E6) — 5.3 Scalabilité (E3, E3bis, E4, E5, E7)  [3 pts]
6. Discussion — points forts et faibles                                [3 pts]
7. Conclusion et perspectives
Annexe A — code complet    Annexe B — notebook commenté                [4 pts]
```

Le §4.4 n'existe pas dans la thèse : c'est **notre** chapitre, celui qui répond à la question
que pose réellement l'énoncé (« porter en Spark une solution Flink »). C'est là qu'on met
la disparition de `HandleWatermark`, le remplacement de la fusion séquentielle par
`treeAggregate`, l'absence de Queryable State, et le découplage partitions/cœurs.
