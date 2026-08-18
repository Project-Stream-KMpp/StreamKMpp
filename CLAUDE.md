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
streamkm.spark     (pas encore implémenté — E4)
```

**Règle d'or (SPEC §1) : les couches `core` et `coreset` ne connaissent pas
Spark.** Pur Scala, sérialisable, testable sans JVM Spark, benchmarkable au JMH.
`streamkm.demo.Main` (E1/E2 seulement, pas de Spark) sert de démonstrateur en
attendant la couche `spark`.

## Les 4 décisions de conception (tranchées, ne pas « simplifier »)

Vérifiées contre la thèse (§2.3.3, §2.3.4, algo 2.4) le 2026-08-18.

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

## État d'avancement

| Étape | Contenu | État |
|---|---|---|
| E0 | Appropriation théorique | fait (groupe) |
| E1 | `streamkm.core` | **fait, compile, 7 tests verts** |
| E2 | `streamkm.coreset` | **fait, compile, 14 tests verts** |
| E3 | Rédaction ch. 3 (rapport) | à faire |
| E4 | `streamkm.spark`, job borné §4.1 | **non commencé — volontairement hors scope de cette session** |
| E5 | Job de mesure du coût (§4.2) | à faire |
| E6 | Optimisation kernel local (JMH) | à faire |
| E7 | Expériences ch. 5 | à faire |
| E8 | Variante à requêtes (§4.3) | à faire, ou décrite si le temps manque |
| E9 | Notebook + rapport | à faire |

`sbt test` est passé au vert le 2026-08-18 avec une seule correction : une
erreur d'inférence de type dans `BucketManager.insert` (`toArray` sans
contexte de type explicite), aucune correction algorithmique nécessaire.

## Commandes

```
sbt test    # compile + fait tourner les 21 tests (core + coreset)
sbt run     # streamkm.demo.Main — démonstration E1/E2 sur un mélange gaussien
```

Scala 2.12.18 (imposé par Spark 3.5, cf. `build.sbt`), aucune dépendance hors
`scalatest`. Les assertions d'invariants (`BucketManager.checkInvariants`,
monotonie du coût de Lloyd) ne sont actives qu'avec `-ea`, déjà configuré pour
`Test` dans `build.sbt`.
