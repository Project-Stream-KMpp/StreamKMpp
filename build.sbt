// Scala 2.12 est imposé par Spark 3.5 (E4). On s'y tient dès maintenant pour éviter
// une migration au moment d'ajouter la couche Spark.
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "streamkm"
ThisBuild / version      := "0.1.0-SNAPSHOT"

// Spark 3.5.x : dernière branche 3.5 supportant nativement Java 17 (E4).
// Scope "provided" : disponible en compile/test, absent du jar assemblé — c'est
// spark-submit qui fournit ces classes au runtime en production.
val sparkVersion = "3.5.3"

lazy val root = (project in file("."))
  .settings(
    name := "streamkm-spark",
    libraryDependencies ++= Seq(
      "org.scalatest"    %% "scalatest"   % "3.2.18"     % Test,
      "org.apache.spark" %% "spark-core"  % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql"   % sparkVersion % Provided,
      // Ajouté pour streamkm.experiments.QualityMetrics : sert de référence externe
      // (org.apache.spark.ml.clustering.KMeans, entraîné sur le dataset complet, pas
      // sur un coreset) dans l'expérience de comparaison à MLlib.
      "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided
    ),
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xlint"
    ),
    // -ea active les `assert` d'invariants du BucketManager pendant les tests.
    // En production (E4) on laisse -ea désactivé : les assertions coûtent cher
    // dans les boucles chaudes.
    //
    // Les --add-opens sont indispensables pour faire tourner Spark sous Java 17 :
    // Spark/Hadoop utilisent de la réflexion sur des modules internes du JDK
    // (java.lang, java.nio, sun.nio.ch...) que le module system de Java 17 bloque
    // par défaut (InaccessibleObjectException sans ce flag). Ce n'est PAS une
    // fantaisie de config : sans ça, `sbt test` plante dès la création du
    // SparkContext. Spark lui-même ajoute ces mêmes flags dans ses scripts de
    // lancement (spark-submit) à partir de la 3.3.
    Test / fork        := true,
    Test / javaOptions ++= Seq(
      "-Xmx2g",
      "-ea",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    ),

    // `run`/`runMain` : mêmes flags JVM que Test (Spark sous Java 17), avec plus de
    // mémoire (le harnais d'expériences streamkm.experiments charge Covertype en
    // entier, ~580k points, en mémoire locale pour la référence séquentielle).
    run / fork        := true,
    run / javaOptions := (Test / javaOptions).value.map {
      case "-Xmx2g" => "-Xmx4g"
      case other    => other
    },

    // Limitation sbt connue (cf. CLAUDE.md, note sous "Commandes") : par défaut,
    // `Compile / run` ET `Compile / runMain` n'exposent PAS les dépendances
    // "Provided" (Spark) sur leur classpath runtime — seul `Test / *` les voit.
    // `run` et `runMain` sont deux tâches sbt DISTINCTES (découvert le
    // 2026-08-22 : corriger seulement `run` ne suffit pas pour
    // `sbt runMain ...`) ; les deux sont donc réalignées sur
    // `Compile / fullClasspath` (qui inclut Provided), pour que `sbt run` ET
    // `sbt runMain streamkm.experiments.QualityMetrics` fonctionnent directement
    // sans devoir passer par `Test/runMain`.
    Compile / run := Defaults.runTask(
      Compile / fullClasspath,
      Compile / run / mainClass,
      Compile / run / runner
    ).evaluated,
    Compile / runMain := Defaults.runMainTask(
      Compile / fullClasspath,
      Compile / run / runner
    ).evaluated
  )
