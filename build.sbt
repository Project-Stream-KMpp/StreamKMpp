// Scala 2.12 est imposé par Spark 3.5 (E4). On s'y tient dès maintenant pour éviter
// une migration au moment d'ajouter la couche Spark.
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "streamkm"
ThisBuild / version      := "0.1.0-SNAPSHOT"

lazy val root = (project in file("."))
  .settings(
    name := "streamkm-spark",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.18" % Test
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
    Test / fork        := true,
    Test / javaOptions ++= Seq("-Xmx2g", "-ea")
  )

// E4 ajoutera :
// libraryDependencies ++= Seq(
//   "org.apache.spark" %% "spark-core"  % "3.5.1" % Provided,
//   "org.apache.spark" %% "spark-sql"   % "3.5.1" % Provided,
//   "org.apache.spark" %% "spark-mllib" % "3.5.1" % Provided
// )
