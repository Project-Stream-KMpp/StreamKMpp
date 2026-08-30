package streamkm.demo

import scala.util.Random

import streamkm.core.{Distance, KMeans, Point}
import streamkm.coreset.{BucketManager, CoresetTree}
import streamkm.experiments.QualityMetrics

object ValidationMain {

  private def approx(a: Double, b: Double, eps: Double = 1e-8): Boolean =
    math.abs(a - b) <= eps

  def main(args: Array[String]): Unit = {

    println("========================================")
    println("TEST 1 - Distance")
    println("========================================")

    val a = Array(0.0, 0.0)
    val b = Array(3.0, 4.0)

    val d2 = Distance.sqdist(a, b)

    println(s"Distance² obtenue : $d2")
    println("Distance² attendue : 25.0")

    assert(approx(d2, 25.0))

    println("OK\n")


    println("========================================")
    println("TEST 2 - KMeans déterministe")
    println("========================================")

    val points = Array(
      Point.of(0.0, 0.0),
      Point.of(0.0, 2.0),
      Point.of(10.0, 10.0),
      Point.of(10.0, 12.0)
    )

    // On impose volontairement les centres initiaux
    // pour tester Lloyd indépendamment du hasard de k-means++.
    val initialCenters = Array(
      Array(0.0, 0.0),
      Array(10.0, 10.0)
    )

    val model = KMeans.lloyd(points, initialCenters)

    println(s"Coût obtenu  : ${model.cost}")
    println("Coût attendu : 4.0")

    println("Centres obtenus :")
    model.centers.foreach { c =>
      println(c.mkString("  (", ", ", ")"))
    }

    assert(approx(model.cost, 4.0))

    val sorted = model.centers.sortBy(_(0))

    assert(approx(sorted(0)(0), 0.0))
    assert(approx(sorted(0)(1), 1.0))

    assert(approx(sorted(1)(0), 10.0))
    assert(approx(sorted(1)(1), 11.0))

    println("OK\n")


    println("========================================")
    println("TEST 3 - KMeans pondéré")
    println("========================================")

    val weighted = Array(
      Point(Array(0.0, 0.0), 3.0),
      Point(Array(10.0, 0.0), 1.0)
    )

    val weightedModel =
      KMeans.lloyd(
        weighted,
        Array(Array(0.0, 0.0))
      )

    println(
      s"Centre obtenu : (${weightedModel.centers(0)(0)}, ${weightedModel.centers(0)(1)})"
    )
    println("Centre attendu : (2.5, 0.0)")

    println(s"Coût obtenu  : ${weightedModel.cost}")
    println("Coût attendu : 75.0")

    assert(approx(weightedModel.centers(0)(0), 2.5))
    assert(approx(weightedModel.centers(0)(1), 0.0))
    assert(approx(weightedModel.cost, 75.0))

    println("OK\n")


    println("========================================")
    println("TEST 4 - Conservation de masse Coreset")
    println("========================================")

    val raw = Array.tabulate(100) { i =>
      Point.of(i.toDouble, 0.0)
    }

    val coreset =
      CoresetTree.build(
        raw,
        m = 10,
        new Random(42L)
      )

    val inputMass  = raw.map(_.weight).sum
    val outputMass = coreset.map(_.weight).sum

    println(s"Nombre de points entrée : ${raw.length}")
    println(s"Taille coreset          : ${coreset.length}")
    println(s"Masse entrée            : $inputMass")
    println(s"Masse coreset           : $outputMass")

    assert(coreset.length <= 10)
    assert(approx(inputMass, outputMass))

    println("OK\n")


    println("========================================")
    println("TEST 5 - BucketManager")
    println("========================================")

    val manager = BucketManager(m = 10, seed = 42L)

    manager.insertAll(raw)

    val finalCoreset = manager.extractCoreset()

    println(s"Points vus       : ${manager.numPointsSeen}")
    println(s"Taille coreset   : ${finalCoreset.length}")
    println(s"Masse du coreset : ${finalCoreset.map(_.weight).sum}")

    assert(approx(manager.numPointsSeen, 100.0))
    assert(finalCoreset.length <= 10)
    assert(approx(finalCoreset.map(_.weight).sum, 100.0))

    println("OK\n")


    if (args.nonEmpty) {

      println("========================================")
      println("TEST 6 - Lecture Covertype")
      println("========================================")

      val path = args(0)

      val covertype =
        QualityMetrics.loadCovertype(
          path,
          dim = 54,
          maxPoints = Some(10)
        )

      println(s"Points chargés : ${covertype.length}")

      assert(covertype.length == 10)

      covertype.zipWithIndex.foreach { case (p, i) =>
        println(
          s"Point $i : dim=${p.dim}, premières valeurs = " +
          p.coords.take(5).mkString("[", ", ", "]")
        )

        assert(p.dim == 54)
        assert(p.weight == 1.0)
      }

      println("OK\n")
    }

    println("========================================")
    println("TOUS LES TESTS SONT PASSÉS")
    println("========================================")
  }
}