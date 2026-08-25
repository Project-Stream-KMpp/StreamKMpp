package streamkm.experiments

import java.io.{File, PrintWriter}

import org.apache.spark.sql.SparkSession

import streamkm.core.KMeans
import streamkm.spark.{CostEvaluator, StreamKMParams, StreamKMPlusPlus}

/**
 * E2 — Trade-off taille de coreset (METHODO §4.2, reproduit fig. 5.1-5.2 de la
 * thèse). Sur Covertype (581 k × 54, cf. `CovertypeLoader`), fait varier `m`
 * à `k` fixé et mesure coût (qualité) et temps (coût de construction du
 * coreset + clustering final). Le coût est calculé sur les données COMPLÈTES
 * via `CostEvaluator` (E5) — jamais sur le coreset lui-même, qui n'est pas
 * comparable (cf. `demo.Main`).
 *
 * Sortie : un CSV brut (une ligne par run), conforme au critère de sortie de
 * E7 dans le METHODO (« un CSV brut par expérience »). Les figures sont
 * produites séparément (notebook / rapport), pas ici.
 *
 * Grille et nombre de runs par défaut RÉDUITS par rapport à la table de la
 * SPEC (m ∈ {200..20000} × k ∈ {25,50,75} × 10 runs = 210 combinaisons) : sur
 * un poste de développement en `local[*]`, k jusqu'à 75 avec m jusqu'à 20000
 * fait tourner Lloyd sur un coreset de 20000 points en dimension 54, ce qui
 * devient coûteux à répéter 10 fois. La grille complète reste accessible via
 * les arguments CLI — à lancer sur une machine plus patiente (ou un cluster)
 * pour produire les chiffres définitifs du rapport.
 */
object E2TradeoffM {

  final case class Row(m: Int, k: Int, run: Int, coresetSize: Int, buildTimeMs: Double, cost: Double)

  private def parseInts(s: String): Seq[Int] = s.split(',').map(_.trim.toInt).toSeq

  def main(args: Array[String]): Unit = {
    val argMap = args.grouped(2).collect { case Array(k, v) => k -> v }.toMap

    val dataPath = argMap.getOrElse("--data", "data/small/covtype_10k.data")
    val outPath  = argMap.getOrElse("--out", "results/e2_tradeoff_m.csv")
    val mValues  = parseInts(argMap.getOrElse("--m", "200,1000,2000,4000,6000,10000,20000"))
    val kValues  = parseInts(argMap.getOrElse("--k", "25,50,75"))
    val numRuns  = argMap.getOrElse("--runs", "3").toInt
    val numParts = argMap.getOrElse("--partitions", "4").toInt
    val baseSeed = argMap.getOrElse("--seed", "42").toLong

    val spark = SparkSession.builder().appName("StreamKM++ E2 trade-off m").master("local[*]").getOrCreate()
    val sc    = spark.sparkContext

    try {
      val points = CovertypeLoader.load(sc, dataPath).repartition(numParts).cache()
      val n       = points.count()
      println(s"[E2] $n points chargés depuis $dataPath, $numParts partitions")

      val rows = for {
        k   <- kValues
        m   <- mValues
        run <- 0 until numRuns
      } yield {
        val seed   = baseSeed + run
        val params = StreamKMParams(k, m, nRestarts = 5, seed = seed)

        val t0      = System.nanoTime()
        val coreset = StreamKMPlusPlus.mergeCoresets(points, params)
        val model   = KMeans.fit(coreset, k, params.nRestarts, seed = seed)
        val buildMs = (System.nanoTime() - t0) / 1e6

        val cost = CostEvaluator.cost(points, model.centers)
        println(f"[E2] k=$k m=$m run=$run coreset=${coreset.length} temps=${buildMs}%.1fms coût=$cost%.4g")
        Row(m, k, run, coreset.length, buildMs, cost)
      }

      writeCsv(outPath, rows)
      println(s"[E2] ${rows.length} lignes écrites dans $outPath")
    } finally {
      spark.stop()
    }
  }

  private def writeCsv(path: String, rows: Seq[Row]): Unit = {
    val file = new File(path)
    Option(file.getParentFile).foreach(_.mkdirs())
    val pw = new PrintWriter(file)
    try {
      pw.println("m,k,run,coreset_size,build_time_ms,cost")
      rows.foreach(r => pw.println(s"${r.m},${r.k},${r.run},${r.coresetSize},${r.buildTimeMs},${r.cost}"))
    } finally pw.close()
  }
}
