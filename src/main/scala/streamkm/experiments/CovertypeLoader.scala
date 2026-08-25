package streamkm.experiments

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import streamkm.core.Point

/**
 * Chargement du jeu de données Covertype (UCI ML Repository) pour les
 * expériences de qualité E2 et E6 (METHODO §4.2, choisi par le groupe pour sa
 * haute dimension : c'est là que le coreset est vraiment mis à l'épreuve, et
 * la thèse rapporte des résultats dessus, ce qui permet une comparaison).
 *
 * Format d'une ligne (`covtype.info`) : 55 colonnes entières séparées par des
 * virgules — 54 attributs numériques (10 mesures continues + 4 colonnes
 * one-hot "Wilderness_Area" + 40 colonnes one-hot "Soil_Type") suivis d'un
 * label de type de couverture forestière (Cover_Type, 1 à 7). Le label n'est
 * PAS une feature de clustering : StreamKM++ est non supervisé, on le rejette
 * ici, il ne sert que de référence descriptive dans le rapport le cas échéant.
 *
 * Aucune standardisation n'est appliquée (features brutes, comme dans le
 * papier d'origine) : à documenter comme limite dans le rapport, les colonnes
 * one-hot (échelle 0/1) et les mesures brutes (échelle jusqu'à ~7000) pèsent
 * très différemment dans une distance euclidienne au carré.
 */
object CovertypeLoader {

  val NumFeatures: Int = 54
  private val NumColumns: Int = NumFeatures + 1 // + Cover_Type

  /**
   * Parse une ligne en `Point` de poids 1.0 (point brut du flux). `None` si la
   * ligne est malformée — même politique que `Point.parse` (couche 1) :
   * rejeter plutôt que planter tout le job pour une ligne corrompue.
   */
  def parseLine(line: String): Option[Point] = {
    val parts = line.split(',')
    if (parts.length != NumColumns) None
    else {
      val coords = new Array[Double](NumFeatures)
      var i  = 0
      var ok = true
      while (i < NumFeatures && ok) {
        try coords(i) = parts(i).toDouble
        catch { case _: NumberFormatException => ok = false }
        i += 1
      }
      if (ok) Some(Point(coords)) else None
    }
  }

  /** Charge le fichier `path` en `RDD[Point]`, lignes malformées rejetées. */
  def load(sc: SparkContext, path: String): RDD[Point] =
    sc.textFile(path).flatMap(parseLine)
}
