package streamkm.experiments

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CovertypeLoaderSpec extends AnyFunSuite with Matchers {

  private val validLine =
    "2596,51,3,258,0,510,221,232,148,6279,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,5"

  test("parseLine : accepte une ligne Covertype valide, rejette le label") {
    val p = CovertypeLoader.parseLine(validLine)
    p shouldBe defined
    p.get.dim shouldBe CovertypeLoader.NumFeatures
    p.get.coords(0) shouldBe 2596.0
    p.get.coords(53) shouldBe 0.0 // dernière colonne de feature (Soil_Type 40), pas le label 5
    p.get.weight shouldBe 1.0
  }

  test("parseLine : rejette une ligne avec le mauvais nombre de colonnes") {
    CovertypeLoader.parseLine("1,2,3") shouldBe None
  }

  test("parseLine : rejette une ligne non numérique") {
    CovertypeLoader.parseLine(validLine.replace("2596", "abcd")) shouldBe None
  }
}
