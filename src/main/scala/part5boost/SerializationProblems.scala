package part5boost

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import part5boost.SerializationProblems.Person

object SerializationProblems {

  val spark = SparkSession
    .builder()
    .appName("Serialization Problems")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  val rdd = sc.parallelize(1 to 100)

  class RDDMultiplier {
    def multiplyRDD() = rdd.map(_ * 2).collect().toList
  }

  val rddMultiplier = new RDDMultiplier
//  rddMultiplier.multiplyRDD()
  // works

  // make class serializable
  class MoreGeneralRDDMultiplier extends Serializable {
    val factor = 2
    def multiplyRDD() = rdd.map(_ * factor).collect().toList
  }
//  val moreGeneralRDDMultiplier = new MoreGeneralRDDMultiplier
//  moreGeneralRDDMultiplier.multiplyRDD()

  // technique: enclose member in local value
  class MoreGeneralRDDMultiplierEnclosure {
    val factor = 2
    def multiplyRDD() = {
      val enclosedFactor = factor
      rdd.map(_ * enclosedFactor).collect().toList
    }
  }
  val moreGeneralRDDMultiplier2 = new MoreGeneralRDDMultiplierEnclosure
  moreGeneralRDDMultiplier2.multiplyRDD()

  /** Exercise
    */
  class MoreGeneralRDDMultiplierNestedClass {
    val factor = 2

    object NestedMultiplier extends Serializable {
      val extraTerm = 10
      val enclosedFactor = factor
      def multiplyRDD() =
        rdd.map(_ * enclosedFactor + extraTerm).collect().toList
    }
  }
  val nestedMulitplier = new MoreGeneralRDDMultiplierNestedClass
  nestedMulitplier.NestedMultiplier.multiplyRDD()

  /** Exercise 2
    */
  case class Person(name: String, age: Int)
  val people: RDD[Person] = sc.parallelize(
    List(
      Person("Alice", 43),
      Person("Bob", 12),
      Person("Charlie", 23),
      Person("Diana", 67)
    )
  )

  class LegalDrinkingAgeChecker(legalAge: Int) {
    def processPeople() = {
      val ageThreshold =
        legalAge // capture the constructor argument in a local value
      people.map(_.age >= ageThreshold).collect().toList
    }
  }
  val peopleChecker = new LegalDrinkingAgeChecker(21)
  peopleChecker.processPeople()

  /** Take things up a notch
    * Exercise 3: make this work
    * - maybe change the structure of the inner classes
    * - don't use Serializable
    *
    * Hint: FunctionX types are serializable
    */

  class PersonProcessor { // <- can't put Serializable here
    class DrinkingAgeChecker(legalAge: Int) {
      val check = { // <- 1: make this a val instead of a method
        val enclosedLegalAge =
          legalAge // <- 3: capture the constructor argument
        age: Int => age >= enclosedLegalAge
      }
    }

    class DrinkingAgeFlagger(checker: Int => Boolean) {
      def flag() = {
        val enclosedChecker = checker // <- 2: capture the lambda
        people.map(p => enclosedChecker(p.age)).collect().toList
      }
    }

    def processPeople() = {
      val usChecker = new DrinkingAgeChecker(21)
      val flagger = new DrinkingAgeFlagger(usChecker.check)
      flagger.flag()
    }
  }

  val personProcessor = new PersonProcessor
  personProcessor.processPeople()

  def main(args: Array[String]): Unit = {}

}
