package part5boost

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf

object KryoSerializer {
  // 1- define a SparkConf object with the Kryo serializer
  val sparkConf = new SparkConf()
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrationRequired", "true")
    .registerKryoClasses(
      Array( // 2 - register the classes we want to serialize
        classOf[Person],
        classOf[Array[Person]]
      )
    )

  val spark = SparkSession
    .builder()
    .appName("Kryo Serialization")
    .config(sparkConf) // 3 - pass the SparkConf object to the SparkSession
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  case class Person(name: String, age: Int)
  def generatePeople(nPersons: Int) =
    (1 to nPersons).map(i => Person(s"Persons$i", i % 100))

  val people = sc.parallelize(generatePeople(10000000))

  def testCaching() = {
    people.persist(StorageLevel.MEMORY_ONLY_SER).count()
    /*
      Compare:
        Java Serialization
         - memory usage 254 MB
         - time 36s

        Kryo Serialization
         - memory usage 174 MB
         - time 19s
     */
  }

  def testShuffling() = {
    people.map(p => (p.age, p)).groupByKey().mapValues(_.size).count()
    /*
  Compare:
    Java Serialization
     - shuffle 74.3 MB
     - time 27s

    Kryo Serialization
     - shuffle 48 MB
     - time 21s
     */
  }

  def main(args: Array[String]): Unit = {
    testShuffling()
    Thread.sleep(1000000)
  }

}
