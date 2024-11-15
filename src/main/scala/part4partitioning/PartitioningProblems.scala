package part4partitioning

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SizeEstimator

object PartitioningProblems {

  val spark = SparkSession
    .builder()
    .appName("Partitioning Problems")
    .master("local[*]") // for parallelism
    .getOrCreate()

  def processNumbers(nPartitions: Int) = {
    val numbers = spark.range(100000000) // ~800mb
    val repartitionedNumbers = numbers.repartition(nPartitions)
    repartitionedNumbers.cache()
    repartitionedNumbers
      .count() // dummy action to force the shuffle to take place and store it in memory

    // the computation I care about
    repartitionedNumbers.selectExpr("sum(id)").show()
  }

  // 1 - use size estimator
  def dfSizeEstimator() = {
    val numbers = spark.range(100000)
    println(
      SizeEstimator.estimate(numbers)
    ) // 3749752b = 3.5mb - usually works, not super accurate, within an order of magnitude - larger number
    // measures the memory footprint of the actual JVM object backing the dataset
    numbers.cache() // 	101.5 kb
    numbers.count()
  }

  // 2 - use query plan
  def estimateWithQueryPlan() = {
    val numbers = spark.range(100000)
    println(
      numbers.queryExecution.optimizedPlan.stats.sizeInBytes
    ) // 800000b - accurate size in bytes for the DATA
  }

  def estimateRDD() = {
    val numbers = spark.sparkContext.parallelize(1 to 100000)
    numbers.cache().count() // 390.8 kb
  }

  def main(args: Array[String]): Unit = {
//    processNumbers(2) // 400mb / partition
//    processNumbers(20) // 40mb / partition
//    processNumbers(200) // 4mb / partition
//    processNumbers(2000) // 400kb / partition
//    processNumbers(20000) // 40kb / partition

//    dfSizeEstimator()
//    estimateWithQueryPlan()
    estimateRDD()
    // 10-100mb rule for partitions size for UNCOMPRESSED DATA
    Thread.sleep(10000000)
  }

}
