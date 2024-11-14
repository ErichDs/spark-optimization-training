package part3caching

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Checkpointing {

  val spark = SparkSession
    .builder()
    .appName("Checkpointing")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext

  def demoCheckpoint() = {
    val flightsDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/flights")

    // do some expensive computation
    val orderedFlights = flightsDF.orderBy("dist")

    // checkpointing is used to avoid falilure in computations
    // needs to be configured
    sc.setCheckpointDir("spark-warehouse")

    //checkpoint a DF = save the DF to disk
    val checkpointedFlights = orderedFlights.checkpoint() // an action

    // query plan difference with checkpointed DFs
    orderedFlights.explain()
    /*
    == Physical Plan ==
     *(1) Sort [dist#16 ASC NULLS FIRST], true, 0
    +- Exchange rangepartitioning(dist#16 ASC NULLS FIRST, 200), true, [id=#10]
       +- FileScan json [_id#7,arrdelay#8,carrier#9,crsarrtime#10L,crsdephour#11L,crsdeptime#12L,crselapsedtime#13,depdelay#14,dest#15,dist#16,dofW#17L,origin#18] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex[file:/Users/erich.parmejano/Documents/Personal/Trainings/spark-optimization/src..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<_id:string,arrdelay:double,carrier:string,crsarrtime:bigint,crsdephour:bigint,crsdeptime:b...
     */

    checkpointedFlights.explain()
    /*
    == Physical Plan ==
     *(1) Scan ExistingRDD[_id#7,arrdelay#8,carrier#9,crsarrtime#10L,crsdephour#11L,crsdeptime#12L,crselapsedtime#13,depdelay#14,dest#15,dist#16,dofW#17L,origin#18]
     */

    checkpointedFlights.show()
  }

  def cachingJobRDD() = {
    val numbers = sc.parallelize(1 to 10000000)
    val descNumbers = numbers.sortBy(-_).persist(StorageLevel.DISK_ONLY)
    descNumbers.sum()
    descNumbers.sum() // shorter time here
  }

  def checkpointingJobRDD() = {
    sc.setCheckpointDir("spark-warehouse")
    val numbers = sc.parallelize(1 to 10000000)
    val descNumbers = numbers.sortBy(-_)
    descNumbers.checkpoint() // checkpoint on RDDs returns Unit
    descNumbers.sum()
    descNumbers.sum()
  }
  /* caching is faster, but checkpointing is more reliable, since it saves the data to avoid recomputation */

  def cachingJobDF() = {
    val flightsDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/flights")

    val orderedFlights = flightsDF.orderBy("dist")
    orderedFlights.persist(StorageLevel.DISK_ONLY)
    orderedFlights.count()
    orderedFlights.count() // should be a shorter job
  }

  def checkpointingJobDF() = {
    sc.setCheckpointDir("spark-warehouse")
    val flightsDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/flights")

    val orderedFlights = flightsDF.orderBy("dist")
    val checkpointedFlights = orderedFlights.checkpoint()
    checkpointedFlights.count()
    checkpointedFlights.count()
  }

  /*
    Checkpointing:
    - saves the RDD/DF to external storage and forgets its lineage
    - makes an intermediate RDD/DF available to other jobs
    - takes more space and is slower than caching
    - does not use Spark memory
    - does not force recomputation of a partition if a node fails

    -> meant to be used when we can't afford recomputation;
    - if a job is slow, use caching
   */

  def main(args: Array[String]): Unit = {
    cachingJobDF()
    checkpointingJobDF()
    Thread.sleep(1000000)
  }
}
