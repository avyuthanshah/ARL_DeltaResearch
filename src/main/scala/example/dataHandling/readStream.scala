package example.dataHandling

import org.apache.spark.sql.SparkSession

object readStream extends App{
  val spark = SparkSession.builder()
    .appName("write to delta")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  val stream = spark.readStream
    .format("delta")
    .option("startingVersion", "0")
    //.option("skipChangeCommits", "true") // Ignore updates and deletes. Completely ignores updates and deletes, treating the data as static.
    .option("ignoreChanges","true") //ignoreChanges in Delta Lake allows structured streaming to handle updates and deletes in a way that fits real-time data processing.
    // It ensures that changes to data are properly reflected downstream, though it may introduce duplicates for unchanged data.
    // This option is useful when you need to maintain data integrity and continuity in your streaming pipelines despite ongoing updates and occasional deletions.
    .load("/home/avyuthan-shah/Desktop/dataF")

    .writeStream
    .format("console")
    //.outputMode("update")//Only the rows that were updated in the result table since the last trigger are output.
    .outputMode("append")//the entire result table for each trigger interval and writes the complete set of results to the sink when there are streaming aggregations on streaming DataFrames/Datasets
    //.outputMode("append")// New rows added since the last trigger.
    //.option("checkpointLocation", "/home/avyuthan-shah/Desktop/dataFcheckpoints")
    .start()

  stream.awaitTermination()
}
