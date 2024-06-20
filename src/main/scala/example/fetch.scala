package example

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object fetch extends App{
  val spark=SparkSession.builder()
    .appName("Fetch Delta Table")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  val delPath="/home/avyuthan-shah/Desktop/dataF"
  val version=DeltaTable.forPath(spark, delPath).history(1).select("version").as[Long](Encoders.scalaLong).head //Time Travel Feature can be accessed by mentioning version of table in option
  // Register the Delta table as a temporary view
  spark.read.format("delta").option("header","true").option("versionAsOf",version).load(delPath).createOrReplaceTempView("delta_table")

  val startTime = System.nanoTime()

  // Run SQL queries on the Delta table
//  val result = spark.sql(
//  s"""
//      |SELECT * FROM delta_table
//      |WHERE AccountNo IN ("557777","667777") AND VALUEDATE IS NULL
//      |ORDER BY VALUEDATE DESC;
//      """.stripMargin)
  val result = spark.sql(
    s"""
       |SELECT * FROM delta_table
       |WHERE AccountNo IN ("134286","359012");
      """.stripMargin)

  val endTime = System.nanoTime()
  val elapsedTime = (endTime - startTime) / 1e9 // Time in seconds

  result.show(truncate=false)

  //Using dataframe
//  val startTime2 = System.nanoTime()
//  val df:DataFrame=spark.read
//    .format("delta")
//    .option("header","true")
//    .option("versionAsOf",version)
//    .option("treatEmptyValueAsNulls","true")
//    .load(delPath)
//
//  val filterdf = df
//    .filter(col("AccountNo").isin("557777", "667777"))
//    .orderBy(col("VALUEDATE").desc)
//
//  val endTime2 = System.nanoTime()
//  val elapsedTime2 = (endTime2 - startTime2) / 1e9 // Time in seconds
//
//  filterdf.show(truncate=false)
//
//  println()
  println(s"Elapsed time for query: '$elapsedTime' ")
//  println(s"Elapsed time for query using dataframe method: '$elapsedTime2' ")
//  print(s"${time.getTime()}")
//  println()
}
