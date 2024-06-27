package example.features

import example.Extra.time
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object schemaEnf extends App{
  val spark = SparkSession.builder()
    .appName("write to delta")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  val delPath = "/home/avyuthan-shah/Desktop/dataF"

  import spark.implicits._
  val newData = Seq(
    ("557777", time.getTime(), "Deposit", "Null", time.getTime(), 0.0, 300.0, 300.0,true),
    ("667777", time.getTime(), "Deposit", "Null", time.getTime(), 0.0, 500.0, 500.0,false),
//    ("887777", time.getTime(), "Deposit", "Null", time.getTime(), 0.0, 300.0, 300.0),
//    ("997777", time.getTime(), "Deposit", "Null", time.getTime(), 0.0, 500.0, 500.0)
  )
    .toDF("AccountNo", "DATE", "TRANSACTIONDETAILS", "CHQNO", "VALUEDATE", "WITHDRAWALAMT", "DEPOSITAMT", "BALANCEAMT","Status")//Status is the new column and it can be enforced by setting mergeSchema option to True
    .withColumn("DATE", to_date($"DATE","yy-MM-dd"))
    .withColumn("VALUEDATE", to_date($"VALUEDATE","yy-MM-dd"))

  val deltaT=DeltaTable.forPath(spark,delPath)

  try {
//    deltaT.as("dt")
//      .merge(
//        newData.as("nd"),"dt.AccountNo=nd.AccountNo AND dt.VALUEDATE=nd.VALUEDATE"
//      )
//      .whenMatched().updateAll()
//      .whenNotMatched().insertAll()
//      .execute()

    //Schema Evolution using option("mergeSchema",true)
    newData.write
      .format("delta")
      .mode("append")
      .option("mergeSchema","true")
      .save(delPath)

    deltaT.toDF.filter(col("AccountNo").isin("557777","667777","887777","997777")).show(truncate=false)

  }catch {
    case e:Exception=>
      println()
      println(s"Error Occured: ${e.getMessage}")
      println()
  }
/*
The merge operation in Delta Lake, as shown in the above code, does not automatically update the schema of the Delta table. The merge operation is primarily used for upserts, allowing to conditionally insert, update, or delete records based on specified conditions.
When using the merge operation to merge data with a Delta table, it assumes that the schema of the incoming data (additionalData) matches the schema of the Delta table (oldData). If there are new columns in the incoming data that are not present in the Delta table, they will be ignored during the merge operation.
To handle schema evolution, one needs to explicitly specify to merge the schema of the incoming data with the schema of the Delta table. It can be achieved by setting the mergeSchema option to true when writing the new data to the Delta table.
 */


}
