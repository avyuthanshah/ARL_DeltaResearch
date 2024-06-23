package example

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object upsert2 extends App{
    val spark = SparkSession.builder()
      .appName("write to delta")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val delPath = "/home/avyuthan-shah/Desktop/dataF"

    import spark.implicits._
    val df = Seq(
        ("557777", "2024-06-16", "Deposit", "Null", "2024-06-16", 0.0, 100.0, 400.0),
        ("557777", "2024-06-18", "Deposit", "Null", "2024-06-18", 0.0, 300.0, 700.0),
        ("667777", "2024-06-13", "Deposit", "Null", "2024-06-13", 0.0, 8000.0, 8000.0)
    )
      .toDF("AccountNo", "DATE", "TRANSACTIONDETAILS", "CHQNO", "VALUEDATE", "WITHDRAWALAMT", "DEPOSITAMT", "BALANCEAMT")
      .withColumn("DATE", to_date($"DATE"))
      .withColumn("VALUEDATE", to_date($"VALUEDATE"))

    val deltaT=DeltaTable.forPath(spark,delPath)

    deltaT.as("dt")
      .merge(
        df.as("nd"),
        "dt.AccountNo = nd.AccountNo AND dt.VALUEDATE = nd.VALUEDATE")
      .whenMatched().updateAll() //Note: To use updateAll the schema should match for both dataframe otherwise use updateExpr with Map() to map schema
      .whenNotMatched().insertAll()//Same condition as updateAll
      .execute()
    println(s"Folder Size After : ${Extra.folderSize.getCurrentFolderSize(delPath)}")


    spark.stop()
}
