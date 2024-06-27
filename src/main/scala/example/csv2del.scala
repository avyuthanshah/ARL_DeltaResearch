package example

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import io.delta.tables.DeltaTable

import example.Extra.status

object csv2del {
  def main(args: Array[String]): Unit = {
    println("CSV To Delta Table")
    val spark=SparkSession.builder()
      .appName("Csv to Delta")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    //Schema for bank_transaction 116k
    val schema = StructType(Array(
      StructField("AccountNo", StringType, nullable = true),
      StructField("DATE", DateType, nullable = false),
      StructField("TRANSACTIONDETAILS", StringType, nullable = true),
      StructField("CHQNO", StringType, nullable = true),
      StructField("VALUEDATE", DateType, nullable = false),
      StructField("WITHDRAWALAMT", DoubleType, nullable = true),
      StructField("DEPOSITAMT", DoubleType, nullable = true),
      StructField("BALANCEAMT", DoubleType, nullable = true)
    ))
    //Schema For rw_transaction
//    val schema = StructType(Array(
//      StructField("txn_id", LongType, nullable = false),
//      StructField("last_modified_date", StringType, nullable = false),
//      StructField("last_modified_date_bs", StringType, nullable = false),
//      StructField("created_date", StringType, nullable = false),
//      StructField("amount", DoubleType, nullable = false),
//      StructField("status", IntegerType, nullable = false),
//      StructField("module_id", IntegerType, nullable = false),
//      StructField("product_id", IntegerType, nullable = false),
//      StructField("product_type_id", IntegerType, nullable = false),
//      StructField("payer_account_id", IntegerType, nullable = false),
//      StructField("receiver_account_id", IntegerType, nullable = false),
//      StructField("reward_point", IntegerType, nullable = false),
//      StructField("cash_back_amount", DoubleType, nullable = false),
//      StructField("revenue_amount", DoubleType, nullable = false),
//      StructField("transactor_module_id", IntegerType, nullable = false),
//      StructField("time", StringType, nullable = false)
//    ))

    val df:DataFrame=spark.read
      .option("header","true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "false")
      .schema(schema)
      .csv("/home/avyuthan-shah/Desktop/F1Intern/Datasets/Bank_transaction/bank.csv")

    import spark.implicits._
    //Filtering Csv file to remove ambiguity during read
//    val filtered_df = df
//      .withColumn("last_modified_date", to_date($"last_modified_date", "yyyy-MM-dd"))
//      .withColumn("created_date", to_date($"created_date", "yyyy-MM-dd"))
//      .withColumn("time", date_format(to_timestamp($"time", "HH:mm:ss"), "HH:mm:ss"))

    val filtered_df = df
      .withColumn("DATE", to_date($"DATE", "dd-MM-yy"))
      .withColumn("VALUEDATE", to_date($"VALUEDATE", "dd-MM-yy"))

    filtered_df.show()
    val repartitioned_df = filtered_df.repartition(4)

    status.writeFile("true")
    Thread.sleep(100)//delay for monitoring

    val startTime=System.nanoTime()
    repartitioned_df.write
      .mode("overwrite")
      .format("delta")
      .save("/home/avyuthan-shah/Desktop/dataF_csv")
    val endTime = System.nanoTime()

    status.writeFile("false")
    val elapsedTime = (endTime - startTime) / 1e9 // Time in seconds
    println()
    println(s"Elapsed time to load from csv: $elapsedTime")

    spark.stop()
  }
}
