package example

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

object csv2del {
  def main(args: Array[String]): Unit = {
    println("CSV To Delta Table")
    val spark=SparkSession.builder()
      .appName("Csv to Delta")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    //Schema for bank_transaction
    val schema = StructType(Array(
      StructField("AccountNo", StringType, nullable = true),
      StructField("DATE", StringType, nullable = false),
      StructField("TRANSACTIONDETAILS", StringType, nullable = true),
      StructField("CHQNO", StringType, nullable = true),
      StructField("VALUEDATE", StringType, nullable = false),
      StructField("WITHDRAWALAMT", StringType, nullable = true),
      StructField("DEPOSITAMT", StringType, nullable = true),
      StructField("BALANCEAMT", StringType, nullable = true)
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
      .csv("/home/avyuthan-shah/Desktop/F1Intern/Datasets/deltaBank_transaction/bank.csv")

    import spark.implicits._  //to use to_date and get map schemas

//Filtering Csv file to remove ambiguity during read
    val filtered_df = df
      .withColumn("last_modified_date", to_date($"last_modified_date", "yyyy-MM-dd"))
      .withColumn("created_date", to_date($"created_date", "yyyy-MM-dd"))
      .withColumn("time", date_format(to_timestamp($"time", "HH:mm:ss"), "HH:mm:ss"))


    filtered_df.show()

    val startTime=System.nanoTime()
    filtered_df.write
      .mode("overwrite")
      .format("delta")
      .save("/home/avyuthan-shah/Desktop/dataF_rwTrans1M")
    val endTime = System.nanoTime()
    val elapsedTime = (endTime - startTime) / 1e9 // Time in seconds
    println()
    println(s"Elapsed time to load from csv: $elapsedTime")
    spark.stop()
  }
}
