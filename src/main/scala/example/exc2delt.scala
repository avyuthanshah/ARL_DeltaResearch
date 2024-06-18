package example

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._

object exc2delt {
  def main(args: Array[String]): Unit = {
    println("Excel To Delta Table")
    val spark=SparkSession.builder()
      .appName("ExcelToDelta")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
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


    val df:DataFrame=spark.read
      .format("com.crealytics.spark.excel")
      .option("header","true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "false")
      .schema(schema)
      .load("/home/avyuthan-shah/Desktop/F1Intern/Datasets/deltaBank_transaction/bank.xlsx")

    df.show()

    //df.write.mode("overwrite").format("parquet").save("/home/avyuthan-shah/Desktop/F1Intern/Datasets/deltaBank_transaction/deltaBT")
    val startTime=System.nanoTime()
    df.write
      .mode("overwrite")
      .format("delta")
      .save("/home/avyuthan-shah/Desktop/dataF")
    val endTime = System.nanoTime()
    val elapsedTime = (endTime - startTime) / 1e9 // Time in seconds
    println()
    println(s"Elapsed Time to upload into delta from excel : $elapsedTime")
    println()
    spark.stop()
  }
}