package example
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import example.Extra.status

object parq2delt extends App{

  val spark = SparkSession.builder()
    .appName("Parquet to Delta")
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

  val df: DataFrame = spark.read
    .format("parquet")
    .option("header", "true")
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "false")
    .schema(schema)
    .load("/home/avyuthan-shah/Desktop/F1Intern/Datasets/Bank_transaction/bank_parquet")

  df.show()

  val repartitioned_df = df.repartition(4)
  //Thread.sleep(5000)//sleep to coordinate with resourceMonitor

  //df.write.mode("overwrite").format("parquet").save("/home/avyuthan-shah/Desktop/F1Intern/Datasets/deltaBank_transaction/deltaBT")


  status.writeFile("true")
  Thread.sleep(100)

  val startTime = System.nanoTime()
  repartitioned_df.write
    .mode("overwrite")
    .format("delta")
    .save("/home/avyuthan-shah/Desktop/dataF")
  val endTime = System.nanoTime()

  status.writeFile("false")


  val elapsedTime = (endTime - startTime) / 1e9 // Time in seconds
  println()
  println(s"Elapsed Time to upload into delta from parquet : $elapsedTime")
  println()

  spark.stop()
}
