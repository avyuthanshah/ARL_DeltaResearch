package example
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import io.delta.tables.DeltaTable

object upsert extends App{
    val spark = SparkSession.builder()
      .appName("write to delta")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val delPath = "/home/avyuthan-shah/Desktop/dataF"

    import spark.implicits._
    val newData = Seq(
      ("337777", "2024-06-06", "Deposit", "Null", "2024-06-06", 0.0, 300.0, 300.0),
      ("337777", "2024-06-09", "Withdraw", "Null", "2024-06-09", 100.0, 0.0, 200.0),
      ("447777", "2024-06-09", "Deposit", "Null", "2024-06-06", 0.0, 500.0, 500.0),
      ("447777", "2024-06-02", "Withdraw", "Null", "2024-06-02", 300.0, 0.0, 100.0),
      ("447777", "2024-06-03", "Withdraw", "Null", "2024-06-03", 100.0, 0.0, 0.0)
    )
      .toDF("AccountNo", "DATE", "TRANSACTIONDETAILS", "CHQNO", "VALUEDATE", "WITHDRAWALAMT", "DEPOSITAMT", "BALANCEAMT")
      .withColumn("DATE", to_date($"DATE"))
      .withColumn("VALUEDATE", to_date($"VALUEDATE"))

    val deltaT=DeltaTable.forPath(spark,delPath)

  deltaT.as("dt")
    .merge(
      newData.as("nd"),
      "dt.AccountNo = nd.AccountNo AND dt.VALUEDATE = nd.VALUEDATE")
    .whenMatched().updateAll() //Note: To use updateAll the schema should match for both dataframe otherwise use updateExpr with Map() to map schema
    .whenNotMatched().insertAll()//Same condition as updateAll
    .execute()

  spark.stop()
}
