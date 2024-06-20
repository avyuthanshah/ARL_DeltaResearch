package example
import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import io.delta.tables.DeltaTable

object upsert extends App{
    val spark = SparkSession.builder()
      .appName("write to delta")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val delPath = "/home/avyuthan-shah/Desktop/dataF"

//    import spark.implicits._
//    val df = Seq(
//      ("337777", "2024-06-06", "Deposit", "Null", "2024-06-06", 0.0, 300.0, 300.0),
//      ("337777", "2024-06-09", "Withdraw", "Null", "2024-06-09", 100.0, 0.0, 200.0),
//      ("447777", "2024-06-09", "Deposit", "Null", "2024-06-06", 0.0, 500.0, 500.0),
//      ("447777", "2024-06-02", "Withdraw", "Null", "2024-06-02", 300.0, 0.0, 100.0),
//      ("447777", "2024-06-03", "Withdraw", "Null", "2024-06-03", 100.0, 0.0, 0.0)
//    )
//      .toDF("AccountNo", "DATE", "TRANSACTIONDETAILS", "CHQNO", "VALUEDATE", "WITHDRAWALAMT", "DEPOSITAMT", "BALANCEAMT")
//      .withColumn("DATE", to_date($"DATE"))
//      .withColumn("VALUEDATE", to_date($"VALUEDATE"))

//
//  deltaT.as("dt")
//    .merge(
//      newData.as("nd"),
//      "dt.AccountNo = nd.AccountNo AND dt.VALUEDATE = nd.VALUEDATE")
//    .whenMatched().updateAll() //Note: To use updateAll the schema should match for both dataframe otherwise use updateExpr with Map() to map schema
//    .whenNotMatched().insertAll()//Same condition as updateAll
//    .execute()
//  println(s"Folder Size After : ${Extra.folderSize.getCurrentFolderSize(delPath)}")

    //Schema for bank_transaction
    val schema = StructType(Array(
      StructField("AccountNo", StringType, nullable = false),
      StructField("DATE", StringType, nullable = false),
      StructField("TRANSACTIONDETAILS", StringType, nullable = false),
      StructField("CHQNO", StringType, nullable = true),
      StructField("VALUEDATE", StringType, nullable = false),
      StructField("WITHDRAWALAMT", DoubleType, nullable = true),
      StructField("DEPOSITAMT", DoubleType, nullable = true),
      StructField("BALANCEAMT", DoubleType, nullable = true)
    ))

    val df:DataFrame=spark.read
//      .option("header","true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "false")
      .schema(schema)
      .csv("/home/avyuthan-shah/Desktop/F1Intern/Datasets/banktrans100k.csv")

    import spark.implicits._  //to use to_date and get map schemas

    //Filtering Csv file to remove ambiguity during read
    val new_df = df
      .withColumn("AccountNo", regexp_replace($"AccountNo", "'", ""))
      .withColumn("DATE", regexp_replace($"DATE", "'", ""))
      .withColumn("CHQNO", regexp_replace($"CHQNO", "'", ""))
      .withColumn("TRANSACTIONDETAILS", regexp_replace($"TRANSACTIONDETAILS", "'", ""))
      .withColumn("VALUEDATE", regexp_replace($"VALUEDATE", "'", ""))

    val filtered_df=new_df
      .withColumn("DATE", to_date($"DATE", "yyyy-MM-dd"))
      .withColumn("VALUEDATE", to_date($"VALUEDATE", "yyyy-MM-dd"))

    filtered_df.show()

    println(s"Folder Size Before: ${Extra.folderSize.getCurrentFolderSize(delPath)}")

  //using merge approach to insert data into delta table

    val deltaT=DeltaTable.forPath(spark,delPath)

    val startTime=System.nanoTime()

    deltaT.as("dt")
      .merge(
        filtered_df.as("nd"),
        "dt.AccountNo = nd.AccountNo AND dt.VALUEDATE = nd.VALUEDATE AND dt.BALANCEAMT=nd.BALANCEAMT")
      .whenMatched().updateAll() //Note: To use updateAll the schema should match for both dataframe otherwise use updateExpr with Map() to map schema
      .whenNotMatched().insertAll()//Same condition as updateAll
      .execute()

    val endTime = System.nanoTime()
    val elapsedTime = (endTime - startTime) / 1e9 // Time in seconds
    println()
    println(s"Elapsed time to load from csv: $elapsedTime")
    println(s"Folder Size After Uploading 1000 Data: ${Extra.folderSize.getCurrentFolderSize(delPath)}")

    spark.stop()
}
