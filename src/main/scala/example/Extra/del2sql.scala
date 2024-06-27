package example.Extra

import org.apache.spark.sql.{SaveMode, SparkSession}
//import io.delta.tables.DeltaTable
//import org.apache.spark.sql.functions._

object del2sql extends App{
  val spark=SparkSession.builder()
    .appName("Delta 2 Sql")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  val delPath="/home/avyuthan-shah/Desktop/dataF_rwTrans1M"
  val dT=spark.read.format("delta").option("header","true").option("treatEmptyValuesAsNulls", "true").load(delPath)


  // Define JDBC connection parameters for MySQL
  val jdbcUrl = "jdbc:mysql://localhost:3306/mydB"
  val jdbcUsername = "root"
  val jdbcPassword = "$QL007server"

  // JDBC properties
  val jdbcProperties = new java.util.Properties()
  jdbcProperties.setProperty("user", jdbcUsername)
  jdbcProperties.setProperty("password", jdbcPassword)
  jdbcProperties.setProperty("driver", "com.mysql.cj.jdbc.Driver")


  val startTime=System.nanoTime()
  // Write Delta table to MySQL
  dT.write.mode(SaveMode.Overwrite)
    .jdbc(jdbcUrl, "rw_1M", jdbcProperties)

  val endTime = System.nanoTime()
  val elapsedTime = (endTime - startTime) / 1e9 // Time in seconds
  println()
  println(s"Elapsed Time to upload into Sql server : $elapsedTime")
  println()

}
