package example

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object getBal extends App{

  def updTab(spark: SparkSession, accNo: String, addBal: Double): Unit = {
    val delPath = "/home/avyuthan-shah/Desktop/dataF"
    spark.read.format("delta").option("header", "true").load(delPath).createOrReplaceTempView("delta_table")

    val result = spark.sql(
      s"""
         |SELECT BALANCEAMT FROM delta_table
         |WHERE AccountNo = $accNo
         |ORDER BY VALUEDATE DESC
         |LIMIT 1;
         |""".stripMargin)

    val balance = result.head.getAs[Double]("BALANCEAMT")

    val dT = DeltaTable.forPath(spark, delPath)
    dT.update(
      condition = col("AccountNo") === accNo,
      set = Map("DEPOSITAMT" -> lit(balance+addBal), "BALANCEAMT" -> lit(balance + addBal))
    )
    dT.toDF.filter(col("AccountNo") === accNo).show()
  }

  def getTab(spark: SparkSession, accNo: String): Unit = {
    val delPath = "/home/avyuthan-shah/Desktop/dataF2"
    spark.read.format("delta").option("header", "true").load(delPath).createOrReplaceTempView("delta_table")

    val result = spark.sql(
      s"""
         |SELECT BALANCEAMT FROM delta_table
         |WHERE AccountNo = $accNo
         |ORDER BY VALUEDATE DESC
         |LIMIT 1;
         |""".stripMargin)

    val balance = result.head.getAs[Double]("BALANCEAMT")

    println(s"Current balance for account $accNo: $balance")
  }
}
