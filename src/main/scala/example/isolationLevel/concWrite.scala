package example.isolationLevel

import example.Extra.time
import org.apache.spark.sql.SparkSession
import io.delta.tables.DeltaTable

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object concWrite extends App{
  implicit val ec: ExecutionContext = ExecutionContext.global
  val spark = SparkSession.builder()
    .appName("ConcurrentWrite")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  val deltaPath = "/home/avyuthan-shah/Desktop/dataF"


  def writeOperation(spark: SparkSession, accountNo: String): Future[Unit] = Future {
    var success = false
    var attempt = 0
    val maxRetries = 3
    val delayBetweenRetries = 1000 // milliseconds

    while (!success && attempt < maxRetries) {
      attempt += 1
      println()
      println(s"Write operation for account $accountNo started, attempt $attempt")
      println()

      try {
        val latest_balance=example.Extra.getBal.getTabBal(spark,deltaPath,accountNo)

        val deltaTable = DeltaTable.forPath(spark, deltaPath)
        deltaTable.as("dt").updateExpr(
          s"AccountNo = '$accountNo'AND BALANCEAMT='$latest_balance'",
          Map(
            "DEPOSITAMT" -> "DEPOSITAMT + 100.0",
            "BALANCEAMT" -> "BALANCEAMT + 100.0",
            "VALUEDATE" -> s"to_date('${time.getTime()}', 'yy-MM-dd')"
          )
        )
        println()
        println(s"Write operation for account $accountNo completed successfully")
        println()
        success = true
      } catch {
        case e: Exception =>
          println()
          println(s"Error in write operation for account $accountNo on attempt $attempt: ${e.getMessage}")
          println()
          if (attempt < maxRetries) {
            println()
            println(s"Retrying after $delayBetweenRetries ms")
            println()
            Thread.sleep(delayBetweenRetries)
          } else {
            println()
            println(s"Max retries reached for account $accountNo")
            println()
            throw e
          }
      }
    }
  }

  val writeFuture1 = writeOperation(spark, "557777")
  val writeFuture2 = writeOperation(spark, "557777")

  val combinedFuture = Future.sequence(Seq(writeFuture1, writeFuture2))

  combinedFuture.onComplete {
    case Success(_) =>
      println()
      println("Both write operations completed successfully")
      println()
      spark.stop()
    case Failure(e) =>
      println()
      println(s"One or more write operations failed: ${e.getMessage}")
      println()
      spark.stop()
  }

  Await.result(combinedFuture, Duration.Inf)

}
