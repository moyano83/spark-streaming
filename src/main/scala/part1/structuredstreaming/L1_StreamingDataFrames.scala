package part1.structuredstreaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import common._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._


object L1_StreamingDataFrames {

  val spark = SparkSession.builder()
    .appName("First Streams")
    .master("local[2]")
    .getOrCreate()

  // We will use sockets to read data from
  def readFromSocket() = {
    // This is a DataFrame, in this case a streaming dataframe
    val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 12345).load()

    // Once the input has been read, it can be transformed like any other dataframe, for example filtering it
    val filteredDF = lines.filter(length(col("value")) <= 5)

    //To find if a DF is static or is streaming you can call the following method
    println(filteredDF.isStreaming)

    // In streaming we need to start a streaming query. This as any other transformation is lazy and just adds to the
    // query graph. Append is the default outputMode
    val query = filteredDF.writeStream.format("console").outputMode("append").start()
    // We need to wait for it after to finish
    query.awaitTermination()
  }

  // You can also stream the contents of a file like a stream
  def readFromFile() = {
    val stocksDF = spark.readStream.format("csv")
      .option("header", "false")
      .option("dateFormat","MMM d yyyy")
      .schema(stocksSchema) // As oppose to the static DF, you need to provide a schema
      .load("src/main/resources/data/stocks")

    stocksDF.writeStream.format("console").outputMode("append").start().awaitTermination()
    // The difference between this stream and a static DF is that if the stream is running and we add another file to the
    // folder specified, spark will pick it up and process it automatically
  }

  def demoTriggers(): Unit ={
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .writeStream
      .format("console")
      .outputMode("append")
      //.trigger(Trigger.ProcessingTime(2.second)) // Runs a query periodically based on the interval passed
      // .trigger(Trigger.Once()) // Process a single batch and terminates the query
      .trigger(Trigger.Continuous(2.second)) // Spark creates a batch every 2 seconds regardless if there is data or not
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    // Before running this program, open a terminal window and type: `nc -lk 12345` and start to send data
    //readFromSocket()
    // Read from file
    //readFromFile()
    demoTriggers()
  }
}