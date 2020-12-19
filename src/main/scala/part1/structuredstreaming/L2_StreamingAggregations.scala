package part1.structuredstreaming

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object L2_StreamingAggregations {

  val spark = SparkSession.builder()
    .appName("Streaming Aggregations")
    .master("local[2]")
    .getOrCreate()

  def getLines() = spark.readStream.format("socket").option("host", "localhost").option("port", 12345).load()


  def streamingCount()={
    // Lets count the lines of the above dataframe. In this case we select outputMode complete because the modes Append
    // and update are not supported on aggregations without water mark (explained later). Neither distinct is supported
    // because spark would have to keep track of all data in memory
    getLines()
      .selectExpr("count(*) as lineCount")
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  // We can perform aggregations or other functions over  the DF
  def numericalAggregations(aggFunction: Column => Column) = {
    val numbers = getLines().select(col("value").cast("integer").as("numbers"))
    val aggDF = numbers.select(aggFunction(col("numbers")).as("agg_so_far"))

    aggDF
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  // Grouping is also supported
  def groupNames():Unit = {
    // This returns a relational Grouped Dataset
    getLines()
      .select(col("value").as("name"))
      .groupBy(col("name"))
      .count()
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    //streamingCount()
    //numericalAggregations(sum)
    groupNames()
  }
}
