package part1.structuredstreaming

import common.{Car, carsSchema}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object L4_StreamingDatasets {
  //DATASETS provides type safety and expresiveness, but there are performance implications as lambdas can not be optimized

  val spark = SparkSession.builder()
    .appName("Streaming Datasets")
    .master("local[2]")
    .getOrCreate()

  // We need to import the encoders to convert case classes to convert DF to DSs
  import spark.implicits._

  def getLines(port:Int = 12345) = spark.readStream.format("socket").option("host", "localhost").option("port", port).load

  def readCarsDataSet():Dataset[Car] = {
    getLines()
      .select(from_json(col("value"), carsSchema).as("car")) // composite column (struct)
      .selectExpr("car.*") // Extract fields from struct
      .as[Car]
  }


  implicit class StreamedDS[T](dataset: Dataset[T]) {
    def stream(mode:String = "append") = dataset.writeStream.format("console").outputMode(mode).start().awaitTermination()
  }
  // You can operate with a Stream of Datasets  like you'll do with a static one (map, filter, flatMap), for example,
  // extract car's name and because it is a dataset, you can maintain the type information Dataset[Car] -> Dataset[String]
  def showCarNames() =
    readCarsDataSet().map(_.Name).stream()

  /**
    * Exercises:
    * - Find cars with hp > 140
    * - Find the average horse power for the entire dataset (use complete outputMode)
    * - Get the cars by their origin
    */

  def EX1_powerfulCars() = readCarsDataSet().filter(_.Horsepower.getOrElse(0L) > 140).stream()
  def EX2_avgPowerfulCars() =
    readCarsDataSet().filter(_.Horsepower.getOrElse(0L) > 140).select(avg(col("HorsePower"))).stream("complete")
  def EX3_carsByOrigin() = readCarsDataSet().groupByKey(_.Origin).count().stream("complete")


  def main(args: Array[String]): Unit = {
    // showCarNames()
    // EX1_powerfulCars()
    // EX2_avgPowerfulCars()
    EX3_carsByOrigin()
  }
}
