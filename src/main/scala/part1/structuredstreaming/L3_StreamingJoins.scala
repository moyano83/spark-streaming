package part1.structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object L3_StreamingJoins {

  val spark = SparkSession.builder()
    .appName("Streaming Joins")
    .master("local[2]")
    .getOrCreate()

  def getLines(port:Int = 12345) = spark.readStream.format("socket").option("host", "localhost").option("port", port).load

  val guitarPlayers = spark
    .read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitarPlayers")
  val guitars = spark
    .read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitars")
  val bands = spark
    .read
    .option("inferSchema", true)
    .json("src/main/resources/data/bands")

  // In static DF, we will join to DFs as below
  val guitarBands = guitarPlayers.join(bands, guitarPlayers("band") === bands("id"), "inner")

  // from_json function gives back a composed column based on the schema passed
  val streamBandDF = getLines()
    .select(from_json(col("value"), bands.schema).as("band"))
    .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

  val streamGuitarPlayersDF = getLines(12346)
    .select(from_json(col("value"), guitarPlayers.schema).as("guitarist"))
    .selectExpr("guitarist.id as id", "guitarist.name as name", "guitarist.guitars as guitars", "guitarist.band as band")


  // This is how we'll Join it in a stream context
  def joinStreamWithStatic() = {
    // The previousDF can be joined with a static DF like any other DF
    val streamBandGuitarristDF = streamBandDF.join(guitarPlayers, guitarPlayers("band") === streamBandDF("id"), "inner")

    // Now we print the results
    streamBandGuitarristDF.writeStream.format("console").outputMode("append").start().awaitTermination()
  }

  // Since Spark 2.3 joining stream with stream is permitted, but only append output Mode is supported
  // inner joins and left/right joins are supported, but MUST have WATERMARKS
  // full outer joins are NOT suported
  def joinWithStream() = {
    val streamedJoin = streamBandDF.join(streamGuitarPlayersDF, streamBandDF("id") === streamGuitarPlayersDF("band"))

    streamedJoin.writeStream.format("console").outputMode("append").start().awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    // Notice that the join happens per batch of stream data (lots of small joins)
    // Important:
    // - In Stream Joining with static, RIGHT OUTER JOIN, FULL OUTER JOIN AND RIGHT SEMI JOIN are not permitted
    // - Static Join with streaming, LEFT OUTER JOIN, FULL JOIN AND LEFT SEMI JOIN are not permitted
    // This is because otherwise spark would have to keep track of all data streamed, which is by definition impossible
    // joinStreamWithStatic()
    // For this to work open a terminal in 12345 and another in 123456
    joinWithStream()
  }
}
