package part3.integrations

import common.carsSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

object L1_IntegratingKafka {

  val spark = SparkSession
    .builder()
    .appName("Integrating Kafka")
    .master("local[2]")
    .getOrCreate()


  def readFromKafka() = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe", "testtopic")
      .load()
      // The below select is needed because the data is written in binary in the kafka topic
      .select(col("topic"), expr("value").cast(StringType).as("actualValue"))
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  // For the above you need to do:
  // 1 - docker-compose up
  // 2 - Connect to the container using: docker exec -it rockthejvm-sparkstreaming-kafka bash (check the container name)
  // 3 - navigate to /opt/kafka/bin
  // 4 - ./kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic testtopic
  // 5 - Execute ./kafka-console-producer.sh --broker-list localhost:9092 --topic testtopic
  // 6 - Write the messages to send to the kafka topic

  def writeToKafka() =
    // We need to transform the data to a format that kafka understands, we need to have a key and a value
    spark.readStream.schema(carsSchema)
      .json("src/main/resources/data/cars")
      .selectExpr("upper(Name) as key", "Name as value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("topic", "testtopic")
      .option("checkpointLocation", "checkpoints") // this is mandatory otherwise the write will fail
      .start()
      .awaitTermination()

    // To view if this has worked we can connect to the container and run:
    // /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic testtopic
    // If you want to rerun this application make sure you delete the checkpoints directory


  // Exercise: write the whole cars data structure to kafka as JSON
  def writeToKafkaJson() =
  // We need to transform the data to a format that kafka understands, we need to have a key and a value
    spark.readStream.schema(carsSchema)
      .json("src/main/resources/data/cars")
      .select(col("Name").as("key"), to_json(struct("*").as("value")))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.s ervers","localhost:9092")
      .option("topic", "testtopic")
      .option("checkpointLocation", "checkpoints") // this is mandatory otherwise the write will fail
      .start()
      .awaitTermination()


  def main(args: Array[String]): Unit = {
    // readFromKafka()
    // writeToKafka()
    // spark.readStream.schema(carsSchema)
    //    .json("src/main/resources/data/cars")
    //    .select(col("Name").as("key"), to_json(struct("*")).as("value"))
    //    .writeStream
    //    .format("console")
    //    .start().awaitTermination()

    writeToKafkaJson()
  }


}
