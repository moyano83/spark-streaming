package part3.integrations

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util

object L2_IntegratingKafkaDStreams {

  val spark = SparkSession
    .builder()
    .appName("Spark DStreams + Kafka")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  // Reading from kafka with DStreams is a bit different, we need to create a map of properties specifying the
  // serializer/deserializing classes
  val kafkaProps: Map[String, Object] = Map(
    "bootstrap.servers" -> "localhost:9092",
    "key.serializer" -> classOf[StringSerializer],
    "value.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> false.asInstanceOf[Object] // because 'false' is not a object
  )

  val kafkaTopic = "testtopic"

  def readFromKafka() = {
    val kafkaTopics = Array(kafkaTopic)
    val kafkaDStreams = KafkaUtils.createDirectStream(
      ssc, // streaming context
      // LocationStrategy that distributes partitions evenly accross the spark cluster. If spark brokers and executors are
      // colocated in the same cluster use PreferBrokers instead
      PreferConsistent,
      // Consumer strategy to the topics provided, the group id is particular to this particular stream
      // Alternative to this is SubscribePattern that allows as to provide a regex as topics. Use Assign to specify
      // offsets and partition per topic. The type parameters represents the types of the key and the value
      ConsumerStrategies.Subscribe[String, String](kafkaTopics, kafkaProps + ("group.id" -> "group1"))
    )

    // Then we can extract the stream
    val processStream = kafkaDStreams.map(record => (record.key(), record.value()))
    processStream.print()
    ssc.start()
    ssc.awaitTermination()
  }

  def writeToKafka() = {
    val inputData = ssc.socketTextStream("localhost", 12345)
    // We need a key value stream
    val processedData = inputData.map(line => line.toUpperCase())

    processedData.foreachRDD(rdd =>
      rdd.foreachPartition(partition =>{
        // Inside this lambda, the code is processed by a single executor
        // First we need a java hashmap created from the kafka props map
        val kafkaHashMap = new java.util.HashMap[String, Object]()
        kafkaProps.foreach(pair=> kafkaHashMap.put(pair._1, pair._2))

        // Now we need to create a producer, which will be available on this executor, make sure you create it for each
        // partition and not for each record
        val producer = new KafkaProducer[String, String](kafkaHashMap)
        partition.foreach(record =>{
          // Then we create a new record passing the topic, key and value
          val message = new ProducerRecord[String, String](kafkaTopic, null, record)
          // Then we need to send the message to the kafka topic using the producer created before
          producer.send(message)
        })
        // Make sure to close the producer after
        producer.close()
    }))

    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    // we need to open a terminal, to produce messages from the kafka container using the script and steps specified in
    // the previous lesson
    //readFromKafka()

    // For the test we need to open netcat on 12345, and also a consumer in kafka to see the messages
    writeToKafka()
  }
}
