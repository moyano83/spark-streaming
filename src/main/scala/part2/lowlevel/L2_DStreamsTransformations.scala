package part2.lowlevel

import common.Person
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.File
import java.sql.Date
import java.time.{LocalDate, Period}
object L2_DStreamsTransformations {

  val spark = SparkSession
    .builder()
    .appName("DStreams Transformations")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  def readPeople() = ssc.socketTextStream("localhost", 12345)
    .map(_.split(":"))
    .map(arr => Person(arr(0).toInt, arr(1), arr(2), arr(3), arr(4), Date.valueOf(arr(5)), arr(6), arr(7).toInt))


  // EXAMPLES of map, filter, flatMap
  def peopleAges():DStream[(String, Int)] = {
    readPeople().map(person => {
      // Calculate the age of the person in years
      val age = Period.between(person.birthDate.toLocalDate, LocalDate.now()).getYears
      (s"${person.firstName} ${person.lastName}", age)
    })
  }
  // This is flatten in a DStream[String]
  def peopleSmallNames() = readPeople().flatMap(person =>List(person.firstName, person.middleName))
  // This is filtered in a DStream[Person]
  def highIncomePeople() = readPeople().filter(person => person.salary > 80000)
  // Reduce by key:
  // Reduce on a tuple, the first is the key, the second the value, it also operates by batches
  def countNameReduce():DStream[(String, Long)] = readPeople().map(_.firstName).map((_, 1L)).reduceByKey(_ + _)

    // Other transformations
  // returns the number of entries in each batch
  def countPeople():DStream[Long] = readPeople().count()
  // The result is a count for each distinct value in the batch
  def countNames():DStream[(String,Long)] = readPeople().map(_.firstName).countByValue()

  // foreach allows to process each DStream RDD independently in whatever style we need, for example by saving the content
  // to a json file
  def saveToJson() = readPeople().foreachRDD{ rdd =>
    val rootDir = "src/main/resources/data/people"
    val ds = spark.createDataset(rdd)
    val file = new File(rootDir)

    val nFile = file.listFiles().length
    val path = s"${rootDir}/people${nFile}.json"

    ds.write.json(path)
  }

  // We are going to import a file that contains 1 million entries of fictional people. We are going to read this file in
  // a streaming socket, we can do that with : cat src/main/resources/data/people-1m/people-1m.txt | nc -lk 12345
  def main(args: Array[String]): Unit = {
    // readPeople().print
    // peopleAges().print
    // peopleSmallNames().print
    // highIncomePeople().print
    // countNameReduce().print
    // countPeople().print
    // countNames().print
    saveToJson()
    ssc.start()
    ssc.awaitTermination()
  }
}
