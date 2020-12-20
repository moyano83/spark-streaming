package part2.lowlevel

import common.Stock
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.{File, FileWriter}
import java.sql.Date
import java.text.SimpleDateFormat

object L1_DStreams {

  val spark = SparkSession.builder().appName("DStreams").master("local[2]").getOrCreate()

  // Spark streaming context is the entry point to the DStreams API
  // we need to pass a spark context and an interval
  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))


  /* Basics of DStreams:
    - Define input sources by creating DStreams
    - Define transformations for the DStreams
    - Call an action on the DStream
    - Trigger to start the computation with ssc.start
    - After point 3, no more computations can be added
    - Then Stop the computation or await for computation
    - You cannot restart a computation
   */

  def readFromSocket() = {
    val socketStream = ssc.socketTextStream("localhost", 12345)
    // This is an example of a transformation on a DStream
    val wordStream = socketStream.flatMap(_.split(" "))
    //action
    // wordStream.print()

    // Instead of printing we can also save it to a file, each folder created is going to be an RDD and each file inside
    // the folder is a new partition of the RDD
    wordStream.saveAsTextFiles("src/main/resources/data/words")

    ssc.start()
    ssc.awaitTermination()
  }

  val stocksPath = "src/main/resources/data/stocks"

  def createNewFile() = new Thread(() =>{
    Thread.sleep(4000)
    val dir = new File(stocksPath)
    val nFiles = dir.listFiles().length
    val newFile = new File(s"${stocksPath}/newStock${nFiles}.csv")
    newFile.createNewFile()
    val writer = new FileWriter(newFile)
    writer.write(
      """
        |AAPL,May 1 2001,9.98
        |AAPL,Jun 1 2001,11.62
        |AAPL,Jul 1 2001,9.4
        |AAPL,Aug 1 2001,9.27
        |AAPL,Sep 1 2001,7.76
        |AAPL,Oct 1 2001,8.78
        |AAPL,Nov 1 2001,10.65
        |""".stripMargin.trim)
    writer.flush()
    writer.close()
  }).start()

  def readFromFile() = {
    createNewFile()
    val textStream: DStream[String] = ssc.textFileStream(stocksPath)
    val formatter = new SimpleDateFormat("MMM d yyyy")
    val stocks = textStream
      .map(_.split(","))
      .map(arr=> Stock(arr(0), new Date(formatter.parse(arr(1)).getTime), arr(2).toDouble))
    stocks.print()

    ssc.start()
    ssc.awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    //readFromSocket()
    // If we start the below method, nothing will be read, because ssc.textFileStream only monitors the directory for new
    // files, we need to call the createNewFile method to add files
    readFromFile()
  }
}
