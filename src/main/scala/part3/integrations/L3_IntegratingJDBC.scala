package part3.integrations

import common.{Car, carsSchema}
import org.apache.spark.sql.{Dataset, SparkSession}

object L3_IntegratingJDBC {
  // You can't not write/read from JDBC in an streaming fashion due to the transactional nature of the DBs, but you can do
  // it in batches
  val spark = SparkSession
    .builder()
    .appName("Integrating JDBC")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val pass = "docker"

  def writeStreamToPosgres() = {
    val carsDF = spark.readStream.schema(carsSchema).json("src/main/resources/data/cars") // This is our streamingDF
    var carsDS = carsDF.as[Car]

    // We need to operate the above DS in batches like this:
    carsDS.writeStream
      .foreachBatch((batch:Dataset[Car],id:Long) =>{
        // from this point each executor can control the batch
        // The batch at this point is a static DF because it is already known by the executor
        batch
          .write
          .format("jdbc")
          .option("driver", driver)
          .option("url", url)
          .option("user", user)
          .option("password", pass)
          .option("dbtable", "public.cars")
          .save()
      }).start().awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    // To run this use docker compose up and then connect to postgress using the psql.sh script at the root of the project
    writeStreamToPosgres()
  }
}
