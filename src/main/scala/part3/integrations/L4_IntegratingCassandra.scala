package part3.integrations

import com.datastax.spark.connector.cql.CassandraConnector
import common.{Car, carsSchema}
import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.{Dataset, ForeachWriter, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object L4_IntegratingCassandra {
  val spark = SparkSession
    .builder()
    .appName("Integrating Cassandra")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  def writeStreamToCassandraInBatches() = {
    val carsDS = spark.readStream.schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    // Same as in Lesson 3, the batch dataset is a static dataset
    carsDS
      .writeStream
      .foreachBatch{(batch:Dataset[Car], _:Long) =>{
        // The following would save batches into cassandra in a single transaction
        batch.select(col("Name"), col("Horsepower"))
          .write
          // This is available through the cassandra wrapper provided by the cassandra connector
          .cassandraFormat("cars", "public")
          .mode(SaveMode.Append)
          .save()
        }
      }
      .start()
      .awaitTermination()
  }

  class CarCassandraForEachWritter extends ForeachWriter[Car] {

    val keyspace = "public"
    val table = "cars"
    // This is the cassandra connector that we will use
    val connector = CassandraConnector(spark.sparkContext.getConf)
    /*
      Spark will traverse the stream and:
        - for every batch `partitionId`
            - for every `epoch` (chunk of data)
                - call the open method, if false skip this chung
                - For each entry in this chunk call the process method (insert the data in this method)
                - call the close method either at the end of he chunk or with an error if it was thrown
     */
    override def open(partitionId: Long, epochId: Long): Boolean = {
      println("Opened connection")
      true
    }

    override def process(car: Car): Unit = {
      // This is cassandra specific boiler plate
      connector.withSessionDo{ session =>
        session.execute(
          s"""
            |insert into ${keyspace}.${table}("Name","Horsepower") values('${car.Name}',${car.Horsepower.orNull});
            |""".stripMargin
        )

      }
    }

    override def close(errorOrNull: Throwable): Unit = println("clossing connection")
  }

  def writeStreamToCassandra() = {
    val carsDS = spark.readStream.schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    // Same as in Lesson 3, the batch dataset is a static dataset
    carsDS
      .writeStream
      // This takes a ForeachWriter which will be responsible of writting each of the cars contained in the DS in a much
      // more optimized fashion
      .foreach(new CarCassandraForEachWritter)
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    // Set up a keyspace in cassandra. Use the ./cql.sh script to connect to cassandra and then execute:
    // cqlsh> create keyspace public with replication = {'class':'SimpleStrategy', 'replication_factor':1};
    // Then create a table:
    // cqlsh> create table public.cars("Name" text primary key, "Horsepower" int);
    //writeStreamToCassandraInBatches()
    // Check the table content with:
    // cqlsh> select * from public.cars;

    // Now delete the data for the second test using
    // cqlsh> truncate public.cars;
    writeStreamToCassandra()
  }
}
