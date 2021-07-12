//import required libraries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._


object sparkStreaming {
  def main(args: Array[String]): Unit = {

    // Create Spark Session
    val spark=SparkSession.builder()
                          .master("local[*]")
                          .appName("spark Streaming")
                          .getOrCreate()

    // Set Spark logging level to ERROR
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions",3)

    //Define Schema
    val schema = new StructType()
      .add("stockID", "string")
      .add("stockName", "string")
      .add("stockPrice", "double")
      .add("stockQty","integer")
      .add("sDate", "date")

    // Create Streaming DataFrame by reading data from socket.
    val df_readStream = spark
      .readStream
      .option("header", true)
      .schema(schema)
      .option("maxFilesPerTrigger", 1)
      .csv("input/streamData")
      .withColumn("FName", element_at(split(input_file_name(),"/"),-1))
      .withColumn("timestamp",current_timestamp())

    //Do some transformations on ReadStream
    val trans_readStream=df_readStream
                          .groupBy("FName","sDate","timestamp")
                          .agg(sum(col("stockPrice")*col("stockQty")))

    //Display out in console
    trans_readStream.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate","False")
      .option("checkpointLocation", "checkpoint")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
      .awaitTermination()

    //Stop sparkSession
    spark.stop()
  }
}