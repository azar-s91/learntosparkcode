import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.Json

import scala.util.parsing.json.JSON

object kafkaSparkDemo {
  def main(args: Array[String]): Unit = {

    val broker_id="localhost:9092"
    val groupid="GRP1"
    val topics="testtopic"

    val topicset=topics.split(",").toSet
    val kafkaParams=Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_id,
      ConsumerConfig.GROUP_ID_CONFIG -> groupid,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val spark=SparkSession.builder().appName("Kafka Demo").master("local").getOrCreate()
    val sc=spark.sparkContext
    val ssc=new StreamingContext(sc,Seconds(5))
    sc.setLogLevel("OFF")

    val message=KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topicset,kafkaParams)
    )

    //val words=message.map(_.value()).flatMap(_.split(" "))
    //val countwords=words.map(x=>(x,1)).reduceByKey(_+_)
    //countwords.print()

    def functordd(value: RDD[String]): Unit ={

      val rowval=value.map(x=>JSON.parseFull(x))
      val df=spark.read.json(value)
      df.show()
    }

    message.map(_.value()).print()
    message.map(_.value()).foreachRDD(x => functordd(x))

    ssc.start()
    ssc.awaitTermination()

  }

}
