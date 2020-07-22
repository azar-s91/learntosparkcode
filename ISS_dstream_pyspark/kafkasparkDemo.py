import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import time

if __name__=="__main__":
    
    #sc=SparkContext(appName="Kafka Spark Demo")
    
    spark=SparkSession.builder.master("local").appName("Kafka Spark Demo").getOrCreate()
    
    sc=spark.sparkContext
    
    ssc=StreamingContext(sc,20)
    
    message=KafkaUtils.createDirectStream(ssc,topics=['testtopic'],kafkaParams= {"metadata.broker.list": "localhost:9092"})
    
    #words=message.map(lambda x: x[1]).flatMap(lambda x: x.split(" "))
    #wordcount=words.map(lambda x: (x,1)).reduceByKey(lambda a,b: a+b)
    #wordcount.pprint()
    
    
    data=message.map(lambda x: x[1])
    
    
    def functordd(rdd):
        try:
            rdd1=rdd.map(lambda x: json.loads(x))
            df=spark.read.json(rdd1)
            df.show()
            df.createOrReplaceTempView("Test")
            df1=spark.sql("select iss_position.latitude,iss_position.longitude,message,timestamp from Test")
            df1.write.format('csv').mode('append').save("testing")
            
        except:
            pass
    
   
    data.foreachRDD(functordd)
    
    ssc.start()
    ssc.awaitTermination()