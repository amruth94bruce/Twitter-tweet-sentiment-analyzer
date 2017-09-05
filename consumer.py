#    Spark
from pyspark import SparkContext  
#    Spark Streaming
from pyspark.streaming import StreamingContext  
#    Kafka
from pyspark.streaming.kafka import KafkaUtils  
#    json parsing
import json  
import unicodedata
import time
from textblob import TextBlob
from elasticsearch import Elasticsearch
from elasticsearch import helpers
sc = SparkContext() 
sc.setLogLevel("WARN")  
ssc = StreamingContext(sc, 4)  
es = Elasticsearch()


#sentiment analyzer		
def getSentiment( str ):
	if not str:
	   str="Its raining"
	sentence = TextBlob(str)
	if float(sentence.sentiment.polarity) > 0.05:
			return "positive"
	elif float(sentence.sentiment.polarity) < -0.05:	
	        return "negative"
	else:
            return "neutral"		
		
kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'tw':1})  
lines = kafkaStream.map(lambda x:  x[1])

#creating JSON string
lines1=lines.map(lambda x:x.split("::")).map(lambda x: "{\"result\":\""+getSentiment(  str( x[0]))+"\","+" \"person\":\""+ x[1]+"\","+ "\"location\":\""+ x[2]+"\","+"\"timestamp\":\""+x[3]+"\","+"\"geo\":\""+x[4]+"\"}")

#writing RDD to Elasticsearch
mapped = lines1.map(lambda x: (None, x))
conf = {"es.resource": "twitter/mem", "es.input.json": "true"}
mapped.foreachRDD(lambda rdd: rdd.saveAsNewAPIHadoopFile(
        path="-",
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=conf))

ssc.start()  
ssc.awaitTermination() 		