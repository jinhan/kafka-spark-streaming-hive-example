from __future__ import print_function

import sys
import os
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import HiveContext, Row

outputPath = '/tmp/checkpoint'

def getSqlContextInstance(sparkContext):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = HiveContext(sparkContext)
	return globals()['sqlContextSingletonInstance']

def createContext():
	sc = SparkContext(appName="PythonStreamingKafkaChCount")
	ssc = StreamingContext(sc, 5)
	zkQuorum, topic = sys.argv[1:]
	kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
	parsed = kvs.map(lambda k,v: json.loads(v))
	times_dstream = parsed.map(lambda x: x['time'])
	channel_dstream = parsed.map(lambda x: x['data'])

	count_values_windowed = channel_dstream.flatMap(lambda x: x.items())\
		.reduceByKeyAndWindow(lambda x,y: x+y, lambda x, y:x-y, 30,5)

	def writeRecord(time, rdd):
		try:
			hiveContext = getSqlContextInstance(rdd.context)
			rowRdd = rdd.map(lambda x: Row(time=time, key=x[0], value=x[1]))
			print(rowRdd.take(5))
			wordsDataFrame = hiveContext.createDataFrame(rowRdd)
			wordsDataFrame.show()
			wordsDataFrame.registerTempTable("wc")
			wordsDataFrame = hiveContext.sql("INSERT INTO TABLE word_count SELECT time, key, value from wc")
		except Exception as e:
			print(str(e))
			pass

	count_values_windowed.foreachRDD(writeRecord)
	return ssc

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Usage: [filename].py <zk> <topic>", file=sys.stderr)
		exit(-1)
	else:
		print("Creating new context")
		if os.path.exists(outputPath):
			os.remove(outputPath)

		ssc = StreamingContext.getOrCreate(outputPath, lambda: createContext())
		ssc.start()
		ssc.awaitTermination()
