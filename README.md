# kafka-spark-streaming-hive-example

```
./bin/spark-submit \
--packages org.apache.spark:spark-streaming-kafka_2.10:1.6.3 \
--jars /usr/hdp/current/spark-client/lib/datanucleus-api-jdo-3.2.6.jar, \
/usr/hdp/current/spark-client/lib/datanucleus-core-3.2.10.jar, \
/usr/hdp/current/spark-client/lib/datanucleus-rdbms-3.2.9.jar \
kafkaConsumer.py master:2181 test
```
