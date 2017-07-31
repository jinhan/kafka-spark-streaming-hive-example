# kafka-spark-streaming-hive-example

Execution

```
./bin/spark-submit \
--packages org.apache.spark:spark-streaming-kafka_2.10:1.6.3 \
--jars /usr/hdp/current/spark-client/lib/datanucleus-api-jdo-3.2.6.jar, \
/usr/hdp/current/spark-client/lib/datanucleus-core-3.2.10.jar, \
/usr/hdp/current/spark-client/lib/datanucleus-rdbms-3.2.9.jar \
kafkaConsumer.py master:2181 test
```

Input Log Format

```
{"data": {"CH_22": 93, "CH_39": 5, "CH_48": 81, "CH_67": 33, "CH_84": 15, "CH_98": 87,
"CH_68": 72, "CH_70": 97, "CH_17": 62}, "time": 1491808854}
{"data": {"CH_8": 49, "CH_48": 66, "CH_90": 88, "CH_53": 82, "CH_50": 94, "CH_74": 66,
"CH_75": 31, "CH_69": 76, "CH_71": 45, "CH_14": 29}, "time": 1491808855}
```

Ambari Dashboard Hive View
![](https://user-images.githubusercontent.com/3071179/28797399-217585ee-767c-11e7-9217-99ec6f38d158.png)
```
SELECT key, SUM(value) FROM word_count WHERE time BETWEEN '2017-04-10 16:19:00' AND
 '2017-04-10 16:20:00' GROUP BY key;
```

Result
![](https://user-images.githubusercontent.com/3071179/28797400-21764c9a-767c-11e7-9eca-ca09fa5a4804.png)
