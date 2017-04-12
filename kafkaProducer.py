import sys
import json
import time
import random
import datetime

from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='master:6667')
kafkaTopicName = 'test'

if len(sys.argv) != 5:
        print "Usage: [filename].py chSize, chCountMax, msgCount, msgRate(kbps)"
        exit(-1)

msgRate = int(sys.argv[4])
byteSent = 0
startTime = datetime.datetime.now()

for _ in range(int(sys.argv[3])):
        chDict = {}
        chDict['time'] = int(time.time())
        chTemp = {}
        for i in range(10):
                chName = "CH_" + str(random.randint(1, int(sys.argv[1])))
                chVal = random.randint(0, int(sys.argv[2]))
                chTemp[chName] = chVal
        chDict['data'] = chTemp
        result = json.dumps(chDict)
        print(result)
        producer.send(kafkaTopicName, result)
        byteSent += len(result)
        expectedTime = (byteSent * 8.0) / (msgRate * 1024)
        elapsedTime = datetime.datetime.now() - startTime
        while elapsedTime.total_seconds() < expectedTime:
                time.sleep(0.001)
                elapsedTime = datetime.datetime.now() - startTime

endTime = datetime.datetime.now()
totalTime = endTime - startTime
print "Run " + str(totalTime.total_seconds()) + "sec"

