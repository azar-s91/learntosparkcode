import json
import requests
from kafka import KafkaProducer
from time import sleep

producer=KafkaProducer(bootstrap_servers=['localhost:9092'],
                      value_serializer=lambda x: json.dumps(x).encode('utf-8')
                      )


for i in range(50):
    res=requests.get('http://api.open-notify.org/iss-now.json')
    data=json.loads(res.content.decode('utf-8'))
    print(data)
    producer.send("testtopic",value=data)
    sleep(5)
    producer.flush()

