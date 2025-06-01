import json
from kafka import KafkaProducer
import pandas as pd
import time

bootstrap_servers='35.188.57.91:9092'
topic_name='test'

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
   
)

df = pd.read_csv("RT_IOT2022.csv")
df = df.drop(columns=["Unnamed: 0"])
df = df.dropna()
df['proto'] = df['proto'].apply(lambda x: str(x))
df['service'] = df['service'].apply(lambda x: str(x))


for index, row in df.iterrows():
    to_json = row.to_dict()
    producer.send(topic_name, value=to_json)
    print(to_json)
    time.sleep(1)
    producer.flush()
    print('Message sent to Kafka')
