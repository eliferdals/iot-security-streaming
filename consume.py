import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'test', bootstrap_servers='35.188.57.91:9092',
    value_deserializer=lambda m: m.decode('utf-8'),group_id='consume-nifi'
    )

for msg in consumer:
    #print(msg.value)
    data = msg.value
    to_json = json.loads(data)