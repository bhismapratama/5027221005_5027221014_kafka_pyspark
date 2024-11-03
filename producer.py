from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sensor_ids = ['S1', 'S2', 'S3']

def send_sensor_data():
    while True:
        sensor_data = {
            'timestamp': datetime.now().isoformat(),
            'sensor_id': random.choice(sensor_ids),
            'temperature': round(random.uniform(60, 100), 2) 
        }
        producer.send('sensor-suhu', sensor_data)
        print(f"Sent: {sensor_data}")
        time.sleep(1) 

if __name__ == "__main__":
    send_sensor_data()
