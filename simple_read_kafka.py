from kafka import KafkaConsumer
from configs import kafka_config, prefix
import json

consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='aggregation_reader'
)

topic_name = f'{prefix}_athlete_results_agg'
consumer.subscribe([topic_name])

print(f"Subscribed to topic: {topic_name}")

try:
    for message in consumer:
        data = message.value
        print(f"Partition {message.partition}, Offset {message.offset}")
        print(f"Sport: {data['sport']}")
        print(f"Medal: {data['medal']}")
        print(f"Sex: {data['sex']}")
        print(f"Country: {data['country_noc']}")
        print(f"Avg Height: {data['avg_height']}")
        print(f"Avg Weight: {data['avg_weight']}")
        print(f"Timestamp: {data['calculation_timestamp']}")
        print("-" * 50)
        
except KeyboardInterrupt:
    print("Stopped by user")
except Exception as e:
    print(f"Error: {e}")
finally:
    consumer.close()