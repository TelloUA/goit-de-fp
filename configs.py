import os

jdbc_config = {
    "url": "jdbc:mysql://217.61.57.46:3306/olympic_dataset",
    "table": "athlete_event_results",
    "user": "neo_data_admin",
    "password": "Proyahaxuqithab9oplp"
}

jar_path = os.path.abspath("mysql-connector-j-8.0.32.jar")

spark_config = {
    'jars_key': 'spark.jars',
    'jars_value': jar_path,
}

SPARK_JARS: list[str] = [
    "org.apache.spark:spark-streaming-kafka-0-10_2.13:4.0.1",
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1", 
    "com.mysql:mysql-connector-j:8.0.32",
]

kafka_config = {
    "bootstrap_servers": "77.81.230.104:9092",
    "username": 'admin',
    "password": 'VawEzo1ikLtrA8Ug8THa',
    "security_protocol": 'SASL_PLAINTEXT',
    "sasl_mechanism": 'PLAIN'
}

prefix = "moskusenko"