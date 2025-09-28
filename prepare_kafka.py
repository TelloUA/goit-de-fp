from pyspark.sql import SparkSession
from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config, SPARK_JARS, prefix
from read import read_table_from_mysql
from pyspark.sql.functions import to_json, struct, col


class SetupKafkaStreaming:
    def __init__(self):
        self.spark = None

    def setup_spark_session(self):
        self.spark = SparkSession.builder \
            .config("spark.jars.packages", ",".join(SPARK_JARS)) \
            .appName("JDBCToKafka") \
            .getOrCreate()

    def create_kafka_topic(self, topic_name):
        admin_client = KafkaAdminClient(
            bootstrap_servers=kafka_config["bootstrap_servers"],
            client_id=f"{prefix}_olympic_pipeline",
            security_protocol=kafka_config['security_protocol'],
            sasl_mechanism=kafka_config['sasl_mechanism'],
            sasl_plain_username=kafka_config['username'],
            sasl_plain_password=kafka_config['password']
        )
            
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
            
        try:
            admin_client.create_topics(new_topics=[topic], validate_only=False)
            print(f"Topic '{topic_name}' created successfully.")
        except Exception as e:
            print(f"An error occurred: {e}")

    def get_data(self, limit=100):
        df = read_table_from_mysql(self.spark, "athlete_event_results", limit=limit)
        return df

    def send_to_kafka(self, df, topic_name):
        try:
            kafka_df = df.select(
                to_json(struct(*df.columns)).alias("value")
            )
            
            kafka_df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
                .option("topic", topic_name) \
                .option("kafka.security.protocol", kafka_config['security_protocol']) \
                .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
                .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
                .mode("append") \
                .save()
            
            print(f"✅ Successfully sent {kafka_df.count()} messages to Kafka")
            
        except Exception as e:
            print(f"❌ Error sending to Kafka: {str(e)}")
            raise

if __name__ == "__main__":

    # Step 3, prepare data of results to be in Kafka
    topic_name = f"{prefix}_athlete_event_results"
    kafka_setup = SetupKafkaStreaming()
    try:
        # kafka_setup.create_kafka_topic(topic_name)
        kafka_setup.setup_spark_session()
        df_results = kafka_setup.get_data()
        kafka_setup.send_to_kafka(df_results, topic_name)
    finally:
        if kafka_setup.spark:
            kafka_setup.spark.stop()