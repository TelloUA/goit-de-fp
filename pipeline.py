from pyspark.sql import SparkSession
from configs import jdbc_config, SPARK_JARS, kafka_config, prefix
from pyspark.sql.functions import to_json, from_json, struct, col, broadcast, avg, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType
from read import read_table_from_mysql

class OlympicDataPipeline:
    def __init__(self):
        self.spark = None

    def setup_spark_session(self):
        self.spark = SparkSession.builder \
            .config("spark.jars.packages", ",".join(SPARK_JARS)) \
            .appName("JDBCToKafka") \
            .getOrCreate()
    
    def read_table_from_mysql(self, table, limit=None):
        df = self.spark.read.format('jdbc').options(
            url=jdbc_config["url"],
            driver='com.mysql.cj.jdbc.Driver',  # com.mysql.jdbc.Driver
            dbtable=table,
            user=jdbc_config["user"],
            password=jdbc_config["password"]) \
            .load()

        if limit is not None:
            df = df.limit(limit)

        return df
    
    def filter_bio_data(self, bio_df):
        numeric_df = bio_df.withColumn("height", col("height").try_cast(DoubleType())) \
                      .withColumn("weight", col("weight").try_cast(DoubleType()))
    
        clean_df = numeric_df.filter(
            col("height").isNotNull() &
            col("weight").isNotNull() &
            (col("height") > 0) &
            (col("weight") > 0)
        )

        return clean_df

    def read_from_kafka_stream(self, topic_name):
        schema = StructType([
            StructField("edition", StringType(), True),
            StructField("edition_id", IntegerType(), True),
            StructField("country_noc", StringType(), True),
            StructField("sport", StringType(), True),
            StructField("event", StringType(), True),
            StructField("result_id", IntegerType(), True),
            StructField("athlete", StringType(), True),
            StructField("athlete_id", IntegerType(), True),
            StructField("pos", StringType(), True),
            StructField("medal", StringType(), True),
            StructField("isTeamSport", BooleanType(), True)
        ])
    
        kafka_stream = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
            .option("kafka.security.protocol", kafka_config['security_protocol']) \
            .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
            .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "earliest") \
            .load()

        parsed_df = kafka_stream.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
    
        return parsed_df
    
    def streaming_join(self, kafka_events_df, clean_bio_df):
        events_aliased = kafka_events_df.alias("events")
        bio_aliased = clean_bio_df.alias("bio")
    
        joined_df = events_aliased.join(
            broadcast(bio_aliased), 
            col("events.athlete_id") == col("bio.athlete_id")
        ).select(
            col("events.sport"),
            col("events.medal"), 
            col("events.country_noc"),
            col("bio.sex"),
            col("bio.height"),
            col("bio.weight")
        )
    
        # joined_df = kafka_events_df.join(broadcast(clean_bio_df), "athlete_id")
        return joined_df
    
    def aggregate_data(self, joined_df):
        aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
            .agg(avg("height").alias("avg_height"), avg("weight").alias("avg_weight"), current_timestamp().alias("calculation_timestamp"))
        return aggregated_df
    
    def write_batch_to_kafka(self, batch_df, output_topic):
        kafka_output_df = batch_df.select(
            to_json(struct(*batch_df.columns)).alias("value")
        )
    
        kafka_output_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
            .option("kafka.security.protocol", kafka_config['security_protocol']) \
            .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
            .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
            .option("topic", output_topic) \
            .mode("append") \
            .save()
        # print(f"Written {kafka_output_df.count()} records to Kafka topic: {output_topic}")

    def write_batch_to_database(self, batch_df, table_name):
        batch_df.write \
            .format("jdbc") \
            .option("url", jdbc_config["url"]) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", table_name) \
            .option("user", jdbc_config["user"]) \
            .option("password", jdbc_config["password"]) \
            .mode("append") \
            .save()
        # print(f"Written {batch_df.count()} records to database table: {table_name}")
    
    def process_batch(self, batch_df, batch_id):
        print(f"Processing batch {batch_id} with {batch_df.count()} records")
        try:
            self.write_batch_to_kafka(batch_df, f"{prefix}_athlete_results_agg")
            self.write_batch_to_database(batch_df, f"{prefix}_athlete_results_agg")
            print(f"Batch {batch_id} processed successfully")
        except Exception as e:
            print(f"Error processing batch {batch_id}: {str(e)}")
            raise

    def start_streaming_output(self, aggregated_df):
        query = aggregated_df.writeStream \
            .foreachBatch(self.process_batch) \
            .outputMode("complete") \
            .option("checkpointLocation", "/tmp/streaming_checkpoint") \
            .start()
    
        return query
    
    def run_pipeline(self):
        self.setup_spark_session()

        topic_name = f"{prefix}_athlete_event_results"

        # Step 1, read bio data
        df_bio = read_table_from_mysql(self.spark, "athlete_bio")
        # Step 2, clean bio data
        df_bio_clean = self.filter_bio_data(df_bio)

        # Step 3, read results from Kafka
        kafka_events_df = self.read_from_kafka_stream(topic_name)
        # Step 4, join by athlete_id
        joined_df = self.streaming_join(kafka_events_df, df_bio_clean)
        # Step 5, aggregate data
        aggregated_df = self.aggregate_data(joined_df)

        # Step 6, write to Kafka and DB
        query = self.start_streaming_output(aggregated_df)
    
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            print("Stopping streaming pipeline...")
            query.stop()

if __name__ == "__main__":
    pipeline = OlympicDataPipeline()
    try:
        pipeline.run_pipeline()
    except Exception as e:
        print(f"Error occurred: {str(e)}")
    finally:
        if pipeline.spark:
            pipeline.spark.stop()
