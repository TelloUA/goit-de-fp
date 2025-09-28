import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp, when, regexp_extract
from pyspark.sql.types import DoubleType

prefix = "moskusenko"

class SilverToGold:
    def __init__(self):
        self.spark = None
        self.silver_path = f'data/{prefix}_silver'
        self.gold_path = f'data/{prefix}_gold'
    
    def setup_spark_session(self):
        self.spark = SparkSession.builder \
            .appName("SilverToGold") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
    
    def create_directories(self):
        os.makedirs(self.gold_path, exist_ok=True)
    
    def read_silver_tables(self):
        athlete_bio_path = os.path.join(self.silver_path, 'athlete_bio')
        athlete_events_path = os.path.join(self.silver_path, 'athlete_event_results')
        
        bio_df = self.spark.read.parquet(athlete_bio_path)
        events_df = self.spark.read.parquet(athlete_events_path)
        
        print(f"Read athlete_bio: {bio_df.count()} rows")
        print(f"Read athlete_event_results: {events_df.count()} rows")
        
        return bio_df, events_df
    
    def clean_bio_data(self, bio_df):
        cleaned_df = bio_df.withColumn("height", 
            when(col("height").rlike(r'^\d+'), 
                 regexp_extract(col("height"), r'^(\d+)', 1).cast(DoubleType()))
            .otherwise(None)
        ).withColumn("weight",
            when(col("weight").rlike(r'^\d+'), 
                 regexp_extract(col("weight"), r'^(\d+)', 1).cast(DoubleType()))
            .otherwise(None)
        )
    
        return cleaned_df.filter(
            col("height").isNotNull() & col("weight").isNotNull() &
            (col("height") > 0) & (col("weight") > 0)
        )
    
    def join_and_aggregate(self, bio_df, events_df):
        events_aliased = events_df.alias("events")
        bio_aliased = bio_df.alias("bio")
        
        joined_df = events_aliased.join(bio_aliased, 
                                       col("events.athlete_id") == col("bio.athlete_id"), 
                                       "inner")
        
        avg_stats = joined_df.groupBy(
            col("events.sport"), 
            col("events.medal"), 
            col("bio.sex"), 
            col("events.country_noc")
        ).agg(
            avg("bio.height").alias("avg_height"),
            avg("bio.weight").alias("avg_weight"),
            current_timestamp().alias("timestamp")
        )

        print(f"Created avg_stats: {avg_stats.count()} rows")
        return avg_stats
    
    def save_gold_table(self, avg_stats):
        gold_path = os.path.join(self.gold_path, 'avg_stats')
        
        avg_stats.write \
            .mode("overwrite") \
            .parquet(gold_path)
        
        avg_stats.show()
        
        print(f"Saved avg_stats to: {gold_path}")
    
    def run(self):
        try:
            print("Starting Silver to Gold process...")
            
            self.create_directories()
            self.setup_spark_session()
            
            bio_df, events_df = self.read_silver_tables()
            clean_bio_df = self.clean_bio_data(bio_df)
            avg_stats = self.join_and_aggregate(clean_bio_df, events_df)
            self.save_gold_table(avg_stats)
            
            print("Silver to Gold process completed!")
            
        except Exception as e:
            print(f"Pipeline failed: {str(e)}")
            raise
        finally:
            if self.spark:
                self.spark.stop()

def silver_to_gold_task():
    pipeline = SilverToGold()
    pipeline.run()

if __name__ == "__main__":
    pipeline = SilverToGold()
    pipeline.run()