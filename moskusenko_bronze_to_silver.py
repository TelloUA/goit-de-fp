import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, regexp_replace, when

prefix = "moskusenko"

class BronzeToSilver:
    def __init__(self):
        self.spark = None
        self.bronze_path = f'data/{prefix}_bronze'
        self.silver_path = f'data/{prefix}_silver'

    def setup_spark_session(self):
        self.spark = SparkSession.builder \
            .appName("BronzeToSilver") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        print(f"Spark version: {self.spark.version}")
    
    def create_directories(self):
        os.makedirs(self.silver_path, exist_ok=True)
    
    def clean_text_column(self, df, column_name):
        return df.withColumn(column_name, 
            when(col(column_name).isNull(), None)
            .otherwise(
                trim(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(col(column_name), r'\s+', ' '),
                            r'[^\w\s\-\.\,\(\)]', ''
                        ),
                        r'^\s+|\s+$', ''
                    )
                )
            )
        )
    
    def clean_all_text_columns(self, df):
        text_columns = [field.name for field in df.schema.fields if field.dataType.typeName() == 'string']
        
        for column_name in text_columns:
            df = self.clean_text_column(df, column_name)
        
        return df
    
    def remove_duplicates(self, df):
        initial_count = df.count()
        deduplicated_df = df.dropDuplicates()
        final_count = deduplicated_df.count()
        
        removed_count = initial_count - final_count
        print(f"Removed {removed_count} duplicate rows ({removed_count/initial_count*100:.2f}%)")
        
        return deduplicated_df
    
    def process_table(self, table_name):
        bronze_table_path = os.path.join(self.bronze_path, table_name)
        
        df = self.spark.read.parquet(bronze_table_path)
        print(f"Read {table_name}: {df.count()} rows")
        
        cleaned_df = self.clean_all_text_columns(df)
        final_df = self.remove_duplicates(cleaned_df)
        
        silver_table_path = os.path.join(self.silver_path, table_name)
        final_df.write \
            .mode("overwrite") \
            .parquet(silver_table_path)
        
        print(f"Saved {table_name} to Silver layer: {final_df.count()} rows")
        final_df.show() 

        return silver_table_path
    
    def run(self):
        try:
            print("Starting Bronze to Silver process...")
            
            self.create_directories()
            self.setup_spark_session()
            
            tables = ['athlete_bio', 'athlete_event_results']
            silver_paths = {}
            
            for table_name in tables:
                silver_path = self.process_table(table_name)
                silver_paths[table_name] = silver_path
            
            print("\nBronze to Silver process completed!")
            return silver_paths
            
        except Exception as e:
            print(f"Pipeline failed: {str(e)}")
            raise
        finally:
            if self.spark:
                self.spark.stop()

def bronze_to_silver_task():
    pipeline = BronzeToSilver()
    return pipeline.run()

if __name__ == "__main__":
    pipeline = BronzeToSilver()
    pipeline.run()