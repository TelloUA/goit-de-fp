import os
import requests
from pyspark.sql import SparkSession

prefix = "moskusenko"

class LandingToBronze:
    def __init__(self):
        self.spark = None
        self.ftp_urls = {
            'athlete_bio': 'https://ftp.goit.study/neoversity/athlete_bio.csv',
            'athlete_event_results': 'https://ftp.goit.study/neoversity/athlete_event_results.csv'
        }
        self.landing_path = f'data/{prefix}_landing'
        self.bronze_path = f'data/{prefix}_bronze'

    def setup_spark_session(self):
        self.spark = SparkSession.builder \
            .appName("LandingToBronze") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
    
    def create_directories(self):
        os.makedirs(self.landing_path, exist_ok=True)
        os.makedirs(self.bronze_path, exist_ok=True)
        print(f"Created directories: {self.landing_path}, {self.bronze_path}")
    
    def download_file_from_ftp(self, url, filename):
        try:
            response = requests.get(url)
            response.raise_for_status()
            
            file_path = os.path.join(self.landing_path, filename)
            with open(file_path, 'wb') as file:
                file.write(response.content)
            
            print(f"Downloaded {filename} to {file_path}")
            return file_path
            
        except requests.exceptions.RequestException as e:
            print(f"Error downloading {filename}: {str(e)}")
            raise
    
    def download_all_files(self):
        downloaded_files = {}
        
        for table_name, url in self.ftp_urls.items():
            filename = f"{table_name}.csv"
            file_path = self.download_file_from_ftp(url, filename)
            downloaded_files[table_name] = file_path
        
        return downloaded_files
    
    def csv_to_parquet(self, csv_path, table_name):
        try:
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(csv_path)
            
            print(f"Read {table_name}: {df.count()} rows, {len(df.columns)} columns")
            
            parquet_path = os.path.join(self.bronze_path, table_name)
            df.write \
                .mode("overwrite") \
                .parquet(parquet_path)
            
            print(f"Saved {table_name} to Bronze layer: {parquet_path}")
            
            print(f"\n{table_name} sample data:")
            df.show()
            
            return parquet_path
            
        except Exception as e:
            print(f"Error converting {table_name} to Parquet: {str(e)}")
            raise
    
    def process_all_tables(self):
        """Process all tables from landing to bronze"""
        downloaded_files = self.download_all_files()
        
        bronze_paths = {}
        for table_name, csv_path in downloaded_files.items():
            parquet_path = self.csv_to_parquet(csv_path, table_name)
            bronze_paths[table_name] = parquet_path
        
        return bronze_paths
    
    def run(self):
        try:
            print("Starting Landing to Bronze process...")
            
            self.create_directories()
            self.setup_spark_session()
            
            bronze_paths = self.process_all_tables()
            
            print("\nLanding to Bronze process completed successfully!")
            print("Bronze layer files created:")
            for table_name, path in bronze_paths.items():
                print(f"  {table_name}: {path}")
                
        except Exception as e:
            print(f"Pipeline failed: {str(e)}")
            raise
        finally:
            if self.spark:
                self.spark.stop()
                print("Spark session stopped")

def landing_to_bronze_task():
    pipeline = LandingToBronze()
    return pipeline.run()

if __name__ == "__main__":
    pipeline = LandingToBronze()
    pipeline.run()