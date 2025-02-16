import pandas as pd
import json
import os
from spark_pipeline import SparkPipeline  # Import the SparkPipeline class

class DataTransformer:
    def __init__(self, csv_file_path, json_file_path):
        self.csv_file_path = csv_file_path
        self.json_file_path = json_file_path

    def csv_to_json(self):
        df = pd.read_csv(self.csv_file_path, header=1)  # Use the second row as the header
        json_str = df.to_json(orient='records', indent=4)
        with open(self.json_file_path, 'w') as json_file:
            json_file.write(json_str)
        print(f"Data from {self.csv_file_path} has been transformed and saved to {self.json_file_path}")

    def run_spark_pipeline(self):
        pipeline = SparkPipeline(self.json_file_path)
        processed_df = pipeline.process_data()
        processed_df.write.mode('overwrite').json(self.json_file_path.replace('.json', '_processed.json'))
        print(f"Processed data saved to {self.json_file_path.replace('.json', '_processed.json')}")

if __name__ == "__main__":
    output_dir = 'output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    transformer1 = DataTransformer('squad_standard_stats.csv', os.path.join(output_dir, 'squad_standard_stats.json'))
    transformer1.csv_to_json()
    transformer1.run_spark_pipeline()
    transformer2 = DataTransformer('player_standard_stats.csv', os.path.join(output_dir, 'player_standard_stats.json'))
    transformer2.csv_to_json()
    transformer2.run_spark_pipeline()
