from pyspark.sql import SparkSession

class SparkPipeline:
    def __init__(self, json_file_path):
        self.json_file_path = json_file_path
        self.spark = SparkSession.builder \
            .appName("WebScrapingPipeline") \
            .getOrCreate()

    def process_data(self):
        df = self.spark.read.json(self.json_file_path)
        df.show()  # Display the DataFrame for verification

        # Perform any additional transformations or processing here
        # For example, filtering, aggregation, etc.

        return df

if __name__ == "__main__":
    pipeline = SparkPipeline('output/squad_standard_stats.json')
    processed_df = pipeline.process_data()
    processed_df.write.mode('overwrite').json('output/processed_squad_standard_stats.json')
