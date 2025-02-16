import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
import sys  # Added to handle command-line arguments
from generate_squadlist import generate_squadlist
from data_transformer import DataTransformer

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python main.py <URL>")
        sys.exit(1)

    team = sys.argv[1]
    team_name = team[37:-6]
    squad_stats_per_team = generate_squadlist(team)

    # Output the DataFrame to a CSV file
    output_file = f"{team_name}_squad_stats.csv"
    if os.path.exists(output_file):
        os.remove(output_file)
    squad_stats_per_team.to_csv(output_file, index=False)
    print(f"Data saved to {output_file}")

    transformer = DataTransformer(output_file, output_file.replace('.csv', '.json'))
    transformer.csv_to_json()
    transformer.run_spark_pipeline()