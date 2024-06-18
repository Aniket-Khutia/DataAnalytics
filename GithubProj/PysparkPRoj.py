# import pip
# pip.main(['install','pyspark'])
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("first_app") \
    .getOrCreate()

# Example usage
df = spark.read.csv("C:\\Users\\HP PC\\OneDrive\\Desktop\\DataProj\\DataAnalysisProjects\\5_CricketT20Analytics\\data_collection\\t20_csv_files\\dim_players.csv", header=True)
num_rows=df.count()
df.show(n=num_rows,truncate=False)