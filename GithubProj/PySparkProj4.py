from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Proj4').getOrCreate()


team=input("Enter team name to get no of wins: ")

data=spark.read.csv("C:\\Users\\HP PC\\OneDrive\\Desktop\\DataProj\\DataAnalysisProjects\\5_CricketT20Analytics\\data_collection\\t20_csv_files\\dim_match_summary.csv",header=True)

cond=data.winner==team
query=data.filter(cond)
# query=data.filter(cond).count()
query.show()


