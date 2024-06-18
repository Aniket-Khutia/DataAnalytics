from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper
import pandas as pd
spark=SparkSession.builder.appName('Proj2').getOrCreate()


#df=spark.read.csv("C:\\Users\\HP PC\\OneDrive\\Desktop\\DataProj\\DataAnalysisProjects\\5_CricketT20Analytics\\data_collection\\t20_csv_files\\dim_players.csv",header=True,inferSchema=True)

df=spark.read.csv("C:\\Users\\HP PC\\OneDrive\\Desktop\\DataProj\\DataAnalysisProjects\\5_CricketT20Analytics\\data_collection\\t20_csv_files\\dim_players.csv",header=True,inferSchema=True)
count_rows=df.count()
# df.printSchema()
#df.show(n=count_rows,truncate=False)
# Renaming the playingRole column to Role
dfnew=df.withColumnRenamed('playingRole','Role')

# Adding a new column to the df where the values are derived from the first 3 letters of the team name
# and changing it to uppercase
dfnew=dfnew.withColumn('teamInit',dfnew['team'][0:3])
dfnew=dfnew.withColumn('teamInit',upper(col('teamInit')))
csv_output_path = "C:\\Users\\HP PC\\OneDrive\\Desktop\\DataProj\\DataAnalysisProjects\\5_CricketT20Analytics\\data_collection\\t20_csv_files\\dim_players_processed.csv"


dfnew.show(truncate=False)
# dfpandas=dfnew.toPandas()
# print(dfpandas.to_string(index=False))


spark.stop()