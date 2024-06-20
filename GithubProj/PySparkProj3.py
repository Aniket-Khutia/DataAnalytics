from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Proj3').getOrCreate()

df=spark.read.csv("C:\\Users\\HP PC\\OneDrive\\Desktop\\DataProj\\DataAnalysisProjects\\5_CricketT20Analytics\\data_collection\\t20_csv_files\\dim_players_processed.csv",header='True')
rows=df.count()
print('df row count= ',rows)
#df.show(n=rows)

tableIND=df['name','team','teamInit','Role'].filter(df.teamInit == 'IND')
tableBAN=df['name','team','teamInit','Role'].filter(df.teamInit == 'BAN')
tableBANIND=tableBAN.union(tableIND)
tableBANIND.sort('name').show()


#list of Bangladeshi players whose batting style is right hand bat and bowling style != NULL

#righthandbat=df.filter(df.battingStyle=='Right hand Bat')
#righthandbat=righthandbat.filter(righthandbat.bowlingStyle!='NULL').filter(righthandbat.teamInit=='BAN')
#righthandbat.show()







#dfnew=df.na.drop(how='any',subset=['team'])
# dfnew.show()
#print("dfnew row count= ",dfnew.count())