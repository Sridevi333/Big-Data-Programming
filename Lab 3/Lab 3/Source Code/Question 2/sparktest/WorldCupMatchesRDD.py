from pyspark.sql.types import *
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sc = spark.sparkContext

# Load a text file and convert each line to a Row.
lines = sc.textFile("WorldCupMatches.txt")
parts = lines.map(lambda l: l.split(","))

# Each line is converted to a tuple.
matches = parts.map(lambda p: (p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15], p[16], p[17], p[18], p[19] .strip()))

# The schema is encoded in a string.
schemaString = "Year Datetime Stage Stadium City Home_Team_Name Home_Team_Goals Away_Team_Goals Away_Team_Name Win_conditions Attendance Half-time_Home_Goals Half-time_Away_Goals Referee Assistant_1 Assistant_2 RoundID MatchID Home_Team_Initials Away_Team_Initials"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

# Apply the schema to the RDD
schemaMatches = spark.createDataFrame(matches, schema)

# Creates a temporary view using the DataFrame
schemaMatches.createOrReplaceTempView("matches")

# SQL can be run over DataFrames that have been registered as a table.
results = spark.sql("SELECT * FROM matches")

results.show()

# Filtering the Home teams that scored goals >=3 and <=10
query1 = spark.sql("SELECT Stage,Stadium,City,Home_Team_Name FROM matches WHERE Home_Team_Goals >= 3 AND Home_Team_Goals <= 10")
query1.show()


# correlated subquery for finding attendance and max_attendance for a stadium
schemaMatches.createOrReplaceTempView("tableA")
schemaMatches.createOrReplaceTempView("tableB")
query2 = spark.sql("SELECT A.Stage,A.Stadium,A.City,A.Home_Team_Name,A.Away_Team_Name,A.Attendance,(SELECT MAX(Attendance)FROM tableB B where A.Stadium = B.Stadium) max_attendance FROM tableA A ORDER BY max_attendance asc")
query2.show()


# Pattern recognization query for team that reached Finals
query3 = spark.sql("SELECT * from matches WHERE Stage LIKE 'Final' AND Home_Team_Initials LIKE 'ENG'")
query3.show()

# Average goals scored by a team
query4 = spark.sql("SELECT Home_Team_Name AS Team, ROUND(AVG(Home_Team_Goals),0) AS average_goals FROM matches GROUP BY Home_Team_Name")
query4.show()

# number of times a country scores goals greater than or equal to 4
query5 = spark.sql("SELECT Home_Team_Name As COUNTRY,COUNT(Home_Team_Goals) AS No_of_Times FROM matches where Home_Team_Goals >=4 GROUP BY Home_Team_Name ORDER BY 2 DESC")
query5.show()
