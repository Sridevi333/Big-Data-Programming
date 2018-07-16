from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

customSchema = StructType([
    StructField("Year", IntegerType(), True),
    StructField("DateTime", TimestampType(), True),
    StructField("Stage", StringType(), True),
    StructField("Stadium", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Home Team Name", StringType(), True),
    StructField("Home Team Goals", IntegerType(), True),
    StructField("Away Team Goals", IntegerType(), True),
    StructField("Away Team Name", StringType(), True),
    StructField("Win conditions", StringType(), True),
    StructField("Attendance", IntegerType(), True),
    StructField("Half-time Home Goals", IntegerType(), True),
    StructField("Half-time Away Goals", IntegerType(), True),
    StructField("Referee", StringType(), True),
    StructField("Assistant 1", StringType(), True),
    StructField("Assistant 2", StringType(), True),
    StructField("RoundID", IntegerType(), True),
    StructField("MatchID", IntegerType(), True),
    StructField("Home Team Initials", StringType(), True),
    StructField("Away Team Initials", StringType(), True)])

#df = spark.read.format("csv").option("header","true").option("delimiter", "\t").option("timestampFormat", "yyyy/MM/dd HH:mm:ss").schema(customSchema).load("WorldCupMatches.csv")
df = spark.read.format("csv").option("header","true").load("WorldCupMatches.csv")
df.show()

# count of teams that reached different levels(stages) of the game
df.select(df['Home_Team_Name'], df['Stage']).groupBy("Stage").count().show()

# Filtering the Home teams that scored goals >=3 and <=10
df.createOrReplaceTempView("table1")
Goals = spark.sql("SELECT Stage,Stadium,City,Home_Team_Name FROM table1 WHERE Home_Team_Goals >= 3 AND Home_Team_Goals <= 10")
Goals.show()

# correlated subquery for finding attendance and max_attendance for a stadium
df.createOrReplaceTempView("tableA")
df.createOrReplaceTempView("tableB")
Max_Attendance = spark.sql("SELECT A.Stage,A.Stadium,A.City,A.Home_Team_Name,A.Away_Team_Name,A.Attendance,(SELECT MAX(Attendance)FROM tableB B where A.Stadium = B.Stadium) max_attendance FROM tableA A ORDER BY max_attendance asc")
Max_Attendance.show()

# Left outer join query
left_outer_join = spark.sql("SELECT A.Stadium,A.City,Home_Team_Name,A.Away_Team_Name,A.Attendance,B.max_attendance FRom tableA A LEFT OUTER JOIN (SELECT Stadium,MAX(Attendance) max_attendance FROM tableB B GROUP BY Stadium) B ON B.Stadium = A.Stadium ORDER BY max_attendance")
left_outer_join.show()

# Pattern recognization query for team that reached Finals
Pattern_reg = spark.sql("SELECT * from table1 WHERE Stage LIKE 'Final' AND Home_Team_Initials LIKE 'ENG'")
Pattern_reg.show()

# Average goals scored by a team
Avg_goals = spark.sql("SELECT Home_Team_Name AS Team, ROUND(AVG(Home_Team_Goals),0) AS average_goals FROM table1 GROUP BY Home_TEam_Name")
Avg_goals.show()

#Count of rows after performing UNION ALL operation
Union_all = spark.sql("SELECT COUNT(*) AS total_rows FROM (SELECT * FROM tableA UNION ALL SELECT * from tableB)sub")
Union_all.show()

join_all = spark.sql("SELECT * FROM tableA FULL JOIN tableB ON tableA.Home_Team_Goals = tableB.Away_Team_Goals")
join_all.show()

# number of times a country scores goals greater than or equal to 4
df.createOrReplaceTempView("table3")
query8 = spark.sql("SELECT Home_Team_Name As COUNTRY,COUNT(Home_Team_Goals) AS No_of_Times FROM table3 where Home_Team_Goals >=4 GROUP BY Home_Team_Name ORDER BY 2 DESC")
query8.show()

# number of maximum number of goals scored by a country between years 2000 nd 2010
query9 = spark.sql("SELECT Home_Team_Name, MAX(Home_Team_Goals) from table3 where Year BETWEEN 2000 AND 2010 GROUP BY Home_Team_Name ORDER BY 2 DESC")
query9.show()

# Total number of goals scored by a country
query10 = spark.sql("SELECT Home_Team_Name AS Country, SUM(Home_Team_Goals) from table3 GROUP BY Country ORDER BY 2 DESC")
query10.show()