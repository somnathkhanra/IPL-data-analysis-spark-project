# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DateType, DecimalType
from pyspark.sql.functions import col, when, sum, avg, row_number
from pyspark.sql.window import Window


# COMMAND ----------


storageAccountName = "ipldatastoragesom"
storageAccountAccessKey = "V/Xw2wHLMIODWDF7f5LSPJYZk3ZSzQj91aHb6M9txyI1L+fyAdy2OXLuQ6EbQQl0phSWv9gDcK0P+ASt3jOW8w=="
sasToken = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-08-14T00:36:10Z&st=2024-08-13T16:36:10Z&spr=https&sig=pofI2yCb7ZDMOt9rOPaj%2BXU%2Fus83W9KFFkPGCVjjsYk%3D"
blobContainerName = "ipl-data"
mountPoint = "/mnt/ipl/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
  source = "wasbs://ipl-data@ipldatastoragesom.blob.core.windows.net/",
  mount_point = "/mnt/ipl",
  extra_configs = {"fs.azure.account.key.ipldatastoragesom.blob.core.windows.net": "V/Xw2wHLMIODWDF7f5LSPJYZk3ZSzQj91aHb6M9txyI1L+fyAdy2OXLuQ6EbQQl0phSWv9gDcK0P+ASt3jOW8w=="})
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)

# COMMAND ----------


display(dbutils.fs.ls("/mnt/ipl/"))
display(dbutils.fs.ls("dbfs:/mnt/ipl/raw_data/"))

# COMMAND ----------


spark

# COMMAND ----------


spark = SparkSession.builder.appName("IPL Data Ingestion and Analysis using Spark").getOrCreate()
spark

# COMMAND ----------


ball_by_ball_schema = StructType([ StructField("match_id", IntegerType(), nullable=True), StructField("over_id", IntegerType(), nullable=True), StructField("ball_id", IntegerType(), nullable=True), StructField("innings_no", IntegerType(), nullable=True), StructField("team_batting", StringType(), nullable=True), StructField("team_bowling", StringType(), nullable=True), StructField("striker_batting_position", IntegerType(), nullable=True), StructField("extra_type", StringType(), nullable=True), StructField("runs_scored", IntegerType(), nullable=True), StructField("extra_runs", IntegerType(), nullable=True), StructField("wides", IntegerType(), nullable=True), StructField("legbyes", IntegerType(), nullable=True), StructField("byes", IntegerType(), nullable=True), StructField("noballs", IntegerType(), nullable=True), StructField("penalty", IntegerType(), nullable=True), StructField("bowler_extras", IntegerType(), nullable=True), StructField("out_type", StringType(), nullable=True), StructField("caught", BooleanType(), nullable=True), StructField("bowled", BooleanType(), nullable=True), StructField("run_out", BooleanType(), nullable=True), StructField("lbw", BooleanType(), nullable=True), StructField("retired_hurt", BooleanType(), nullable=True), StructField("stumped", BooleanType(), nullable=True), StructField("caught_and_bowled", BooleanType(), nullable=True), StructField("hit_wicket", BooleanType(), nullable=True), StructField("obstructingfeild", BooleanType(), nullable=True), StructField("bowler_wicket", BooleanType(), nullable=True), StructField("match_date", DateType(), nullable=True), StructField("season", IntegerType(), nullable=True), StructField("striker", IntegerType(), nullable=True), StructField("non_striker", IntegerType(), nullable=True), StructField("bowler", IntegerType(), nullable=True), StructField("player_out", IntegerType(), nullable=True), StructField("fielders", IntegerType(), nullable=True), StructField("striker_match_sk", IntegerType(), nullable=True), StructField("strikersk", IntegerType(), nullable=True), StructField("nonstriker_match_sk", IntegerType(), nullable=True), StructField("nonstriker_sk", IntegerType(), nullable=True), StructField("fielder_match_sk", IntegerType(), nullable=True), StructField("fielder_sk", IntegerType(), nullable=True), StructField("bowler_match_sk", IntegerType(), nullable=True), StructField("bowler_sk", IntegerType(), nullable=True), StructField("playerout_match_sk", IntegerType(), nullable=True), StructField("battingteam_sk", IntegerType(), nullable=True), StructField("bowlingteam_sk", IntegerType(), nullable=True), StructField("keeper_catch", BooleanType(), nullable=True), StructField("player_out_sk", IntegerType(), nullable=True), StructField("matchdatesk", DateType(), nullable=True) ])

# COMMAND ----------


ball_by_ball_df = spark.read.schema(ball_by_ball_schema).format("csv").option("header", "true").load("dbfs:/mnt/ipl/raw_data/ball_by_ball.csv")

#display(ball_by_ball_df)

# COMMAND ----------

# Filter to include only valid deliveries (excluding extras like wides and no balls for specific analyses)
ball_by_ball_df = ball_by_ball_df.filter((col("wides") == 0) & (col("noballs") == 0) )

# Aggregation: Calculate the total and average runs scored in each match and inning
total_and_avg_runs = ball_by_ball_df.groupBy("match_id", "innings_no").agg(
    sum("runs_scored").alias("total_runs"),
    avg("runs_scored").alias("average_runs")
)


# COMMAND ----------

# Window Function: Calculate running total of runs in each match for each over

WindowSpec = Window.partitionBy("match_id", "innings_no").orderBy("over_id")

ball_by_ball_df = ball_by_ball_df.withColumn("running_total_runs", sum("runs_scored").over(WindowSpec))


# COMMAND ----------

# Conditional Column: Flag for high impact balls (either a wicket or more than 6 runs including extras)
ball_by_ball_df = ball_by_ball_df.withColumn(
    "high_impact", when((col("runs_scored") + col("extra_runs") > 6) | (col("bowler_wicket") == True), True).otherwise(False)
)
#ball_by_ball_df.show(2)
ball_by_ball_df.printSchema()

# COMMAND ----------


match_schema = StructType([
    StructField("match_sk", IntegerType(), nullable=True),
    StructField("match_id", IntegerType(), nullable=True),
    StructField("team1", StringType(), nullable=True),
    StructField("team2", StringType(), nullable=True),
    StructField("match_date", DateType(), nullable=True),
    StructField("season_year", IntegerType(), nullable=True),
    StructField("venue_name", StringType(), nullable=True),
    StructField("city_name", StringType(), nullable=True),
    StructField("country_name", StringType(), nullable=True),
    StructField("toss_winner", StringType(), nullable=True),
    StructField("match_winner", StringType(), nullable=True),
    StructField("toss_name", StringType(), nullable=True),
    StructField("win_type", StringType(), nullable=True),
    StructField("outcome_type", StringType(), nullable=True),
    StructField("manofmatch", StringType(), nullable=True),
    StructField("win_margin", IntegerType(), nullable=True),
    StructField("country_id", IntegerType(), nullable=True)
])

match_df = spark.read.schema(match_schema).format("csv").option("header", "true").load("dbfs:/mnt/ipl/raw_data/match.csv")
#display(match_df)

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofmonth, when

# Extracting year, month, and day from the match date for more detailed time-based analysis
match_df = match_df.withColumn("year", year("match_date"))
match_df = match_df.withColumn("month", month("match_date"))
match_df = match_df.withColumn("dayofmonth", dayofmonth("match_date"))

# High margin win: categorizing win margins into 'high', 'medium', and 'low'
match_df = match_df.withColumn(
    "win_margin_category",
    when(col("win_margin") >= 100, "high")
    .when((col("win_margin") >=  50) & (col("win_margin") < 100), "medium")
    .otherwise("low")
)

# Analyze the impact of the toss: who wins the toss and the match
match_df = match_df.withColumn(
    "toss_match_winner",
    when(col("toss_winner") == col("match_winner"), "Yes").otherwise("No")
)

# Show the enhanced match DataFrame
match_df.show(2)

# COMMAND ----------


player_schema = StructType([
    StructField("player_sk", IntegerType(), nullable=True),
    StructField("player_id", IntegerType(), nullable=True),
    StructField("player_name", StringType(), nullable=True),
    StructField("dob", DateType(), nullable=True),
    StructField("batting_hand", StringType(), nullable=True),
    StructField("bowling_skill", StringType(), nullable=True),
    StructField("country_name", StringType(), nullable=True)
])

player_df = spark.read.schema(player_schema).format("csv").option("header", "true").load("dbfs:/mnt/ipl/raw_data/player.csv")

player_df.show(2)

# COMMAND ----------

from pyspark.sql.functions import lower, regexp_replace


# Normalize and clean player names
player_df = player_df.withColumn("player_name", lower(regexp_replace("player_name", "[^a-zA-Z0-9 ]", "")))
#player_df.show(10)

# Handle missing values in 'batting_hand' and 'bowling_skill' with a default 'unknown'
player_df = player_df.na.fill({"batting_hand": "unknown", "bowling_skill": "unknown"})

player_df.show(10)

# Categorizing players based on batting hand
player_df = player_df.withColumn(
    "batting_style",
    when(col("batting_hand").contains("Left"), "Left-Handed").otherwise("Right-Handed")
)
#player_df.show(100)

# COMMAND ----------

player_match_schema = StructType([
    StructField("player_match_sk", IntegerType(), nullable=True),
    StructField("playermatch_key", DecimalType(10, 2), nullable=True),
    StructField("match_id", IntegerType(), nullable=True),
    StructField("player_id", IntegerType(), nullable=True),
    StructField("player_name", StringType(), nullable=True),
    StructField("dob", DateType(), nullable=True),
    StructField("batting_hand", StringType(), nullable=True),
    StructField("bowling_skill", StringType(), nullable=True),
    StructField("country_name", StringType(), nullable=True),
    StructField("role_desc", StringType(), nullable=True),
    StructField("player_team", StringType(), nullable=True),
    StructField("opposit_team", StringType(), nullable=True),
    StructField("season_year", IntegerType(), nullable=True),
    StructField("is_manofthematch", BooleanType(), nullable=True),
    StructField("age_as_on_match", IntegerType(), nullable=True),
    StructField("isplayers_team_won", BooleanType(), nullable=True),
    StructField("batting_status", StringType(), nullable=True),
    StructField("bowling_status", StringType(), nullable=True),
    StructField("player_captain", StringType(), nullable=True),
    StructField("opposit_captain", StringType(), nullable=True),
    StructField("player_keeper", StringType(), nullable=True),
    StructField("opposit_keeper", StringType(), nullable=True)
])

player_match_df = spark.read.schema(player_match_schema).format("csv").option("header", "true").load("dbfs:/mnt/ipl/raw_data/player_match.csv")

#player_match_df.show(2)

# COMMAND ----------

from pyspark.sql.functions import col, when, current_date, expr

# Add a 'veteran_status' column based on player age

player_match_df = player_match_df.withColumn(
    "veteran_status",
    when(col("age_as_on_match") >= 35, "Veteran").otherwise("Non-Veteran")
)

# Dynamic column to calculate years since debut

player_match_df = player_match_df.withColumn(
    "years_since_debut",
    (year(current_date()) - col("season_year"))
)

player_match_df.show(2)

# COMMAND ----------

team_schema = StructType([
    StructField("team_sk", IntegerType(), nullable=True),
    StructField("team_id", IntegerType(), nullable=True),
    StructField("team_name", StringType(), nullable=True)
])
 
team_df = spark.read.schema(team_schema).format("csv").option("header", "true").load("dbfs:/mnt/ipl/raw_data/team.csv")

team_df.show(2)

# COMMAND ----------

ball_by_ball_df.printSchema()
match_df.printSchema()
player_df.printSchema()
player_match_df.printSchema()
team_df.printSchema()

# COMMAND ----------

ball_by_ball_df.coalesce(1).write.mode("overwrite").format("parquet").save("dbfs:/mnt/ipl/transformed_data/ball_by_ball")
match_df.coalesce(1).write.mode("overwrite").format("parquet").save("dbfs:/mnt/ipl/transformed_data/match")
player_df.coalesce(1).write.mode("overwrite").format("parquet").save("dbfs:/mnt/ipl/transformed_data/player")
player_match_df.coalesce(1).write.mode("overwrite").format("parquet").save("dbfs:/mnt/ipl/transformed_data/player_match")
team_df.coalesce(1).write.mode("overwrite").format("parquet").save("dbfs:/mnt/ipl/transformed_data/team")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Delta Live Tables

# COMMAND ----------

# Creating Delta Live Tables
spark.read.format("parquet").load("dbfs:/mnt/ipl/transformed_data/ball_by_ball").write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("live_ball_by_ball")
spark.read.format("parquet").load("dbfs:/mnt/ipl/transformed_data/match").write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("live_match")
spark.read.format("parquet").load("dbfs:/mnt/ipl/transformed_data/player").write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("live_player")
spark.read.format("parquet").load("dbfs:/mnt/ipl/transformed_data/player_match").write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("live_player_match")
spark.read.format("parquet").load("dbfs:/mnt/ipl/transformed_data/team").write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("live_team")



# COMMAND ----------

# MAGIC %md
# MAGIC ###  Creating External Delta Live Tables

# COMMAND ----------

# Creating External Delta Live Tables
spark.sql("""
CREATE TABLE IF NOT EXISTS external_ball_by_ball
USING DELTA
LOCATION 'dbfs:/mnt/ipl/delta/ball_by_ball'
AS SELECT * FROM parquet.`dbfs:/mnt/ipl/transformed_data/ball_by_ball`
""")
spark.sql("""Refresh table external_ball_by_ball""")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS external_match
USING DELTA
LOCATION 'dbfs:/mnt/ipl/delta/match'
AS SELECT * FROM parquet.`dbfs:/mnt/ipl/transformed_data/match`
""")
spark.sql("""Refresh table external_match""")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS external_player
USING DELTA
LOCATION 'dbfs:/mnt/ipl/delta/player'
AS SELECT * FROM parquet.`dbfs:/mnt/ipl/transformed_data/player`
""")
spark.sql("""Refresh table external_player""")

# COMMAND ----------

spark.sql("""
          CREATE TABLE IF NOT EXISTS external_player_match
          USING DELTA
          LOCATION 'dbfs:/mnt/ipl/delta/player_match'
          AS SELECT * FROM parquet.`dbfs:/mnt/ipl/transformed_data/player_match`
          """)
spark.sql("""Refresh table external_player_match""")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS external_team
USING DELTA
LOCATION 'dbfs:/mnt/ipl/delta/team'
AS SELECT * FROM parquet.`dbfs:/mnt/ipl/transformed_data/team`
""")
spark.sql("""Refresh table external_team""")

# COMMAND ----------

spark.sql("SELECT * FROM external_ball_by_ball").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performing a query on the created Delta Live Tables

# COMMAND ----------

# Performing a query on the created Delta Live Tables
query_result = spark.sql("""
SELECT m.match_id, m.match_date, t.team_name AS team_batting, p.player_name, SUM(b.runs_scored) AS total_runs
FROM live_ball_by_ball b
JOIN live_match m ON b.match_id = m.match_id
JOIN live_team t ON b.team_batting = t.team_name
JOIN live_player p ON b.striker_batting_position = p.player_id
GROUP BY m.match_id, m.match_date, t.team_name, p.player_name
ORDER BY total_runs DESC
""")

display(query_result)

# COMMAND ----------

top_scoring_batsman_per_season = spark.sql("""
select p.player_name, m.season_year,
sum(b.runs_scored) as total_runs
from live_ball_by_ball b join
live_match m on b.match_id = m.match_id join 
live_player_match pm on m.match_id = pm.match_id and b.striker_batting_position = pm.player_id
join live_player p on p.player_id = pm.player_id
group by p.player_name, m.season_year
order by m.season_year, total_runs desc
""")
display(top_scoring_batsman_per_season)

# COMMAND ----------

economical_bowlers_powerplay = spark.sql("""
select p.player_name,
avg(b.runs_scored) as avg_runs_per_ball,
count(b.bowler_wicket) as total_wickets
from live_ball_by_ball b join live_player_match pm 
on pm.match_id = b.match_id and b.bowler = pm.player_id
join live_player p on p.player_id = pm.player_id
where b.over_id <= 6 group by p.player_name
having count(*)>= 1
order by avg_runs_per_ball, total_wickets desc                                    
""")
display(economical_bowlers_powerplay)

# COMMAND ----------

toss_impact_individual_matches = spark.sql("""
select match_id, toss_winner, toss_name, match_winner,
case when toss_winner == match_winner then "Won"
    else "Lost"
end as match_outcome
from live_match  where toss_name is not null
order by match_id                                           
""")

display(toss_impact_individual_matches)

# COMMAND ----------

average_runs_in_wins = spark.sql("""
select p.player_name, 
avg(b.runs_scored) as avg_runs_in_wins,
count(*) as innings_played
from live_ball_by_ball b 
join live_player_match pm  on b.match_id = pm.match_id and b.striker = pm.player_id
join live_player p on p.player_id = pm.player_id
join live_match m on pm.match_id = m.match_id
where m.match_winner = pm.player_team
group by p.player_name
order by avg_runs_in_wins desc                                 
""")

display(average_runs_in_wins)

# COMMAND ----------

scores_by_venue = spark.sql("""
    select venue_name, avg(total_runs) as avg_score, max(total_runs) as highest_score
    from ( 
        select b.match_id, m.venue_name, sum(runs_scored) as total_runs
        from live_ball_by_ball b join
        live_match m on b.match_id = m.match_id
        group by b.match_id, m.venue_name
    )
    group by venue_name order by avg_score desc
""")
display(scores_by_venue)

# COMMAND ----------

dismissal_types = spark.sql("""
select out_type, count(*) as frequency from live_ball_by_ball
where out_type is not null 
group by out_type
order by frequency desc                            
""")

display(dismissal_types)

# COMMAND ----------

team_toss_win_performance = spark.sql("""
select team1, count(*) as matches_played,
sum(case when toss_winner == match_winner then 1 else 0 end) as wins_after_toss
from live_match
where toss_winner = team1 
group by team1 
order by wins_after_toss desc

""")

display(team_toss_win_performance)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Plotting Data**

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

# Assuming 'economical_bowlers_powerplay' is already executed and available as a Spark DataFrame
economical_bowlers_pd = economical_bowlers_powerplay.toPandas()

# Visualizing using Matplotlib
plt.figure(figsize=(12, 10))
# Limiting to top 10 for clarity in the plot
top_economical_bowlers = economical_bowlers_pd.nsmallest(20, 'avg_runs_per_ball')
plt.bar(top_economical_bowlers['player_name'], top_economical_bowlers['avg_runs_per_ball'], color='lightgreen')
plt.xlabel('Bowler Name')
plt.ylabel('Average Runs per Ball')
plt.title('Most Economical Bowlers in Powerplay Overs (Top 20)')
plt.xticks(rotation=50)
plt.tight_layout()
plt.show()




# COMMAND ----------



toss_impact_pd = toss_impact_individual_matches.toPandas()


# Creating a countplot to show win/loss after winning toss
plt.figure(figsize=(12, 8))
sns.countplot(x='toss_winner', hue='match_outcome', data=toss_impact_pd)
plt.title('Impact of Winning Toss on Match Outcomes')
plt.xlabel('Toss Winner')
plt.ylabel('Number of Matches')
plt.legend(title='Match Outcome')
plt.xticks(rotation=50)
plt.tight_layout()
plt.show()


# COMMAND ----------

average_runs_pd = average_runs_in_wins.toPandas()
# Using seaborn to plot average runs in winning matches
plt.figure(figsize=(10, 8))
top_scorers = average_runs_pd.nlargest(20, 'avg_runs_in_wins')
sns.barplot(x='player_name', y='avg_runs_in_wins', data=top_scorers)
plt.title('Average Runs Scored by Batsmen in Winning Matches (Top 20 Scorers)')
plt.xlabel('Player Name')
plt.ylabel('Average Runs in Wins')
plt.xticks(rotation=50)
plt.tight_layout()
plt.show()


# COMMAND ----------

scores_by_venue_pd = scores_by_venue.toPandas()

# Plot
plt.figure(figsize=(14, 10))
sns.barplot(x='avg_score', y='venue_name', data=scores_by_venue_pd)
plt.title('Distribution of Scores by Venue')
plt.xlabel('Average Score')
plt.ylabel('Venue')
plt.show()

# COMMAND ----------

dismissal_types_pd = dismissal_types.toPandas()

# Plot
plt.figure(figsize=(12, 10))
sns.barplot(x='out_type', y='frequency', data=dismissal_types_pd, palette='pastel')
plt.title('Most Frequent Dismissal Types')
plt.xlabel('Dismissal Type')
plt.ylabel('Frequency')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

team_toss_win_pd = team_toss_win_performance.toPandas()

# Plot
plt.figure(figsize=(12, 10))
sns.barplot(x='team1', y='wins_after_toss', data=team_toss_win_pd)
plt.title('Team Performance After Winning Toss')
plt.xlabel('Team')
plt.ylabel('Wins After Winning Toss')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------


