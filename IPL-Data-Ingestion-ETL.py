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

dbutils.fs.unmount("/mnt/ipl_raw")

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

# Creating Delta Live Tables
spark.read.format("parquet").load("dbfs:/mnt/ipl/transformed_data/ball_by_ball").write.format("delta").mode("overwrite").saveAsTable("live_ball_by_ball")
spark.read.format("parquet").load("dbfs:/mnt/ipl/transformed_data/match").write.format("delta").mode("overwrite").saveAsTable("live_match")
spark.read.format("parquet").load("dbfs:/mnt/ipl/transformed_data/player").write.format("delta").mode("overwrite").saveAsTable("live_player")
spark.read.format("parquet").load("dbfs:/mnt/ipl/transformed_data/player_match").write.format("delta").mode("overwrite").saveAsTable("live_player_match")
spark.read.format("parquet").load("dbfs:/mnt/ipl/transformed_data/team").write.format("delta").mode("overwrite").saveAsTable("live_team")



# COMMAND ----------

spark.sql("SELECT * FROM live_ball_by_ball").display()

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
