from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc, broadcast

# Initialize Spark Session
spark = SparkSession.builder.appName("SparkFundamentals").getOrCreate()

# Disable automatic broadcast joins
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Load Data from CSV files
df_matchesBucketed = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/matches.csv")
df_matchDetailsBucketed = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/match_details.csv")
df_medalsmatchesplayersBucketed = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals_matches_players.csv")
df_medals = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals.csv")
df_maps = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/maps.csv")

# Explicitly Broadcast JOIN medals and maps since they are small tables
df_medals = broadcast(df_medals)
df_maps = broadcast(df_maps)

# Bucket join match_details, matches, and medals_matches_players on match_id
num_buckets = 16
df_matchesBucketed = df_matchesBucketed.repartitionByRange(num_buckets, col("match_id"))
df_matchDetailsBucketed = df_matchDetailsBucketed.repartitionByRange(num_buckets, col("match_id"))
df_medalsmatchesplayersBucketed = df_medalsmatchesplayersBucketed.repartitionByRange(num_buckets, col("match_id"))

# Perform bucketed join
df_joined = (
    df_matchDetailsBucketed
    .join(df_matchesBucketed, "match_id")
    .join(df_medalsmatchesplayersBucketed, "match_id")
    .join(df_medals, "medal_id")  # Broadcast join with medals
    .join(df_maps, "map_id")      # Broadcast join with maps
)

# Which player averages the most kills per game?
kills_per_game = (
    df_joined
    .groupBy("player_id")
    .agg(avg("kills").alias("avg_kills"))
    .orderBy(desc("avg_kills"))
)

kills_per_game.show(10)

# Which playlist gets played the most?
playlist_count = (
    df_matchesBucketed
    .groupBy("playlist")
    .agg(count("match_id").alias("num_matches"))
    .orderBy(desc("num_matches"))
)

playlist_count.show(10)

# Which map gets played the most?
map_count = (
    df_matchesBucketed
    .groupBy("map")
    .agg(count("match_id").alias("num_matches"))
    .orderBy(desc("num_matches"))
)

map_count.show(10)

# Which map do players get the most Killing Spree medals on?
killing_spree_medals = (
    df_medalsmatchesplayersBucketed
    .filter(col("medal_type") == "Killing Spree")
    .groupBy("map")
    .agg(count("medal_id").alias("num_medals"))
    .orderBy(desc("num_medals"))
)

killing_spree_medals.show(10)

# Experimenting with .sortWithinPartitions()
sorted_df1 = df_joined.sortWithinPartitions("playlist")
sorted_df2 = df_joined.sortWithinPartitions("map")

sorted_df1.write.mode("overwrite").parquet("/home/iceberg/data/sorted_by_playlist")
sorted_df2.write.mode("overwrite").parquet("/home/iceberg/data/sorted_by_map")

# Print sorted file sizes
import os

playlist_size = os.path.getsize("/home/iceberg/data/sorted_by_playlist")
map_size = os.path.getsize("/home/iceberg/data/sorted_by_map")

print(f"Playlist Sorted Size: {playlist_size}")
print(f"Map Sorted Size: {map_size}")

