from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, FloatType, IntegerType, 
    ShortType, ByteType, DoubleType
)
import glob
from functools import reduce

# Recursively find all .laz files
base_path = "/Volumes/nordics_dbsql_demo/geo_analysis/lidar"
laz_files = glob.glob(f"{base_path}/**/*.laz", recursive=True)

# Create a DataFrame of file paths
paths_df = spark.createDataFrame(
    [(path,) for path in laz_files],
    schema=["file_path"]
)

# Read all files in parallel using unionAll with reduce (no for loops)
# Spark will parallelize the reads when the action is triggered
if laz_files:
    # Define function to read a single file (lazy operation)
    def read_las_file(file_path):
        """Read a single LAS file - returns lazy DataFrame"""
        return spark.read.format("las").option("path", file_path).load()
    
    # Use map to create list of lazy DataFrames, then reduce with unionAll
    # Spark will optimize and parallelize the actual reads
    dfs = list(map(read_las_file, laz_files))
    
    # Union all DataFrames using reduce (no for loop)
    # This creates a single DataFrame that Spark will read in parallel
    df = reduce(lambda df1, df2: df1.unionAll(df2), dfs) if dfs else None
    
    if df is None:
        # Create empty DataFrame with LAS schema if no files were read
        df = spark.createDataFrame([], schema=StructType([
            StructField("x", FloatType(), True),
            StructField("y", FloatType(), True),
            StructField("z", FloatType(), True),
            StructField("intensity", IntegerType(), True),
            StructField("return_number", ShortType(), True),
            StructField("number_of_returns", ShortType(), True),
            StructField("scan_direction_flag", ByteType(), True),
            StructField("edge_of_flight_line", ByteType(), True),
            StructField("classification", ShortType(), True),
            StructField("synthetic", ByteType(), True),
            StructField("key_point", ByteType(), True),
            StructField("withheld", ByteType(), True),
            StructField("scan_angle", ShortType(), True),
            StructField("user_data", ShortType(), True),
            StructField("point_source_id", IntegerType(), True),
            StructField("gps_time", DoubleType(), True),
            StructField("red", ShortType(), True),
            StructField("green", ShortType(), True),
            StructField("blue", ShortType(), True)
        ]))
else:
    print("No .laz files found")
    # Create empty DataFrame with LAS schema
    df = spark.createDataFrame([], schema=StructType([
        StructField("x", FloatType(), True),
        StructField("y", FloatType(), True),
        StructField("z", FloatType(), True),
        StructField("intensity", IntegerType(), True),
        StructField("return_number", ShortType(), True),
        StructField("number_of_returns", ShortType(), True),
        StructField("scan_direction_flag", ByteType(), True),
        StructField("edge_of_flight_line", ByteType(), True),
        StructField("classification", ShortType(), True),
        StructField("synthetic", ByteType(), True),
        StructField("key_point", ByteType(), True),
        StructField("withheld", ByteType(), True),
        StructField("scan_angle", ShortType(), True),
        StructField("user_data", ShortType(), True),
        StructField("point_source_id", IntegerType(), True),
        StructField("gps_time", DoubleType(), True),
        StructField("red", ShortType(), True),
        StructField("green", ShortType(), True),
        StructField("blue", ShortType(), True)
    ]))

# Write to table - Spark will read all files in parallel during this action
df.write.mode("append").option("mergeSchema", "true").saveAsTable("nordics_dbsql_demo.geo_analysis.lidar_test")
