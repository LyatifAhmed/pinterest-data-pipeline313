# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

%sql
# Disable format checks during the reading of Delta tables
SET spark.databricks.delta.formatCheck.enabled=false

from pyspark.sql.functions import from_json, col, base64
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, FloatType, StringType
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

# Stream pin post
df_pin = spark \
    .readStream \
    .format('kinesis') \
    .option('streamName','streaming-0affcdd81315-pin') \
    .option('initialPosition','earliest') \
    .option('region','us-east-1') \
    .option('awsAccessKey', ACCESS_KEY) \
    .option('awsSecretKey', SECRET_KEY) \
    .load()

# Stream geolocation
df_geo = spark\
    .readStream\
    .format('kinesis')\
    .option('streamName', 'streaming-0affcdd81315-geo')\
    .option('initialPosition', 'earliest')\
    .option('region', 'us-east-1')\
    .option('awsAccessKey', ACCESS_KEY)\
    .option('awsSecretKey', SECRET_KEY)\
    .load()

#Stream user
df_user = spark\
    .readStream\
    .format('kinesis')\
    .option('streamName', 'streaming-0affcdd81315-user')\
    .option('initialPosition', 'earliest')\
    .option('region', 'us-east-1')\
    .option('awsAccessKey', ACCESS_KEY)\
    .option('awsSecretKey', SECRET_KEY)\
    .load()

df_pin.display(10)

#Cast to string to read json
df_pin = df_pin.selectExpr("CAST(data as STRING)")
df_geo = df_geo.selectExpr("CAST(data as STRING)")
df_user = df_user.selectExpr("CAST(data as STRING)")

###Construct schema
# pin schema
df_pin_schema = StructType([\
    StructField("index", IntegerType(),True),\
    StructField("unique_id", StringType(),True),\
    StructField("title", StringType(),True),\
    StructField("follower_count", StringType(),True),\
    StructField("poster_name", StringType(),True),\
    StructField("tag_list", StringType(),True),\
    StructField("is_image_or_video", StringType(),True),\
    StructField("image_src", StringType(),True),\
    StructField("save_location", StringType(),True),\
    StructField("category", StringType(),True),\
    StructField("downloaded", IntegerType(),True),\
    StructField("description", StringType(),True)\
])

#geo schema
df_geo_schema = StructType([\
    StructField("ind", IntegerType(),True),\
    StructField("country", StringType(),True),\
    StructField("latitude", StringType(),True),\
    StructField("longitude", StringType(),True),\
    StructField("timestamp", StringType(),True),\
])

#geo schema
df_user_schema = StructType([\
    StructField("ind", IntegerType(),True),\
    StructField("first_name", StringType(),True),\
    StructField("last_name", StringType(),True),\
    StructField("age", StringType(),True),\
    StructField("date_joined", StringType(),True),\
])

#geo schema
df_geo_schema = StructType([\
    StructField("ind", IntegerType(),True),\
    StructField("country", StringType(),True),\
    StructField("latitude", StringType(),True),\
    StructField("longitude", StringType(),True),\
    StructField("timestamp", StringType(),True),\
])

#geo schema
df_user_schema = StructType([\
    StructField("ind", IntegerType(),True),\
    StructField("first_name", StringType(),True),\
    StructField("last_name", StringType(),True),\
    StructField("age", StringType(),True),\
    StructField("date_joined", StringType(),True),\
])
     
#Convert json to dataframe using the defined schemas

df_pin = df_pin.withColumn("data", from_json(col("data"),df_pin_schema))\
    .select("data.*") # data : Kinesis streaming data header name

#geo
df_geo = df_geo.withColumn('data', from_json(col('data'), df_geo_schema))\
    .select('data.*')

#user
df_user = df_user.withColumn('data', from_json(col('data'), df_user_schema))\
    .select('data.*')

df_pin.display(10)

df_pin.printSchema()

# Replace empty and entries with no relevant data with null
cleaned_df_pin = df_pin.replace({" " : None, "No description available Story format" : None, "Untitled" : None, "User Info Error" : None, "Image src error." : None, "multi-video(story page format)" : "video", "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e" : None, "No Title Data Available" : None})
# Ensure every follower count is a number
cleaned_df_pin = cleaned_df_pin.withColumn("follower_count" ,regexp_replace ("follower_count", "M", "000000"))
cleaned_df_pin = cleaned_df_pin.withColumn("follower_count" ,regexp_replace ("follower_count", "k", "000"))
# Change data types to integer
cleaned_df_pin = cleaned_df_pin.withColumn("follower_count", cleaned_df_pin["follower_count"].cast("int"))
cleaned_df_pin = cleaned_df_pin.withColumn("downloaded", cleaned_df_pin["downloaded"].cast("int"))
cleaned_df_pin = cleaned_df_pin.withColumn("index", cleaned_df_pin["index"].cast("int"))

# Clean save location to only have save location path
cleaned_df_pin = cleaned_df_pin.withColumn("save_location" ,regexp_replace ("save_location", "Local save in /", "/"))
# Rename index column to ind
cleaned_df_pin = cleaned_df_pin.withColumnRenamed("index", "ind")
# Reorder columns
cleaned_df_pin = cleaned_df_pin.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category")
cleaned_df_pin = cleaned_df_pin.fillna(0)
cleaned_df_pin.printSchema()
cleaned_df_pin.select('ind', 'follower_count', 'save_location').display(10)

df_geo.printSchema()
# Create coordinates column containing an array of latitude and longitude
cleaned_df_geo = df_geo.withColumn("coordinates", array("latitude", "longitude"))
# Drop latitude and longitude columns
cleaned_df_geo = cleaned_df_geo.drop ("latitude", "longitude")
# Transform timestamp column data type to timestamp type
cleaned_df_geo = cleaned_df_geo.withColumn("timestamp", to_timestamp("timestamp"))
# Reorder columns
cleaned_df_geo = cleaned_df_geo.select("ind", "country", "coordinates", "timestamp")
cleaned_df_geo.printSchema()
display(cleaned_df_geo)

df_user.printSchema()
# Create user_name column by concatenating first and last names
cleaned_df_user = df_user.withColumn("user_name", concat_ws(" ", "first_name", "last_name"))
# Drop first and last name columns
cleaned_df_user = cleaned_df_user.drop("first_name", "last_name")
# Convert date_joined date type to timestamp
cleaned_df_user = cleaned_df_user.withColumn("date_joined", to_timestamp("date_joined"))
# Reorder columns
cleaned_df_user = cleaned_df_user.select("ind", "user_name", "age", "date_joined")
cleaned_df_user.printSchema()
display(cleaned_df_user)

dbutils.fs.rm("/tmp/kinesis/_checkpoints/", True)
(
    cleaned_df_pin.writeStream
    .format("delta") 
    .outputMode("append") 
    .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") 
    .table("0affcdd81315_pin_table")
)

dbutils.fs.rm("/tmp/kinesis/_checkpoints/", True)
(
    cleaned_df_pin.writeStream
    .format("delta") 
    .outputMode("append") 
    .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") 
    .table("0affcdd81315_pin_table")
)

dbutils.fs.rm("/tmp/kinesis/_checkpoints/", True)
(
    cleaned_df_user.writeStream 
    .format("delta") 
    .outputMode("append") 
    .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") 
    .table("0affcdd81315_user_table")
)