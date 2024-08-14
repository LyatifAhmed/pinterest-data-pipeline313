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

# AWS S3 bucket name
AWS_S3_BUCKET = "user-0affcdd81315-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/MOUNT"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

display(dbutils.fs.ls("/mnt/MOUNT/topics"))

%sql
# Disable format checks during the reading of Delta tables
SET spark.databricks.delta.formatCheck.enabled=false

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/MOUNT/topics/0affcdd81315.pin/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_pin = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_pin)

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/MOUNT/topics/0affcdd81315.geo/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_geo = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_geo)

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/MOUNT/topics/0affcdd81315.user/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_user = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_user)

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
cleaned_df_pin.printSchema()
display(cleaned_df_pin)

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
cleaned_df_user = df_user.withColumn("user_name", concat("first_name", "last_name"))
# Drop first and last name columns
cleaned_df_user = cleaned_df_user.drop("first_name", "last_name")
# Convert date_joined date type to timestamp
cleaned_df_user = cleaned_df_user.withColumn("date_joined", to_timestamp("date_joined"))
# Reorder columns
cleaned_df_user = cleaned_df_user.select("ind", "user_name", "age", "date_joined")
cleaned_df_user.printSchema()
display(cleaned_df_user)

display(cleaned_df_pin)
display(cleaned_df_geo)
display(cleaned_df_user)

# MOST POPULAR CATEGORY IN EACH COUNTRY

# Combine pin and geo dataframes
popular_category_country_df = cleaned_df_pin.join(cleaned_df_geo, cleaned_df_geo["ind"] == cleaned_df_pin["ind"], how="inner")
# Create window
from pyspark.sql.window import Window
window = Window.partitionBy("country", "category")
# Create category count column and select columns to show
popular_category_country_df = popular_category_country_df.withColumn("category_count", count("category").over(window)).select("country", "category", "category_count")
# Add column assigning row numbers to each unique category within each country
popular_category_country_df = popular_category_country_df.withColumn("row", row_number().over(window.orderBy("category_count")))
# Filter rows so only rows with row number 1 remain and re order columns then drop row column
popular_category_country_df = popular_category_country_df.filter(col("row") == 1).select("country", "category", "category_count").drop("row")

display(popular_category_country_df)

# MOST POPULAR CATEGORY EACH YEAR (2018 - 2022)
# Combine pin and geo dataframes
popular_category_year_df = cleaned_df_pin.join(cleaned_df_geo, cleaned_df_geo["ind"] == cleaned_df_pin["ind"], how="inner")
display(popular_category_year_df)
# create partition by year and order by category_count descending
windowYearByCatCount = Window.partitionBy("post_year").orderBy(col("category_count").desc())
# find which was the most popular category each year between 2018 and 2022
popular_category_year_df.withColumn("post_year", year("timestamp")) \
.filter(col("post_year") >= 2018) \
.filter(col("post_year") <= 2022) \
.groupBy("post_year", "category") \
.agg(count("category").alias("category_count")) \
.withColumn("rank", row_number().over(windowYearByCatCount)) \
.filter(col("rank") == 1) \
.drop("rank") \
.show()

# MOST POPULAR USER IN EACH COUNTRY AND MOST POPULAR COUNTRY
# Combine pin and geo dataframes
user_followers_country_df = cleaned_df_pin.join(cleaned_df_geo, cleaned_df_geo["ind"] == cleaned_df_pin["ind"], how="inner")
# Select columns to show, drop null value rows and duplicate rows and change order
user_followers_country_df = user_followers_country_df.select("country", "poster_name", "follower_count").na.drop().dropDuplicates(["country", "poster_name"]).orderBy(["country", "follower_count"], ascending = [True, False])
# Group by country with the most followers for each country in descending order
country_most_followers_df = user_followers_country_df.groupBy("country").agg(max("follower_count").alias("follower_count")).orderBy("follower_count", ascending = False)
display(user_followers_country_df)
display(country_most_followers_df)

# MOST POPULAR CATEGORY FOR DIFFERENT AGE GROUPS
# Combine pin and user dataframes
popular_category_age_df = cleaned_df_pin.join(cleaned_df_user, cleaned_df_user["ind"] == cleaned_df_pin["ind"], how="inner")

# Creates age group column with conditionals for each age range
popular_category_age_df = popular_category_age_df.withColumn("age_group", when(popular_category_age_df.age < 18, popular_category_age_df.age).when(popular_category_age_df.age <= 24, "18-24").when(popular_category_age_df.age <= 35, "25-35").when(popular_category_age_df.age <= 50, "36-50").otherwise("50+"))

# Create window
window = Window.partitionBy("age_group", "category")
# Create category count column and select columns to show
popular_category_age_df = popular_category_age_df.withColumn("category_count", count("category").over(window)).select("age_group", "category", "category_count")
# Add column assigning row numbers to each unique category for each age group
popular_category_age_df = popular_category_age_df.withColumn("row", row_number().over(window.orderBy("category_count")))
# Filter rows so only rows with row number 1 remain and re order columns then drop row column
popular_category_age_df = popular_category_age_df.filter(popular_category_age_df.row == 1).orderBy(["age_group", "category_count"], ascending = [True, False]).drop("row")

display(popular_category_age_df)

# MEDIAN FOLLOWER COUNT FOR DIFFERENT AGE GROUPS
# Combine pin and user dataframes
median_follower_age_df = cleaned_df_pin.join(cleaned_df_user, cleaned_df_user["ind"] == cleaned_df_pin["ind"], how="inner")
# Creates age group column with conditionals for each age range
median_follower_age_df = median_follower_age_df.withColumn("age_group", when(median_follower_age_df.age < 18, median_follower_age_df.age).when(median_follower_age_df.age <= 24, "18-24").when(median_follower_age_df.age <= 35, "25-35").when(median_follower_age_df.age <= 50, "36-50").otherwise("50+"))
# Creates a median follower count column after grouping by age group
median_follower_age_df = median_follower_age_df.groupBy("age_group").agg(expr("percentile_approx(follower_count, 0.5)").alias("median_follower_count")).orderBy("age_group")
display(median_follower_age_df)

# NUMBER OF USERS JOINING EACH YEAR (2015 - 2020)
# Range of dates from 2015 to 2020 
dates = ("2015-01-01", "2020-12-31")
# Filter out date joined column for dates between 2015 and 2020, renaming it as post year with just the year value and select columns to show
users_joining_df = cleaned_df_user.filter(cleaned_df_user.date_joined.between(*dates)).select(year("date_joined").alias("post_year"))
# Create window
window = Window.partitionBy("post_year")
# Create number of users joined column for each year
users_joining_df = users_joining_df.withColumn("number_users_joined", count("post_year").over(window))
# Add column assigning row numbers to each user joining for a specific year
users_joining_df = users_joining_df.withColumn("row", row_number().over(window.orderBy("number_users_joined")))
# Filter rows so only rows with row number 1 remain and re order columns then drop row column
users_joining_df = users_joining_df.filter(users_joining_df.row == 1).orderBy("post_year").drop("row")
display(users_joining_df)

# MEDIAN FOLLOWER COUNT OF USERS BASED ON JOINING YEAR (2015 - 2020)
# Combine pin and user dataframes
median_follower_year_df = cleaned_df_pin.join(cleaned_df_user, cleaned_df_user["ind"] == cleaned_df_pin["ind"], how="inner")
# Range of dates from 2015 to 2020 
dates = ("2015-01-01", "2020-12-31")
# Filter out date joined column for dates between 2015 and 2020, renaming it as post year with just the year value and select columns to show
median_follower_year_df = median_follower_year_df.filter(median_follower_year_df.date_joined.between(*dates)).select(year("date_joined").alias("post_year"), "follower_count")
# Creates a median follower count column after grouping by post year
median_follower_year_df = median_follower_year_df.groupBy("post_year").agg(expr("percentile_approx(follower_count, 0.5)").alias("median_follower_count")).orderBy("post_year")
display(median_follower_year_df)







