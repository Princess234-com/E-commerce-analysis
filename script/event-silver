import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
    DecimalType,
)
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    to_timestamp,
    to_date,
    count,
    sum,
    when,
    coalesce,
    round,
)
#config

RAW_EVENTS_PATH = "s3://e-commerce353/bronze/raw/events/events.csv"
SILVER_EVENTS_PATH = "s3://e-commerce353/silver/events/"
QUARANTINE_EVENTS_PATH = "s3://e-commerce353/quarantine/events/"

SILVER_USERS_PATH = "s3://e-commerce353/silver/users/"
SILVER_PRODUCTS_PATH = "s3://e-commerce353/silver/products/"
USER_PROFILES_PATH = "s3://e-commerce353/gold/user_profiles/"
PIPELINE_VERSION = "1.0"

#schema
events_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DecimalType(10, 2), True),
    StructField("quantity", IntegerType(), True),
    StructField("search_query", StringType(), True),
    StructField("device", StringType(), True),
])

#reading raw csv

def read_raw_csv(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    return spark.read.option("header", "true").schema(schema).csv(path)

#ref intergity join functions

def apply_referential_integrity(events: DataFrame, users: DataFrame, products: DataFrame):


    user_key = users.select(col("user_id").alias("ref_user_id")).dropDuplicates()
    product_key = products.select(col("product_id").alias("ref_product_id")).dropDuplicates()

    checked = (
        events
        .join(user_key, events["user_id"] == user_key["ref_user_id"], "left")
        .join(product_key, events["product_id"] == product_key["ref_product_id"], "left")
        .select(
            events["*"],
            user_key["ref_user_id"],
            product_key["ref_product_id"]
        )
    )

    invalid_df = checked.filter(
        col("ref_user_id").isNull() |
        col("ref_product_id").isNull() |
        col("event_id").isNull() |
        (col("price") < 1)
    )

    valid_df = checked.filter(
        col("ref_user_id").isNotNull() &
        col("ref_product_id").isNotNull() &
        col("event_id").isNotNull() &
        (col("price") >= 1)
    )

    valid_df = valid_df.drop("ref_user_id", "ref_product_id")
    invalid_df = invalid_df.drop("ref_user_id", "ref_product_id")

    return valid_df, invalid_df

#build user profile

def build_user_profiles(events_df):
    profiles_df = (
        events_df
        .groupBy("user_id")
        .agg(
            count("event_id").alias("total_events"),
            sum(when(col("event_type") == "product_view", 1).otherwise(0)).alias("product_views"),
            sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_additions"),
            sum(when(col("event_type") == "wishlist", 1).otherwise(0)).alias("wishlist_addition"),
            sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("number_of_purchases"),
            coalesce(
                sum(
                    when(
                        col("event_type") == "purchase",
                        coalesce(col("quantity"), lit(1)) * col("price")
                    ).otherwise(0)
                ),
                lit(0.0)
            ).alias("total_revenue")
        )
        .withColumn(
            "conversion_rate",
            when(col("product_views") > 0,
                 round(col("number_of_purchases") / col("product_views"), 4)
            ).otherwise(lit(0.0))
        )
        .withColumn(
            "average_order_value",
            when(col("number_of_purchases") > 0,
                 round(col("total_revenue") / col("number_of_purchases"), 2)
            ).otherwise(lit(0.0))
        )
        .select(
            "user_id",
            "total_events",
            "product_views",
            "cart_additions",
            "wishlist_addition",
            "number_of_purchases",
            "total_revenue",
            "conversion_rate",
            "average_order_value"
        )
    )

    return profiles_df

# main job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read raw events
events = read_raw_csv(spark, RAW_EVENTS_PATH, events_schema)

# Convert timestamp
events = events.withColumn(
    "timestamp",
    to_timestamp("timestamp", "M/d/yyyy h:mm:ss a")
)

# Add partition column
events = events.withColumn("event_date", to_date("timestamp"))

# Read reference tables
users = spark.read.parquet(SILVER_USERS_PATH)
products = spark.read.parquet(SILVER_PRODUCTS_PATH)

# Apply referential integrity
valid_events, invalid_events = apply_referential_integrity(events, users, products)

# Write quarantine
invalid_events.write.mode("append").parquet(QUARANTINE_EVENTS_PATH)
# user profiles
user_profiles = build_user_profiles(valid_events)

# Add metadata
silver_events = (
    valid_events
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_system", lit("e-commerce353_events"))
    .withColumn("pipeline_version", lit(PIPELINE_VERSION))
)

# Write silver partitioned by event_date
silver_events.write.format("parquet") \
    .partitionBy("event_date") \
    .mode("overwrite") \
    .save(SILVER_EVENTS_PATH)

#user profile 
user_profiles.write.mode("overwrite").parquet(USER_PROFILES_PATH)

job.commit()
