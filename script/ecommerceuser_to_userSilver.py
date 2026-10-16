import sys
from awsglue.transforms import ApplyMapping
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lit, current_timestamp

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

RAW_PATH = "s3://e-commerce353/bronze/raw/users/users.csv"
SILVER_PATH = "s3://e-commerce353/silver/users/"
QUARANTINE_PATH = "s3://e-commerce353/quarantine/users"
PIPELINE_VERSION = "1.0"

# reading data
users = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(RAW_PATH)
)

raw_count = users.count()
print(f"RAW payment records: {raw_count}")

# datatype conversion
users = (
    users
    .withColumn("user_id", col("user_id").cast("string"))
    .withColumn("country", col("country").cast("string"))
    .withColumn("age", col("age").cast("int"))
    .withColumn("gender", col("gender").cast("string"))
    .withColumn("device", col("device").cast("string"))
    .withColumn("acquisition_channel", col("acquisition_channel").cast("string"))
    .withColumn("signup_date", col("signup_date").cast("date"))
)

# invalid users
invalid_users = users.filter(
    col("user_id").isNull() |
    col("age").isNull() |
    (col("age") < 10) |
    (col("age") > 100) |
    col("country").isNull() |
    col("gender").isNull() |
    col("device").isNull() |
    col("acquisition_channel").isNull() |
    col("signup_date").isNull()
)

invalid_count = invalid_users.count()
print(f"Invalid customer records: {invalid_count}")

if invalid_count > 0:
    invalid_users.write.mode("append").parquet(QUARANTINE_PATH)

# valid users
valid_users = users.filter(
    col("user_id").isNotNull() &
    col("age").isNotNull() &
    (col("age") >= 10) &
    (col("age") <= 100) &
    col("country").isNotNull() &
    col("gender").isNotNull() &
    col("device").isNotNull() &
    col("acquisition_channel").isNotNull() &
    col("signup_date").isNotNull()
)

before_dedup = valid_users.count()
valid_users = valid_users.dropDuplicates(["user_id"])
after_dedup = valid_users.count()
duplicates_removed = before_dedup - after_dedup

# metadata
silver_users = (
    valid_users
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_system", lit("e-commerce353"))
    .withColumn("pipeline_version", lit(PIPELINE_VERSION))
)

silver_users.write.mode("overwrite").parquet(SILVER_PATH)

silver_count = silver_users.count()


print("CUSTOMERS PIPELINE COMPLETE")

print(f"RAW records:         {raw_count}")
print(f"Invalid records:     {invalid_count}")
print(f"Duplicates removed:  {duplicates_removed}")
print(f"SILVER records:      {silver_count}")
print(f"SILVER path:         {SILVER_PATH}")


job.commit()
