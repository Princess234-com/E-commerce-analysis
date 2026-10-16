import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DecimalType
)
from pyspark.sql.functions import (
    col, lit, current_timestamp
)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#paths

raw_path = "s3://e-commerce353/bronze/raw/products/products.csv"
silver_path = "s3://e-commerce353/silver/products/"
quarantine_path = "s3://e-commerce353/quarantine/products/"
pipeline_version = "1.0"

# validation config

validation_rules = {
    "products": {
        "schema": {
            "product_id": "string",
            "product_name": "string",
            "category": "string",
            "brand": "string",
            "price": "decimal(10,2)",
            "rating": "decimal(10,2)",
            "inventory": "int"
        },

        "required_columns": [
            "product_id",
            "product_name",
            "category",
            "brand",
            "price",
            "rating",
            "inventory"
        ]
    }
}

#building schema

def build_schema(schema_dict):
    type_map = {
        "string": StringType(),
        "int": IntegerType(),
        "decimal(10,2)": DecimalType(10, 2)
    }

    fields = [
        StructField(col_name, type_map[col_type], True)
        for col_name, col_type in schema_dict.items()
    ]

    return StructType(fields)

# null validation

def apply_null_checks(df, required_columns):
    invalid = None
    for col_name in required_columns:
        condition = col(col_name).isNull()
        invalid = condition if invalid is None else (invalid | condition)

    invalid_df = df.filter(invalid)
    valid_df = df.filter(~invalid)

    return invalid_df, valid_df

# applying business rules
def apply_business_rules(df):
    return df.filter(
        (col("price") > 0) &
        (col("rating") >= 0) & (col("rating") <= 5) &
        (col("inventory") >= 0)
    )

#validation

def validate_dataset(df, rules):
    required_cols = rules["required_columns"]

    invalid_nulls, valid_df = apply_null_checks(df, required_cols)

    # Apply business rules
    valid_business_df = apply_business_rules(valid_df)

    # Records failing business rules
    invalid_business_df = valid_df.subtract(valid_business_df)

    # Combine all invalids
    invalid_df = invalid_nulls.unionByName(invalid_business_df)

    return invalid_df, valid_business_df

# reading raw data

rules = validation_rules["products"]
schema = build_schema(rules["schema"])

products = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .schema(schema)
        .csv(raw_path)
)

raw_count = products.count()

# validation

invalid_df, valid_df = validate_dataset(products, rules)

invalid_count = invalid_df.count()
valid_count = valid_df.count()

if invalid_count > 0:
    invalid_df.write.mode("append").parquet(quarantine_path)

# loading to silver

silver_df = (
    valid_df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_system", lit("e-commerce353_products"))
        .withColumn("pipeline_version", lit(pipeline_version))
)

silver_df.write.mode("overwrite").parquet(silver_path)
silver_count = silver_df.count()

# metrics


print("PRODUCTS PIPELINE COMPLETE")

print(f"RAW records:         {raw_count}")
print(f"Invalid records:     {invalid_count}")
print(f"Valid records:       {valid_count}")
print(f"SILVER records:      {silver_count}")
print(f"SILVER path:         {silver_path}")


job.commit()
