import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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
    when,
    lit,
    current_timestamp,
    to_date,
)

#config

RAW_ORDERS_PATH = "s3://e-commerce353/bronze/raw/orders/orders.csv"
SILVER_ORDERS_PATH = "s3://e-commerce353/silver/orders/"
QUARANTINE_ORDERS_PATH = "s3://e-commerce353/quarantine/orders/"

SILVER_USERS_PATH = "s3://e-commerce353/silver/users/"
SILVER_PRODUCTS_PATH = "s3://e-commerce353/silver/products/"

PIPELINE_VERSION = "1.0"

# defining schema
ORDERS_VALIDATION_RULES = {
    "schema": {
        "order_id": "string",
        "timestamp": "timestamp",
        "user_id": "string",
        "product_id": "string",
        "quantity": "decimal(10,2)",  
        "unit_price": "decimal(10,2)",
        "discount": "decimal(10,2)",
        "total_amount": "decimal(10,2)",
    },
    "required_columns": [
        "order_id",
        "user_id",
        "product_id",
        "timestamp",
        "quantity",
        "unit_price",
    ],
    "foreign_keys": {
        "user_id": {
            "reference_df": "users",
            "reference_column": "user_id",
        },
        "product_id": {
            "reference_df": "products",
            "reference_column": "product_id",
        },
    },
    "business_rules": {
        "quantity_min": 1,
        "unit_price_min": 1,
    },
}


# helper functions

def build_schema(schema_dict: dict) -> StructType:
    type_map = {
        "string": StringType(),
        "timestamp": TimestampType(),
        "int": IntegerType(),
        "decimal(10,2)": DecimalType(10, 2),
    }

    fields = []
    for name, type_str in schema_dict.items():
        spark_type = type_map.get(type_str.lower())
        if spark_type is None:
            raise ValueError(f"Unsupported type '{type_str}' for column '{name}'")
        fields.append(StructField(name, spark_type, nullable=True))

    return StructType(fields)


def apply_null_checks(df, required_columns):
    null_condition = None
    for col_name in required_columns:
        cond = col(col_name).isNull()
        null_condition = cond if null_condition is None else (null_condition | cond)

    invalid_nulls_df = df.filter(null_condition)
    valid_df = df.filter(~null_condition)

    return invalid_nulls_df, valid_df


def apply_foreign_key_checks(df, fk_rules, reference_dfs):
    current_valid_df = df
    all_invalid_fk_df = None

    for fk_column, rule in fk_rules.items():
        ref_df_name = rule["reference_df"]
        ref_column = rule["reference_column"]

        if ref_df_name not in reference_dfs:
            raise ValueError(f"Reference dataframe '{ref_df_name}' not provided")

        # Select distinct keys to prevent row multiplication
        ref_keys = (
            reference_dfs[ref_df_name]
            .select(col(ref_column).alias("_ref_key"))
            .distinct()
        )

        # Single Left Join to split into valid and invalid sets
        joined_df = current_valid_df.join(
            ref_keys,
            current_valid_df[fk_column] == ref_keys["_ref_key"],
            how="left"
        )

        invalid_fk = joined_df.filter(col("_ref_key").isNull()).drop("_ref_key")
        valid_fk = joined_df.filter(col("_ref_key").isNotNull()).drop("_ref_key")

        if all_invalid_fk_df is None:
            all_invalid_fk_df = invalid_fk
        else:
            all_invalid_fk_df = all_invalid_fk_df.unionByName(invalid_fk)

        current_valid_df = valid_fk

    if all_invalid_fk_df is None:
        all_invalid_fk_df = current_valid_df.limit(0)

    return all_invalid_fk_df, current_valid_df


def apply_business_rules(df, rules):
    quantity_min = rules.get("quantity_min", 1)
    unit_price_min = rules.get("unit_price_min", 1)

    invalid_condition = (
        col("quantity").isNull() |
        col("unit_price").isNull() |
        (col("quantity") < quantity_min) |
        (col("unit_price") < unit_price_min)
    )

    invalid_business_df = df.filter(invalid_condition)
    valid_df = df.filter(~invalid_condition)

    return invalid_business_df, valid_df


#main code

def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    rules = ORDERS_VALIDATION_RULES

    # 1. Read raw CSV with correct structural schema
    schema = build_schema(rules["schema"])
    orders_df = (
        spark.read
        .schema(schema)
        .option("header", "true")
        .csv(RAW_ORDERS_PATH)
    )

 
    deduped_orders_df = orders_df.dropDuplicates(["order_id"])

    # reading ref tables
    users_df = spark.read.parquet(SILVER_USERS_PATH)
    products_df = spark.read.parquet(SILVER_PRODUCTS_PATH)

    reference_dfs = {
        "users": users_df,
        "products": products_df,
    }

    # validation
    invalid_nulls_df, valid_df = apply_null_checks(
        deduped_orders_df,
        rules["required_columns"],
    )

    invalid_fk_df, valid_df = apply_foreign_key_checks(
        valid_df,
        rules["foreign_keys"],
        reference_dfs,
    )

    invalid_business_df, valid_df = apply_business_rules(
        valid_df,
        rules["business_rules"],
    )

    # combine all quarantined records
    invalid_df = (
        invalid_nulls_df
        .unionByName(invalid_fk_df)
        .unionByName(invalid_business_df)
    )

    # add metadata, rename timestamp column, and build partition column
    invalid_df_audit = (
        invalid_df
        .withColumnRenamed("timestamp", "order_timestamp")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_system", lit("e-commerce353_orders"))
        .withColumn("pipeline_version", lit(PIPELINE_VERSION))
        .withColumn("order_date", to_date(col("order_timestamp")))
    )

    silver_df = (
        valid_df
        .withColumnRenamed("timestamp", "order_timestamp")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_system", lit("e-commerce353_orders"))
        .withColumn("pipeline_version", lit(PIPELINE_VERSION))
        .withColumn("order_date", to_date(col("order_timestamp")))
    )


    (
        invalid_df_audit.write
        .mode("append")
        .partitionBy("order_date")
        .parquet(QUARANTINE_ORDERS_PATH)
    )

    (
        silver_df.write
        .mode("overwrite")
        .partitionBy("order_date")
        .parquet(SILVER_ORDERS_PATH)
    )

    print("ORDERS PIPELINE COMPLETE")
    print(f"SILVER path:     {SILVER_ORDERS_PATH}")
    print(f"QUARANTINE path: {QUARANTINE_ORDERS_PATH}")

    job.commit()


if __name__ == "__main__":
    main()
