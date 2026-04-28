from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)
from spark_utils_emr import SparkUtils
import argparse


def get_transactions_schema():
    return StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", StringType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount_rate", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("order_timestamp", TimestampType(), True),
        StructField("shipping_region", StringType(), True),
        StructField("device_type", StringType(), True),
    ])


def get_products_schema():
    return StructType([
        StructField("product_id", IntegerType(), True),
        StructField("category", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("product_unit_price", DoubleType(), True),
    ])


def get_customers_schema():
    return StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("customer_region", StringType(), True),
        StructField("signup_date", StringType(), True),
    ])


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--raw-path", required=True)
    parser.add_argument("--output-path", required=True)

    args = parser.parse_args()

    su = SparkUtils("retail-batch-processing-pipeline")
    spark = su.spark

    transactions_path = f"{args.raw_path.rstrip('/')}/transactions/"
    products_path = f"{args.raw_path.rstrip('/')}/products/products.csv"
    customers_path = f"{args.raw_path.rstrip('/')}/customers/customers.csv"

    transactions_df = (
        spark.read
        .option("header", "true")
        .schema(get_transactions_schema())
        .csv(transactions_path)
    )

    products_df = (
        spark.read
        .option("header", "true")
        .schema(get_products_schema())
        .csv(products_path)
    )

    customers_df = (
        spark.read
        .option("header", "true")
        .schema(get_customers_schema())
        .csv(customers_path)
    )

    # Transformation 1: data cleaning
    clean_transactions_df = (
        transactions_df
        .dropDuplicates(["order_id"])
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("quantity").isNotNull())
        .filter(F.col("quantity") != "")
        .filter(F.col("shipping_region").isNotNull())
        .filter(F.col("shipping_region") != "")
        .withColumn("quantity", F.col("quantity").cast("int"))
        .filter(F.col("quantity") > 0)
        .filter(F.col("unit_price") > 0)
    )

    # Transformation 2: joins
    enriched_df = (
        clean_transactions_df
        .join(products_df, on="product_id", how="left")
        .join(customers_df, on="customer_id", how="left")
    )

    # Transformation 3: derived columns
    derived_df = (
        enriched_df
        .withColumn("gross_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("discount_amount", F.round(F.col("gross_amount") * F.col("discount_rate"), 2))
        .withColumn("net_amount", F.round(F.col("gross_amount") - F.col("discount_amount"), 2))
        .withColumn("order_date", F.to_date(F.col("order_timestamp")))
        .withColumn("year_month", F.date_format(F.col("order_timestamp"), "yyyy-MM"))
    )

    # Transformation 4: filtering
    completed_orders_df = (
        derived_df
        .filter(F.col("order_status") == "completed")
        .filter(F.col("category").isNotNull())
        .filter(F.col("customer_region").isNotNull())
    )

    # Transformation 5: aggregation
    result_df = (
        completed_orders_df
        .groupBy("year_month", "shipping_region", "category")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum("quantity").alias("total_units_sold"),
            F.round(F.sum("gross_amount"), 2).alias("gross_revenue"),
            F.round(F.sum("discount_amount"), 2).alias("total_discount"),
            F.round(F.sum("net_amount"), 2).alias("net_revenue"),
            F.round(F.avg("net_amount"), 2).alias("avg_order_value")
        )
        .orderBy(F.desc("net_revenue"))
    )

    # Persistence: write final data to S3 in Parquet with partitioning
    (
        result_df.write
        .mode("overwrite")
        .partitionBy("year_month")
        .parquet(args.output_path)
    )

    spark.stop()


if __name__ == "__main__":
    main()
