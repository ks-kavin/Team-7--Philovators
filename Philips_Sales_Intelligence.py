# Databricks notebook source
print("PHILIPS SALES INTELLIGENCE PLATFORM")
print("Databricks environment ready!")

import sys
print("Python version:", sys.version)

# COMMAND ----------

print("Spark version:", spark.version)
print("Spark session:", spark)

# COMMAND ----------

catalogs = spark.sql("SHOW CATALOGS")

display(catalogs)

# COMMAND ----------

catalog_name = "workspace"

schemas = spark.sql(
    f"SHOW SCHEMAS IN `{catalog_name}`"
)

display(schemas)

# COMMAND ----------

spark.sql("""
CREATE VOLUME IF NOT EXISTS
workspace.default.philips_sales_volume
""")

print("Philips project Volume created successfully!")

# COMMAND ----------

display(
    spark.sql("""
        SHOW VOLUMES IN workspace.default
    """)
)

# COMMAND ----------

raw_path = "/Volumes/workspace/default/philips_sales_volume/raw"

dbutils.fs.mkdirs(raw_path)

print("Raw folder created successfully!")


# COMMAND ----------

display(dbutils.fs.ls(
    "/Volumes/workspace/default/philips_sales_volume/"
))

# COMMAND ----------

display(
    dbutils.fs.ls(
        "/Volumes/workspace/default/philips_sales_volume/raw/"
    )
)

# COMMAND ----------

import openpyxl

print("OpenPyXL version:", openpyxl.__version__)

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

import openpyxl
import pyspark

print("OpenPyXL:", openpyxl.__version__)
print("Spark:", spark.version)
print("Bronze ingestion environment is ready!")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# ==========================================
# PHILIPS SALES INTELLIGENCE
# BRONZE LAYER - CONFIGURATION
# ==========================================

RAW_PATH = "/Volumes/workspace/default/philips_sales_volume/raw"

BRONZE_SCHEMA = "workspace.default"

ORDERS_FILE = f"{RAW_PATH}/Orders_Raw.xlsx"
CUSTOMERS_FILE = f"{RAW_PATH}/Customers_Raw.xlsx"
PRODUCTS_FILE = f"{RAW_PATH}/Products_Raw (1).xlsx"

print("Source paths configured successfully!")

# COMMAND ----------

files = dbutils.fs.ls(RAW_PATH)

for file in files:
    print(file.name, "|", file.size, "bytes")

# COMMAND ----------

from openpyxl import load_workbook

excel_files = {
    "orders": ORDERS_FILE,
    "customers": CUSTOMERS_FILE,
    "products": PRODUCTS_FILE
}

for dataset_name, file_path in excel_files.items():

    workbook = load_workbook(
        filename=file_path,
        read_only=True,
        data_only=True
    )

    print("\n" + "=" * 60)
    print("DATASET:", dataset_name.upper())
    print("WORKSHEETS:", workbook.sheetnames)

    for sheet in workbook.worksheets:

        print("\nSheet name:", sheet.title)
        print("Rows:", sheet.max_row)
        print("Columns:", sheet.max_column)

        headers = next(
            sheet.iter_rows(
                min_row=1,
                max_row=1,
                values_only=True
            )
        )

        print("Headers:", list(headers))

    workbook.close()

# COMMAND ----------

# MAGIC %md
# MAGIC # BRONZE LAYER – RAW DATA INGESTION
# MAGIC
# MAGIC ### Objective
# MAGIC
# MAGIC Load the three original Excel workbooks into Bronze Delta tables while preserving their original values.
# MAGIC
# MAGIC ### Implementation
# MAGIC
# MAGIC We used OpenPyXL to read the Excel worksheets and converted the records into Spark DataFrames.
# MAGIC
# MAGIC The raw values were preserved as strings to avoid unwanted transformations during ingestion.
# MAGIC
# MAGIC We also added metadata columns:
# MAGIC
# MAGIC - `_source_file` – Original filename.
# MAGIC - `_source_sheet` – Worksheet name.
# MAGIC - `_ingested_at_utc` – Ingestion timestamp.
# MAGIC - `_source_file_sha256` – Source file hash.
# MAGIC - `_source_row_number` – Original row number.
# MAGIC
# MAGIC ### Bronze tables
# MAGIC
# MAGIC 1. `workspace.default.bronze_orders`
# MAGIC 2. `workspace.default.bronze_customers`
# MAGIC 3. `workspace.default.bronze_products`
# MAGIC
# MAGIC ### Results
# MAGIC
# MAGIC | Bronze table | Records |
# MAGIC |---|---:|
# MAGIC | bronze_orders | 5,000 |
# MAGIC | bronze_customers | 500 |
# MAGIC | bronze_products | 200 |
# MAGIC
# MAGIC ### Why Bronze?
# MAGIC
# MAGIC Bronze acts as the raw data layer. Preserving original values allows us to trace the source, investigate issues and reprocess data when required.
# MAGIC
# MAGIC We do not perform business-level cleaning in this layer.

# COMMAND ----------


# ============================================================
# PHILIPS SALES INTELLIGENCE PLATFORM
# PHASE 1: BRONZE LAYER - EXCEL INGESTION
# ============================================================

from openpyxl import load_workbook
from pyspark.sql.types import (
    StructType, StructField, StringType
)
from datetime import datetime, date, timezone
import hashlib
import os


# ------------------------------------------------------------
# 1. DATASET CONFIGURATION
# ------------------------------------------------------------

datasets = {
    "orders": {
        "file": ORDERS_FILE,
        "sheet": "Orders_Raw",
        "table": "workspace.default.bronze_orders",
        "expected_rows": 5000
    },
    "customers": {
        "file": CUSTOMERS_FILE,
        "sheet": "Customers_Raw",
        "table": "workspace.default.bronze_customers",
        "expected_rows": 500
    },
    "products": {
        "file": PRODUCTS_FILE,
        "sheet": "Products_Raw",
        "table": "workspace.default.bronze_products",
        "expected_rows": 200
    }
}


# ------------------------------------------------------------
# 2. CONVERT EXCEL VALUES TO STRINGS
# ------------------------------------------------------------

def convert_excel_value(value):

    if value is None:
        return None

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, bool):
        return str(value)

    return str(value)


# ------------------------------------------------------------
# 3. CALCULATE SOURCE FILE HASH
# ------------------------------------------------------------

def calculate_file_hash(file_path):

    sha256 = hashlib.sha256()

    with open(file_path, "rb") as file:

        for chunk in iter(
            lambda: file.read(1024 * 1024), b""
        ):
            sha256.update(chunk)

    return sha256.hexdigest()


# ------------------------------------------------------------
# 4. BRONZE INGESTION FUNCTION
# ------------------------------------------------------------

def ingest_excel_to_bronze(dataset_name, config):

    file_path = config["file"]
    sheet_name = config["sheet"]
    table_name = config["table"]
    expected_rows = config["expected_rows"]

    print("\n" + "=" * 65)
    print("INGESTING:", dataset_name.upper())
    print("=" * 65)

    # Check source file
    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)

    # Calculate deterministic file hash
    file_hash = calculate_file_hash(file_path)

    # --------------------------------------------------------
    # READ EXCEL WORKBOOK
    # --------------------------------------------------------

    workbook = load_workbook(
        filename=file_path,
        read_only=True,
        data_only=True
    )

    try:

        if sheet_name not in workbook.sheetnames:
            raise ValueError(
                f"Worksheet not found: {sheet_name}"
            )

        sheet = workbook[sheet_name]

        # Read headers
        headers = next(
            sheet.iter_rows(
                min_row=1,
                max_row=1,
                values_only=True
            )
        )

        headers = [
            str(header).strip()
            if header is not None else ""
            for header in headers
        ]

        if any(not header for header in headers):
            raise ValueError("Blank column header found")

        if len(headers) != len(set(headers)):
            raise ValueError("Duplicate headers found")

        # Read records
        records = []

        ingestion_time = datetime.now(
            timezone.utc
        ).isoformat()

        for excel_row_number, row in enumerate(
            sheet.iter_rows(min_row=2, values_only=True),
            start=2
        ):

            values = [
                convert_excel_value(value)
                for value in row
            ]

            # Preserve every non-empty source record
            if any(value is not None for value in values):

                values = values[:len(headers)]

                while len(values) < len(headers):
                    values.append(None)

                records.append(
                    values + [
                        os.path.basename(file_path),
                        sheet_name,
                        ingestion_time,
                        file_hash,
                        str(excel_row_number)
                    ]
                )

    finally:
        workbook.close()

    actual_rows = len(records)

    print("Source file:", os.path.basename(file_path))
    print("Worksheet:", sheet_name)
    print("Source columns:", len(headers))
    print("Source records:", actual_rows)

    # Verify expected row count
    if actual_rows != expected_rows:

        raise ValueError(
            f"{dataset_name}: Expected {expected_rows} rows, "
            f"found {actual_rows}"
        )

    # --------------------------------------------------------
    # CREATE SPARK DATAFRAME
    # --------------------------------------------------------

    metadata_columns = [
        "_source_file",
        "_source_sheet",
        "_ingested_at_utc",
        "_source_file_sha256",
        "_source_row_number"
    ]

    all_columns = headers + metadata_columns

    schema = StructType([
        StructField(column, StringType(), True)
        for column in all_columns
    ])

    bronze_df = spark.createDataFrame(
        records,
        schema=schema
    )

    # --------------------------------------------------------
    # CHECK EXISTING TABLE
    # --------------------------------------------------------

    table_exists = spark.catalog.tableExists(table_name)

    if table_exists:

        existing_df = spark.table(table_name)

        # Validate schema before append
        if existing_df.columns != bronze_df.columns:

            raise ValueError(
                f"Schema mismatch in {table_name}. "
                "Do not append until schema evolution is reviewed."
            )

        # Check whether this exact source file was ingested
        existing_batch = (
            existing_df
            .filter(
                existing_df["_source_file_sha256"] == file_hash
            )
            .limit(1)
            .count()
        )

        if existing_batch > 0:

            existing_rows = (
                existing_df
                .filter(
                    existing_df["_source_file_sha256"] == file_hash
                )
                .count()
            )

            if existing_rows != actual_rows:

                raise RuntimeError(
                    "Existing ingestion batch has an unexpected "
                    "row count. Manual investigation required."
                )

            print("This exact source file is already ingested.")
            print("Duplicate ingestion skipped.")

            return {
                "dataset": dataset_name,
                "source_rows": actual_rows,
                "stored_rows": existing_rows,
                "status": "ALREADY INGESTED - SKIPPED"
            }

    # --------------------------------------------------------
    # WRITE TO DELTA TABLE
    # --------------------------------------------------------

    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(table_name)
    )

    # --------------------------------------------------------
    # VERIFY STORED RECORDS
    # --------------------------------------------------------

    stored_df = spark.table(table_name)

    stored_rows = (
        stored_df
        .filter(
            stored_df["_source_file_sha256"] == file_hash
        )
        .count()
    )

    if stored_rows != actual_rows:

        raise RuntimeError(
            f"Bronze verification failed for {dataset_name}: "
            f"expected {actual_rows}, found {stored_rows}"
        )

    print("Bronze table:", table_name)
    print("Verified records:", stored_rows)
    print("Status: SUCCESS")

    return {
        "dataset": dataset_name,
        "source_rows": actual_rows,
        "stored_rows": stored_rows,
        "status": "SUCCESS"
    }


# ------------------------------------------------------------
# 5. INGEST ALL THREE DATASETS
# ------------------------------------------------------------

ingestion_results = []

for dataset_name, config in datasets.items():

    result = ingest_excel_to_bronze(
        dataset_name,
        config
    )

    ingestion_results.append(result)


# ------------------------------------------------------------
# 6. DISPLAY INGESTION SUMMARY
# ------------------------------------------------------------

print("\n" + "=" * 65)
print("BRONZE INGESTION SUMMARY")
print("=" * 65)

for result in ingestion_results:
    print(result)

total_rows = sum(
    result["stored_rows"]
    for result in ingestion_results
)

print("\nTotal source records:", total_rows)
print("Bronze ingestion process completed.")


# COMMAND ----------

bronze_tables = [
    "workspace.default.bronze_orders",
    "workspace.default.bronze_customers",
    "workspace.default.bronze_products"
]

for table_name in bronze_tables:
    df = spark.table(table_name)

    print("\n" + "=" * 60)
    print("TABLE:", table_name)
    print("=" * 60)

    print("Row count:", df.count())
    print("Column count:", len(df.columns))

    print("\nSchema:")
    df.printSchema()

print("\nBRONZE VERIFICATION COMPLETED")

# COMMAND ----------

print("ORDERS SAMPLE")
display(
    spark.table("workspace.default.bronze_orders")
    .select(
        "order_id",
        "customer_id",
        "product_id",
        "order_date",
        "quantity",
        "unit_price",
        "discount_pct",
        "order_status",
        "updated_at"
    )
    .limit(10)
)

print("CUSTOMERS SAMPLE")
display(
    spark.table("workspace.default.bronze_customers")
    .limit(5)
)

print("PRODUCTS SAMPLE")
display(
    spark.table("workspace.default.bronze_products")
    .limit(5)
)

# COMMAND ----------

from pyspark.sql import functions as F

orders = spark.table("workspace.default.bronze_orders")
customers = spark.table("workspace.default.bronze_customers")
products = spark.table("workspace.default.bronze_products")


# --------------------------------------------------
# 1. MISSING VALUES
# --------------------------------------------------

def profile_missing_values(df, table_name):
    print("\n" + "=" * 70)
    print("MISSING VALUE ANALYSIS:", table_name)
    print("=" * 70)

    expressions = [
        F.sum(
            F.when(
                F.col(c).isNull() | (F.trim(F.col(c)) == ""),
                1
            ).otherwise(0)
        ).alias(c)
        for c in df.columns
        if not c.startswith("_")
    ]

    df.select(*expressions).show(truncate=False)


profile_missing_values(orders, "ORDERS")
profile_missing_values(customers, "CUSTOMERS")
profile_missing_values(products, "PRODUCTS")


# --------------------------------------------------
# 2. DUPLICATE IDENTIFIER ANALYSIS
# --------------------------------------------------

def profile_duplicates(df, key, table_name):
    print("\n" + "=" * 70)
    print("DUPLICATE ANALYSIS:", table_name)
    print("=" * 70)

    duplicate_groups = (
        df.groupBy(key)
        .count()
        .filter(
            (F.col(key).isNotNull()) &
            (F.col("count") > 1)
        )
    )

    print("Duplicate", key, "groups:", duplicate_groups.count())
    print("Records belonging to duplicate groups:",
          duplicate_groups.agg(F.sum("count")).first()[0] or 0)

    duplicate_groups.orderBy(F.desc("count")).show(10)


profile_duplicates(orders, "order_id", "ORDERS")
profile_duplicates(customers, "customer_id", "CUSTOMERS")
profile_duplicates(products, "product_id", "PRODUCTS")


# --------------------------------------------------
# 3. INVALID NUMERIC VALUES IN ORDERS
# --------------------------------------------------

print("\n" + "=" * 70)
print("ORDERS NUMERIC VALIDATION")
print("=" * 70)

numeric_checks = orders.select(
    F.count(
        F.when(
            F.expr("try_cast(trim(quantity) AS INT) IS NULL"),
            1
        )
    ).alias("invalid_quantity"),

    F.count(
        F.when(
            F.expr("try_cast(trim(quantity) AS INT) <= 0"),
            1
        )
    ).alias("non_positive_quantity"),

    F.count(
        F.when(
            F.expr("try_cast(trim(unit_price) AS DOUBLE) IS NULL"),
            1
        )
    ).alias("invalid_unit_price"),

    F.count(
        F.when(
            F.expr("try_cast(trim(unit_price) AS DOUBLE) <= 0"),
            1
        )
    ).alias("non_positive_unit_price"),

    F.count(
        F.when(
            F.expr("try_cast(trim(discount_pct) AS DOUBLE) IS NULL"),
            1
        )
    ).alias("invalid_discount"),

    F.count(
        F.when(
            F.expr("""
                try_cast(trim(discount_pct) AS DOUBLE) < 0
                OR try_cast(trim(discount_pct) AS DOUBLE) > 1
            """),
            1
        )
    ).alias("discount_outside_range")
)

numeric_checks.show(truncate=False)


# --------------------------------------------------
# 4. DATE VALIDATION
# --------------------------------------------------

print("\n" + "=" * 70)
print("ORDER DATE VALIDATION")
print("=" * 70)

orders.select(
    F.count("*").alias("total_orders"),

    F.count(
        F.when(
            F.expr("try_cast(trim(order_date) AS DATE) IS NULL"),
            1
        )
    ).alias("unparseable_order_dates")
).show()

print("Sample distinct order date values:")
orders.select("order_date").distinct().show(20, truncate=False)


# --------------------------------------------------
# 5. INVALID CUSTOMER AND PRODUCT REFERENCES
# --------------------------------------------------

print("\n" + "=" * 70)
print("REFERENCE VALIDATION")
print("=" * 70)

customer_ids = customers.select("customer_id").distinct()
product_ids = products.select("product_id").distinct()

missing_customers = (
    orders.filter(
        F.col("customer_id").isNotNull() &
        (F.trim("customer_id") != "")
    )
    .join(customer_ids, "customer_id", "left_anti")
)

missing_products = (
    orders.filter(
        F.col("product_id").isNotNull() &
        (F.trim("product_id") != "")
    )
    .join(product_ids, "product_id", "left_anti")
)

print("Orders referencing unknown customers:",
      missing_customers.count())

print("Orders referencing unknown products:",
      missing_products.count())


print("\nDATA PROFILING COMPLETED")

# COMMAND ----------

from pyspark.sql import functions as F

# --------------------------------------------------
# 1. CHECK ALL KNOWN ORDER DATE FORMATS
# --------------------------------------------------

orders = spark.table("workspace.default.bronze_orders")

orders_checked = orders.withColumn(
    "parsed_order_date",
    F.expr("""
        COALESCE(
            try_to_timestamp(trim(order_date), 'yyyy-MM-dd'),
            try_to_timestamp(trim(order_date), 'dd-MMM-yyyy'),
            try_to_timestamp(trim(order_date), 'yyyy/MM/dd')
        )
    """)
)

print("=" * 70)
print("DATE FORMAT VALIDATION")
print("=" * 70)

orders_checked.select(
    F.count("*").alias("total_records"),
    F.count(
        F.when(
            F.col("order_date").isNull() |
            (F.trim("order_date") == ""),
            1
        )
    ).alias("missing_dates"),
    F.count(
        F.when(
            (F.col("order_date").isNotNull()) &
            (F.trim("order_date") != "") &
            F.col("parsed_order_date").isNull(),
            1
        )
    ).alias("non_missing_unparseable_dates")
).show(truncate=False)

print("Actual unparseable date values:")

orders_checked.filter(
    F.col("order_date").isNotNull() &
    (F.trim("order_date") != "") &
    F.col("parsed_order_date").isNull()
).select("order_date").distinct().show(50, truncate=False)


# --------------------------------------------------
# 2. INVESTIGATE CUSTOMER DUPLICATES
# --------------------------------------------------

customers = spark.table("workspace.default.bronze_customers")

customer_columns = [
    c for c in customers.columns if not c.startswith("_")
]

customer_variations = (
    customers.select(*customer_columns)
    .distinct()
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
)

print("=" * 70)
print("CUSTOMER DUPLICATE INVESTIGATION")
print("=" * 70)

print("Customer IDs with conflicting business records:",
      customer_variations.count())

customer_variations.show(truncate=False)

customers.join(
    customer_variations.select("customer_id"),
    "customer_id",
    "inner"
).orderBy("customer_id").show(30, truncate=False)


# --------------------------------------------------
# 3. INVESTIGATE PRODUCT DUPLICATES
# --------------------------------------------------

products = spark.table("workspace.default.bronze_products")

product_columns = [
    c for c in products.columns if not c.startswith("_")
]

product_variations = (
    products.select(*product_columns)
    .distinct()
    .groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
)

print("=" * 70)
print("PRODUCT DUPLICATE INVESTIGATION")
print("=" * 70)

print("Product IDs with conflicting business records:",
      product_variations.count())

product_variations.show(truncate=False)

products.join(
    product_variations.select("product_id"),
    "product_id",
    "inner"
).orderBy("product_id").show(30, truncate=False)


print("ADDITIONAL INVESTIGATION COMPLETED")

# COMMAND ----------

from pyspark.sql import functions as F

orders = spark.table("workspace.default.bronze_orders")

# Parse updated_at using supported timestamp formats
orders_versioned = orders.withColumn(
    "parsed_updated_at",
    F.coalesce(
        F.expr("try_to_timestamp(trim(updated_at))"),
        F.to_timestamp(
            F.trim(F.col("updated_at")),
            "yyyy-MM-dd'T'HH:mm:ss"
        ),
        F.to_timestamp(
            F.trim(F.col("updated_at")),
            "yyyy-MM-dd'T'HH:mm:ss.SSS"
        )
    )
)

# Identify duplicate order IDs
duplicate_ids = (
    orders_versioned
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .select("order_id")
)

duplicate_records = orders_versioned.join(
    duplicate_ids,
    "order_id",
    "inner"
)

print("=" * 65)
print("DUPLICATE ORDER VERSION SAMPLE")
print("=" * 65)

duplicate_records.select(
    "order_id",
    "source_record_id",
    "updated_at",
    "parsed_updated_at",
    "_source_row_number"
).orderBy(
    "order_id",
    "parsed_updated_at"
).show(30, truncate=False)


# Check missing or unparseable timestamps
print("=" * 65)
print("TIMESTAMP VALIDATION")
print("=" * 65)

duplicate_records.select(
    F.count("*").alias("duplicate_records"),
    F.count(
        F.when(
            F.col("parsed_updated_at").isNull(), 1
        )
    ).alias("missing_or_unparseable_updated_at")
).show()


# Identify ties at the latest timestamp
print("=" * 65)
print("LATEST TIMESTAMP TIE CHECK")
print("=" * 65)

latest_times = (
    duplicate_records
    .groupBy("order_id")
    .agg(
        F.max("parsed_updated_at").alias("latest_updated_at")
    )
)

latest_records = duplicate_records.join(
    latest_times,
    "order_id"
).filter(
    F.col("parsed_updated_at") ==
    F.col("latest_updated_at")
)

ties = (
    latest_records
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
)

print("Order IDs with tied latest timestamps:", ties.count())

ties.show(truncate=False)

print("\nORDER VERSION INVESTIGATION COMPLETED")

# COMMAND ----------

from pyspark.sql import functions as F

orders = spark.table("workspace.default.bronze_orders")
customers = spark.table("workspace.default.bronze_customers")
products = spark.table("workspace.default.bronze_products")

customer_ids = customers.select("customer_id").distinct()
product_ids = products.select("product_id").distinct()

# 1. Safely parse all supported date formats
checked = orders.withColumn(
    "parsed_order_date",
    F.coalesce(
        F.expr("try_to_date(trim(order_date), 'yyyy-MM-dd')"),
        F.expr("try_to_date(trim(order_date), 'dd-MMM-yyyy')"),
        F.expr("try_to_date(trim(order_date), 'yyyy/MM/dd')")
    )
)

# 2. Safely convert numeric columns
checked = (
    checked
    .withColumn("qty_num", F.expr("try_cast(trim(quantity) AS INT)"))
    .withColumn("price_num", F.expr("try_cast(trim(unit_price) AS DOUBLE)"))
    .withColumn("discount_num", F.expr("try_cast(trim(discount_pct) AS DOUBLE)"))
)

# 3. Check customer and product references
checked = (
    checked
    .join(
        customer_ids.withColumn("customer_exists", F.lit(True)),
        "customer_id",
        "left"
    )
    .join(
        product_ids.withColumn("product_exists", F.lit(True)),
        "product_id",
        "left"
    )
)

# 4. Generate rejection reasons
checked = checked.withColumn(
    "rejection_reasons",
    F.concat_ws(
        "; ",
        F.when(
            F.col("customer_id").isNull() |
            (F.trim("customer_id") == "") |
            F.col("customer_exists").isNull(),
            F.lit("Missing or unknown customer ID")
        ),
        F.when(
            F.col("product_id").isNull() |
            (F.trim("product_id") == "") |
            F.col("product_exists").isNull(),
            F.lit("Missing or unknown product ID")
        ),
        F.when(
            F.col("parsed_order_date").isNull(),
            F.lit("Missing or invalid order date")
        ),
        F.when(
            F.col("qty_num").isNull() | (F.col("qty_num") <= 0),
            F.lit("Invalid quantity")
        ),
        F.when(
            F.col("price_num").isNull() | (F.col("price_num") <= 0),
            F.lit("Invalid unit price")
        ),
        F.when(
            F.col("discount_num").isNull() |
            (F.col("discount_num") < 0) |
            (F.col("discount_num") > 1),
            F.lit("Invalid discount percentage")
        )
    )
)

# 5. Calculate unique affected records
rejected = checked.filter(F.col("rejection_reasons") != "")

print("TOTAL SOURCE RECORDS:", orders.count())
print("RECORDS WITH VALIDATION FAILURES:", rejected.count())
print(
    "UNIQUE AFFECTED ORDER IDs:",
    rejected.select("order_id").distinct().count()
)

# 6. Count each rejection reason
print("\nREJECTION REASON COUNTS")

reason_counts = (
    rejected
    .select(
        F.explode(
            F.split("rejection_reasons", "; ")
        ).alias("reason")
    )
    .groupBy("reason")
    .count()
    .orderBy(F.desc("count"))
)

reason_counts.show(truncate=False)

# 7. Display rejected order samples
print("\nSAMPLE REJECTED ORDERS")

rejected.select(
    "order_id",
    "customer_id",
    "product_id",
    "order_date",
    "quantity",
    "unit_price",
    "discount_pct",
    "rejection_reasons"
).show(20, truncate=False)

print("\nREJECTION IMPACT ANALYSIS COMPLETED")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

orders = spark.table("workspace.default.bronze_orders")
customers = spark.table("workspace.default.bronze_customers")
products = spark.table("workspace.default.bronze_products")

# --------------------------------------------------
# 1. PARSE UPDATED TIMESTAMP
# --------------------------------------------------

orders = orders.withColumn(
    "parsed_updated_at",
    F.expr("try_to_timestamp(trim(updated_at))")
)

# --------------------------------------------------
# 2. KEEP LATEST VERSION OF EACH ORDER
# --------------------------------------------------

order_window = (
    Window
    .partitionBy("order_id")
    .orderBy(
        F.col("parsed_updated_at").desc_nulls_last(),
        F.col("source_record_id").desc_nulls_last(),
        F.col("_source_row_number").cast("int").desc_nulls_last()
    )
)

latest_orders = (
    orders
    .withColumn("_version_rank", F.row_number().over(order_window))
    .filter(F.col("_version_rank") == 1)
    .drop("_version_rank")
)

print("SOURCE RECORDS:", orders.count())
print("LATEST ORDER RECORDS:", latest_orders.count())
print("DUPLICATE VERSIONS REMOVED:",
      orders.count() - latest_orders.count())

# --------------------------------------------------
# 3. PARSE REQUIRED FIELDS
# --------------------------------------------------

checked = latest_orders.withColumn(
    "parsed_order_date",
    F.coalesce(
        F.expr("try_to_date(trim(order_date), 'yyyy-MM-dd')"),
        F.expr("try_to_date(trim(order_date), 'dd-MMM-yyyy')"),
        F.expr("try_to_date(trim(order_date), 'yyyy/MM/dd')")
    )
).withColumn(
    "qty_num",
    F.expr("try_cast(trim(quantity) AS INT)")
).withColumn(
    "price_num",
    F.expr("try_cast(trim(unit_price) AS DOUBLE)")
).withColumn(
    "discount_num",
    F.expr("try_cast(trim(discount_pct) AS DOUBLE)")
)

# --------------------------------------------------
# 4. CHECK CUSTOMER AND PRODUCT REFERENCES
# --------------------------------------------------

customer_ids = (
    customers
    .select("customer_id")
    .filter(F.col("customer_id").isNotNull())
    .distinct()
    .withColumn("_customer_exists", F.lit(True))
)

product_ids = (
    products
    .select("product_id")
    .filter(F.col("product_id").isNotNull())
    .distinct()
    .withColumn("_product_exists", F.lit(True))
)

checked = (
    checked
    .join(customer_ids, "customer_id", "left")
    .join(product_ids, "product_id", "left")
)

# --------------------------------------------------
# 5. GENERATE ALL REJECTION REASONS
# --------------------------------------------------

checked = checked.withColumn(
    "rejection_reasons",
    F.concat_ws(
        "; ",

        F.when(
            F.col("customer_id").isNull() |
            (F.trim("customer_id") == "") |
            F.col("_customer_exists").isNull(),
            "Missing or unknown customer ID"
        ),

        F.when(
            F.col("product_id").isNull() |
            (F.trim("product_id") == "") |
            F.col("_product_exists").isNull(),
            "Missing or unknown product ID"
        ),

        F.when(
            F.col("parsed_order_date").isNull(),
            "Missing or invalid order date"
        ),

        F.when(
            F.col("qty_num").isNull() |
            (F.col("qty_num") <= 0),
            "Invalid quantity"
        ),

        F.when(
            F.col("price_num").isNull() |
            (F.col("price_num") <= 0),
            "Invalid unit price"
        ),

        F.when(
            F.col("discount_num").isNull() |
            (F.col("discount_num") < 0) |
            (F.col("discount_num") > 1),
            "Invalid discount percentage"
        )
    )
)

# --------------------------------------------------
# 6. SEPARATE VALID AND REJECTED ORDERS
# --------------------------------------------------

rejected_latest = checked.filter(
    F.col("rejection_reasons") != ""
)

valid_latest = checked.filter(
    F.col("rejection_reasons") == ""
)

print("\n" + "=" * 60)
print("LATEST-VERSION VALIDATION RESULTS")
print("=" * 60)

print("Latest order records:", latest_orders.count())
print("Valid latest orders:", valid_latest.count())
print("Rejected latest orders:", rejected_latest.count())

print(
    "Reconciliation:",
    valid_latest.count() + rejected_latest.count()
)

# --------------------------------------------------
# 7. REJECTION REASON COUNTS
# --------------------------------------------------

print("\nREJECTION REASONS AFTER DEDUPLICATION")

(
    rejected_latest
    .select(
        F.explode(
            F.split("rejection_reasons", "; ")
        ).alias("reason")
    )
    .groupBy("reason")
    .count()
    .orderBy(F.desc("count"))
    .show(truncate=False)
)

print("\nSAMPLE REJECTED LATEST ORDERS")

rejected_latest.select(
    "order_id",
    "source_record_id",
    "updated_at",
    "rejection_reasons"
).show(20, truncate=False)

print("\nLATEST-VERSION VALIDATION COMPLETED")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, DateType,
    TimestampType
)

# ============================================================
# PHILIPS HACKATHON - SILVER LAYER IMPLEMENTATION
# ============================================================

print("=" * 75)
print("PHILIPS SALES PERFORMANCE - SILVER DATA PIPELINE")
print("=" * 75)

# ------------------------------------------------------------
# STEP 1: READ BRONZE TABLES
# ------------------------------------------------------------

orders_bronze = spark.table(
    "workspace.default.bronze_orders"
)

customers_bronze = spark.table(
    "workspace.default.bronze_customers"
)

products_bronze = spark.table(
    "workspace.default.bronze_products"
)

print("\nSTEP 1: Bronze tables loaded successfully")

# ------------------------------------------------------------
# STEP 2: PARSE ORDER UPDATE TIMESTAMP
# ------------------------------------------------------------

orders = orders_bronze.withColumn(
    "parsed_updated_at",
    F.expr("try_to_timestamp(trim(updated_at))")
)

# ------------------------------------------------------------
# STEP 3: SELECT LATEST ORDER VERSION
# ------------------------------------------------------------

order_window = (
    Window
    .partitionBy("order_id")
    .orderBy(
        F.col("parsed_updated_at").desc_nulls_last(),
        F.col("source_record_id").desc_nulls_last(),
        F.col("_source_row_number").cast("int").desc_nulls_last()
    )
)

latest_orders = (
    orders
    .withColumn(
        "_version_rank",
        F.row_number().over(order_window)
    )
    .filter(F.col("_version_rank") == 1)
    .drop("_version_rank")
)

source_count = orders.count()
latest_count = latest_orders.count()
duplicate_versions_removed = source_count - latest_count

print("\nSTEP 2: LATEST ORDER VERSION SELECTION")
print("Source records:", source_count)
print("Latest order records:", latest_count)
print("Older versions removed:", duplicate_versions_removed)

# ------------------------------------------------------------
# STEP 4: STANDARDIZE DATES AND CONVERT NUMERIC COLUMNS
# ------------------------------------------------------------

typed_orders = (
    latest_orders

    .withColumn(
        "order_date",
        F.coalesce(
            F.expr(
                "try_to_date(trim(order_date), 'yyyy-MM-dd')"
            ),
            F.expr(
                "try_to_date(trim(order_date), 'dd-MMM-yyyy')"
            ),
            F.expr(
                "try_to_date(trim(order_date), 'yyyy/MM/dd')"
            )
        )
    )

    .withColumn(
        "quantity",
        F.expr("try_cast(trim(quantity) AS INT)")
    )

    .withColumn(
        "unit_price",
        F.expr("try_cast(trim(unit_price) AS DOUBLE)")
    )

    .withColumn(
        "discount_pct",
        F.expr("try_cast(trim(discount_pct) AS DOUBLE)")
    )

    .withColumn(
        "shipping_fee",
        F.expr("try_cast(trim(shipping_fee) AS DOUBLE)")
    )

    .withColumn(
        "tax_rate",
        F.expr("try_cast(trim(tax_rate) AS DOUBLE)")
    )

    .withColumn(
        "updated_at",
        F.expr("try_to_timestamp(trim(updated_at))")
    )

    .withColumn(
        "ingested_at",
        F.expr("try_to_timestamp(trim(ingested_at))")
    )

    .withColumn(
        "promised_delivery_date",
        F.coalesce(
            F.expr(
                "try_to_date(trim(promised_delivery_date), 'yyyy-MM-dd')"
            ),
            F.expr(
                "try_to_date(trim(promised_delivery_date), 'dd-MMM-yyyy')"
            ),
            F.expr(
                "try_to_date(trim(promised_delivery_date), 'yyyy/MM/dd')"
            )
        )
    )
)

print("\nSTEP 3: Order data types standardized")

# ------------------------------------------------------------
# STEP 5: PREPARE CUSTOMER AND PRODUCT DIMENSIONS
# ------------------------------------------------------------

customer_business_columns = [
    "customer_id",
    "customer_name",
    "city",
    "email",
    "signup_date",
    "country",
    "customer_segment",
    "loyalty_tier",
    "customer_status",
    "updated_at"
]

customers = (
    customers_bronze
    .select(*customer_business_columns)
    .dropDuplicates(customer_business_columns)
    .withColumn(
        "signup_date",
        F.coalesce(
            F.expr(
                "try_to_date(trim(signup_date), 'yyyy-MM-dd')"
            ),
            F.expr(
                "try_to_date(trim(signup_date), 'dd-MMM-yyyy')"
            ),
            F.expr(
                "try_to_date(trim(signup_date), 'yyyy/MM/dd')"
            )
        )
    )
    .withColumn(
        "updated_at",
        F.expr("try_to_timestamp(trim(updated_at))")
    )
)

product_business_columns = [
    "product_id",
    "product_name",
    "category",
    "list_price",
    "brand",
    "supplier_id",
    "unit_cost",
    "stock_status",
    "launch_date",
    "is_active"
]

products = (
    products_bronze
    .select(*product_business_columns)
    .dropDuplicates(product_business_columns)
    .withColumn(
        "list_price",
        F.expr("try_cast(trim(list_price) AS DOUBLE)")
    )
    .withColumn(
        "unit_cost",
        F.expr("try_cast(trim(unit_cost) AS DOUBLE)")
    )
    .withColumn(
        "launch_date",
        F.coalesce(
            F.expr(
                "try_to_date(trim(launch_date), 'yyyy-MM-dd')"
            ),
            F.expr(
                "try_to_date(trim(launch_date), 'dd-MMM-yyyy')"
            ),
            F.expr(
                "try_to_date(trim(launch_date), 'yyyy/MM/dd')"
            )
        )
    )
)

# Verify dimension uniqueness
duplicate_customer_ids = (
    customers
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

duplicate_product_ids = (
    products
    .groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicate_customer_ids > 0:
    raise ValueError(
        "Conflicting customer IDs detected. Silver write stopped."
    )

if duplicate_product_ids > 0:
    raise ValueError(
        "Conflicting product IDs detected. Silver write stopped."
    )

print("\nSTEP 4: Customer and product dimensions prepared")
print("Customer records:", customers.count())
print("Product records:", products.count())

# ------------------------------------------------------------
# STEP 6: VALIDATE CUSTOMER AND PRODUCT REFERENCES
# ------------------------------------------------------------

customer_lookup = (
    customers
    .select("customer_id")
    .filter(F.col("customer_id").isNotNull())
    .distinct()
    .withColumn("_customer_exists", F.lit(True))
)

product_lookup = (
    products
    .select("product_id")
    .filter(F.col("product_id").isNotNull())
    .distinct()
    .withColumn("_product_exists", F.lit(True))
)

checked_orders = (
    typed_orders
    .join(customer_lookup, "customer_id", "left")
    .join(product_lookup, "product_id", "left")
)

# ------------------------------------------------------------
# STEP 7: BUILD REJECTION REASONS
# ------------------------------------------------------------

checked_orders = checked_orders.withColumn(
    "rejection_reasons",
    F.concat_ws(
        "; ",

        F.when(
            F.col("customer_id").isNull() |
            (F.trim("customer_id") == "") |
            F.col("_customer_exists").isNull(),
            F.lit("Missing or unknown customer ID")
        ),

        F.when(
            F.col("product_id").isNull() |
            (F.trim("product_id") == "") |
            F.col("_product_exists").isNull(),
            F.lit("Missing or unknown product ID")
        ),

        F.when(
            F.col("order_date").isNull(),
            F.lit("Missing or invalid order date")
        ),

        F.when(
            F.col("quantity").isNull() |
            (F.col("quantity") <= 0),
            F.lit("Invalid quantity")
        ),

        F.when(
            F.col("unit_price").isNull() |
            (F.col("unit_price") <= 0),
            F.lit("Invalid unit price")
        ),

        F.when(
            F.col("discount_pct").isNull() |
            (F.col("discount_pct") < 0) |
            (F.col("discount_pct") > 1),
            F.lit("Invalid discount percentage")
        )
    )
)

# ------------------------------------------------------------
# STEP 8: SEPARATE VALID AND REJECTED ORDERS
# ------------------------------------------------------------

rejected_orders = checked_orders.filter(
    F.col("rejection_reasons") != ""
)

valid_orders = (
    checked_orders
    .filter(F.col("rejection_reasons") == "")
    .drop(
        "rejection_reasons",
        "_customer_exists",
        "_product_exists",
        "parsed_updated_at"
    )
)

# Rejected table retains diagnostic fields and original columns.
rejected_orders = rejected_orders.drop(
    "_customer_exists",
    "_product_exists",
    "parsed_updated_at"
)

# ------------------------------------------------------------
# STEP 9: DEFINE SILVER SCHEMAS
# ------------------------------------------------------------

# ------------------------------------------------------------
# STEP 10: WRITE SILVER DELTA TABLES
# ------------------------------------------------------------

valid_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.silver_orders")

rejected_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.silver_rejected_orders")

customers.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.silver_customers")

products.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.silver_products")

print("\nSTEP 5: Silver Delta tables written successfully")

# ------------------------------------------------------------
# STEP 11: POST-WRITE VERIFICATION
# ------------------------------------------------------------

silver_orders = spark.table(
    "workspace.default.silver_orders"
)

silver_rejected = spark.table(
    "workspace.default.silver_rejected_orders"
)

silver_customers = spark.table(
    "workspace.default.silver_customers"
)

silver_products = spark.table(
    "workspace.default.silver_products"
)

silver_valid_count = silver_orders.count()
silver_rejected_count = silver_rejected.count()

print("\n" + "=" * 75)
print("SILVER LAYER RECONCILIATION")
print("=" * 75)

print("Original Bronze records:", source_count)
print("Latest order versions:", latest_count)
print("Older versions removed:", duplicate_versions_removed)
print("Valid Silver orders:", silver_valid_count)
print("Rejected Silver orders:", silver_rejected_count)

print(
    "Latest-version reconciliation:",
    silver_valid_count + silver_rejected_count
)

print("\nDIMENSION TABLE COUNTS")
print("Silver customers:", silver_customers.count())
print("Silver products:", silver_products.count())

# ------------------------------------------------------------
# STEP 12: ASSERT EXPECTED RESULTS
# ------------------------------------------------------------

assert latest_count == 4817, "Unexpected latest order count"

assert silver_valid_count + silver_rejected_count == latest_count, (
    "Silver order reconciliation failed"
)

assert silver_valid_count == 4732, (
    "Valid order count differs from diagnostic result"
)

assert silver_rejected_count == 85, (
    "Rejected order count differs from diagnostic result"
)

assert duplicate_customer_ids == 0, (
    "Customer dimension contains duplicate IDs"
)

assert duplicate_product_ids == 0, (
    "Product dimension contains duplicate IDs"
)

print("\n" + "=" * 75)
print("SILVER PIPELINE COMPLETED SUCCESSFULLY")
print("=" * 75)

# COMMAND ----------

silver_tables = [
    "workspace.default.silver_orders",
    "workspace.default.silver_rejected_orders",
    "workspace.default.silver_customers",
    "workspace.default.silver_products"
]

print("=" * 70)
print("SILVER LAYER FINAL VERIFICATION")
print("=" * 70)

for table_name in silver_tables:
    df = spark.table(table_name)

    print("\nTable:", table_name)
    print("Record count:", df.count())
    print("Column count:", len(df.columns))

    print("\nSchema:")
    df.printSchema()

print("\nALL SILVER TABLES VERIFIED")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# ============================================================
# PHILIPS HACKATHON - GOLD REVENUE PIPELINE
# ============================================================

print("=" * 70)
print("GOLD LAYER - SALES REVENUE CALCULATION")
print("=" * 70)

# ------------------------------------------------------------
# STEP 1: LOAD SILVER TABLES
# ------------------------------------------------------------

orders = spark.table("workspace.default.silver_orders").alias("o")
customers = spark.table("workspace.default.silver_customers").alias("c")
products = spark.table("workspace.default.silver_products").alias("p")

# ------------------------------------------------------------
# STEP 2: CHECK DIMENSION KEY UNIQUENESS
# ------------------------------------------------------------

duplicate_customers = (
    customers.groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

duplicate_products = (
    products.groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

assert duplicate_customers == 0, "Duplicate customer IDs found"
assert duplicate_products == 0, "Duplicate product IDs found"

print("Dimension uniqueness checks passed")

# ------------------------------------------------------------
# STEP 3: JOIN ORDERS WITH CUSTOMERS AND PRODUCTS
# ------------------------------------------------------------

joined = (
    orders
    .join(
        customers,
        F.col("o.customer_id") == F.col("c.customer_id"),
        "left"
    )
    .join(
        products,
        F.col("o.product_id") == F.col("p.product_id"),
        "left"
    )
)

# ------------------------------------------------------------
# STEP 4: SELECT BUSINESS COLUMNS
# ------------------------------------------------------------

gold = joined.select(
    F.col("o.order_id"),
    F.col("o.source_record_id"),
    F.col("o.order_date"),
    F.col("o.customer_id"),
    F.col("c.customer_name"),
    F.col("c.city").alias("customer_city"),
    F.col("c.country").alias("customer_country"),
    F.col("c.customer_segment"),
    F.col("c.loyalty_tier"),

    F.col("o.product_id"),
    F.col("p.product_name"),
    F.col("p.category").alias("product_category"),
    F.col("p.brand"),
    F.col("p.list_price"),
    F.col("p.unit_cost"),

    F.col("o.quantity"),
    F.col("o.unit_price"),
    F.col("o.discount_pct"),
    F.col("o.order_status"),
    F.col("o.sales_channel"),
    F.col("o.payment_method"),
    F.col("o.promo_code"),
    F.col("o.shipping_fee"),
    F.col("o.tax_rate"),
    F.col("o.updated_at"),
    F.col("o.source_batch_id"),
    F.col("o._source_file")
)

# ------------------------------------------------------------
# STEP 5: CALCULATE BASE REVENUE
# ------------------------------------------------------------

gold = gold.withColumn(
    "base_revenue",
    (
        F.col("quantity").cast(DecimalType(18, 2)) *
        F.col("unit_price").cast(DecimalType(18, 4)) *
        (
            F.lit(1).cast(DecimalType(10, 4)) -
            F.col("discount_pct").cast(DecimalType(10, 4))
        )
    ).cast(DecimalType(18, 4))
)

# ------------------------------------------------------------
# STEP 6: CALCULATE ADDITIONAL LOYALTY DISCOUNT
# ------------------------------------------------------------

# Loyalty discount eligibility:
# Gold or Platinum + no promo code + positive unit cost.

gold = gold.withColumn(
    "_loyalty_eligible",
    (
        F.upper(F.trim(F.col("loyalty_tier"))).isin(
            "GOLD", "PLATINUM"
        )
        &
        (
            F.col("promo_code").isNull() |
            (F.trim(F.col("promo_code")) == "")
        )
        &
        F.col("unit_cost").isNotNull()
        &
        (F.col("unit_cost") > 0)
    )
)

# Calculate 2% additional discount.
gold = gold.withColumn(
    "_loyalty_discount_uncapped",
    F.col("base_revenue") *
    F.lit(0.02).cast(DecimalType(5, 4))
)

# Maximum discount allowed = Quantity x Unit Cost.
gold = gold.withColumn(
    "_loyalty_discount_cap",
    F.greatest(
        F.lit(0).cast(DecimalType(18, 4)),
        (
            F.col("quantity").cast(DecimalType(18, 2)) *
            F.col("unit_cost").cast(DecimalType(18, 4))
        )
    )
)

# Apply the cap only to eligible customers.
gold = gold.withColumn(
    "loyalty_discount",
    F.when(
        F.col("_loyalty_eligible"),
        F.least(
            F.col("_loyalty_discount_uncapped"),
            F.col("_loyalty_discount_cap")
        )
    ).otherwise(
        F.lit(0).cast(DecimalType(18, 4))
    ).cast(DecimalType(18, 4))
)

# ------------------------------------------------------------
# STEP 7: HANDLE SHIPPING CHARGES
# ------------------------------------------------------------

gold = gold.withColumn(
    "shipping_revenue",
    F.coalesce(
        F.col("shipping_fee").cast(DecimalType(18, 4)),
        F.lit(0).cast(DecimalType(18, 4))
    )
)

# ------------------------------------------------------------
# STEP 8: CALCULATE NET REVENUE
# ------------------------------------------------------------

# Only Completed and Shipped orders are revenue eligible.
gold = gold.withColumn(
    "revenue_eligible",
    F.upper(F.trim(F.col("order_status"))).isin(
        "COMPLETED", "SHIPPED"
    )
)

gold = gold.withColumn(
    "net_revenue",
    F.when(
        F.col("revenue_eligible"),
        F.round(
            F.col("base_revenue")
            - F.col("loyalty_discount")
            + F.col("shipping_revenue"),
            2
        )
    ).otherwise(
        F.lit(0).cast(DecimalType(18, 2))
    ).cast(DecimalType(18, 2))
)

# ------------------------------------------------------------
# STEP 9: REMOVE INTERNAL CALCULATION COLUMNS
# ------------------------------------------------------------

gold = gold.drop(
    "_loyalty_eligible",
    "_loyalty_discount_uncapped",
    "_loyalty_discount_cap"
)

# ------------------------------------------------------------
# STEP 10: VALIDATE GOLD DATA
# ------------------------------------------------------------

silver_order_count = spark.table(
    "workspace.default.silver_orders"
).count()

gold_count = gold.count()

unmatched_customers = gold.filter(
    F.col("customer_name").isNull()
).count()

unmatched_products = gold.filter(
    F.col("product_name").isNull()
).count()

duplicate_gold_orders = (
    gold.groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("\nGOLD VALIDATION")
print("Silver orders:", silver_order_count)
print("Gold records:", gold_count)
print("Unmatched customers:", unmatched_customers)
print("Unmatched products:", unmatched_products)
print("Duplicate Gold order IDs:", duplicate_gold_orders)

assert gold_count == silver_order_count
assert unmatched_customers == 0
assert unmatched_products == 0
assert duplicate_gold_orders == 0

# ------------------------------------------------------------
# STEP 11: WRITE GOLD DELTA TABLE
# ------------------------------------------------------------

gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_sales")

# ------------------------------------------------------------
# STEP 12: VERIFY SAVED GOLD TABLE
# ------------------------------------------------------------

gold_saved = spark.table("workspace.default.gold_sales")

print("\n" + "=" * 70)
print("GOLD TABLE RECONCILIATION")
print("=" * 70)

print("Gold table: workspace.default.gold_sales")
print("Total records:", gold_saved.count())
print("Total columns:", len(gold_saved.columns))

print("\nRevenue by order status:")

gold_saved.groupBy("order_status").agg(
    F.count("*").alias("order_count"),
    F.sum("net_revenue").alias("total_net_revenue")
).orderBy("order_status").show(truncate=False)

print("\nSample Gold records:")

gold_saved.select(
    "order_id",
    "customer_name",
    "product_name",
    "product_category",
    "quantity",
    "base_revenue",
    "loyalty_discount",
    "shipping_revenue",
    "order_status",
    "net_revenue"
).show(10, truncate=False)

assert gold_saved.count() == 4732

print("\n" + "=" * 70)
print("GOLD REVENUE PIPELINE COMPLETED SUCCESSFULLY")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer – Business Analytics and Reporting
# MAGIC
# MAGIC **Objective:** Generate business reports from the validated Gold sales table.
# MAGIC
# MAGIC The reports will help Philips understand sales performance, customer contribution, product category performance, order status distribution, and pricing differences.
# MAGIC
# MAGIC All reports use the Gold table so that revenue follows the approved business rules.

# COMMAND ----------

from pyspark.sql import functions as F

gold = spark.table("workspace.default.gold_sales")

# ============================================================
# REPORT 1: DAILY REVENUE
# ============================================================

print("=" * 70)
print("REPORT 1: DAILY REVENUE")
print("=" * 70)

daily_revenue = (
    gold
    .groupBy("order_date")
    .agg(
        F.count("*").alias("total_orders"),
        F.sum("net_revenue").alias("daily_revenue")
    )
    .orderBy("order_date")
)

display(daily_revenue)


# ============================================================
# REPORT 2: CATEGORY-WISE REVENUE
# ============================================================

print("=" * 70)
print("REPORT 2: CATEGORY-WISE REVENUE")
print("=" * 70)

category_revenue = (
    gold
    .groupBy("product_category")
    .agg(
        F.count("*").alias("total_orders"),
        F.sum("net_revenue").alias("total_revenue")
    )
    .orderBy(F.desc("total_revenue"))
)

display(category_revenue)


# ============================================================
# REPORT 3: TOP 10 CUSTOMERS
# ============================================================

print("=" * 70)
print("REPORT 3: TOP 10 CUSTOMERS")
print("=" * 70)

top_customers = (
    gold
    .groupBy(
        "customer_id",
        "customer_name",
        "customer_city"
    )
    .agg(
        F.count("*").alias("total_orders"),
        F.sum("net_revenue").alias("total_revenue")
    )
    .orderBy(F.desc("total_revenue"))
    .limit(10)
)

display(top_customers)


# ============================================================
# REPORT 4: ORDERS BY STATUS
# ============================================================

print("=" * 70)
print("REPORT 4: ORDERS BY STATUS")
print("=" * 70)

orders_by_status = (
    gold
    .groupBy("order_status")
    .agg(
        F.count("*").alias("total_orders"),
        F.sum("net_revenue").alias("total_revenue")
    )
    .orderBy(F.desc("total_orders"))
)

display(orders_by_status)


# ============================================================
# REPORT 5: ORDER PRICE VS PRODUCT LIST PRICE
# ============================================================

print("=" * 70)
print("REPORT 5: ORDER PRICE VS PRODUCT LIST PRICE")
print("=" * 70)

price_comparison = (
    gold
    .groupBy("product_id", "product_name", "product_category")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_order_unit_price"),
        F.round(F.avg("list_price"), 2).alias("product_list_price"),
        F.count("*").alias("total_orders")
    )
    .withColumn(
        "price_difference",
        F.round(
            F.col("avg_order_unit_price") -
            F.col("product_list_price"),
            2
        )
    )
    .orderBy("product_id")
)

display(price_comparison)


print("=" * 70)
print("FIRST FIVE GOLD REPORTS COMPLETED")
print("=" * 70)

# COMMAND ----------

from pyspark.sql import functions as F

gold = spark.table("workspace.default.gold_sales")

# Consider only Completed and Shipped orders
eligible_orders = gold.filter(
    F.col("revenue_eligible") == True
)

# Group by loyalty tier and product category
promo_report = eligible_orders.groupBy(
    F.coalesce(F.col("loyalty_tier"), F.lit("Unknown")).alias("loyalty_tier"),
    F.coalesce(F.col("product_category"), F.lit("Unknown")).alias("product_category")
).agg(
    F.count("*").alias("total_eligible_orders"),

    F.sum(
        F.when(
            F.col("promo_code").isNotNull() &
            (F.trim(F.col("promo_code")) != ""),
            1
        ).otherwise(0)
    ).alias("promo_orders")
)

# Calculate percentage
promo_report = promo_report.withColumn(
    "promo_usage_percentage",
    F.round(
        F.col("promo_orders") * 100 /
        F.col("total_eligible_orders"),
        2
    )
).orderBy("loyalty_tier", "product_category")

display(promo_report)

# Save as a Gold reporting table
promo_report.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_promo_usage_report")

# COMMAND ----------

from pyspark.sql import functions as F

gold = spark.table("workspace.default.gold_sales")

# Only revenue-eligible orders
eligible_orders = gold.filter(
    F.col("revenue_eligible") == True
)

# Group by customer city and product category
shipping_report = eligible_orders.groupBy(
    F.coalesce(F.col("customer_city"), F.lit("Unknown")).alias("customer_city"),
    F.coalesce(F.col("product_category"), F.lit("Unknown")).alias("product_category")
).agg(
    F.sum("shipping_revenue").alias("total_shipping_fees"),
    F.sum("net_revenue").alias("total_net_revenue"),
    F.count("*").alias("total_orders")
)

# Calculate shipping contribution percentage
shipping_report = shipping_report.withColumn(
    "shipping_contribution_percentage",
    F.when(
        F.col("total_net_revenue") != 0,
        F.round(
            F.col("total_shipping_fees") * 100 /
            F.col("total_net_revenue"),
            2
        )
    ).otherwise(0)
).orderBy("customer_city", "product_category")

display(shipping_report)

# Save Gold report
shipping_report.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_shipping_contribution_report")

# COMMAND ----------

from pyspark.sql import functions as F

gold = spark.table("workspace.default.gold_sales")

# Expected totals from revenue-eligible orders
expected = gold.filter(
    F.col("revenue_eligible") == True
).agg(
    F.count("*").alias("expected_orders"),
    F.sum("shipping_revenue").alias("expected_shipping"),
    F.sum("net_revenue").alias("expected_net_revenue")
)

# Actual totals from the saved report
report = spark.table(
    "workspace.default.gold_shipping_contribution_report"
)

actual = report.agg(
    F.sum("total_orders").alias("report_orders"),
    F.sum("total_shipping_fees").alias("report_shipping"),
    F.sum("total_net_revenue").alias("report_net_revenue")
)

display(expected.crossJoin(actual))

# COMMAND ----------

# Create a separate test table from Gold
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.gold_sales_upsert_test
USING DELTA
AS
SELECT *
FROM workspace.default.gold_sales
""")

# Verify the copied table
test_count = spark.table(
    "workspace.default.gold_sales_upsert_test"
).count()

original_count = spark.table(
    "workspace.default.gold_sales"
).count()

print("Original Gold rows:", original_count)
print("Test table rows:", test_count)

assert original_count == test_count
print("SUCCESS: Test table created and verified!")

# COMMAND ----------

spark.sql("""
DESCRIBE DETAIL workspace.default.gold_sales
""").select(
    "format",
    "name",
    "numFiles"
).display()

# COMMAND ----------

from pyspark.sql import functions as F

test = spark.table("workspace.default.gold_sales_upsert_test")

# Select one existing order
existing = test.filter(
    F.col("order_id") == "O01002"
)

# Confirm that the order exists exactly once
assert existing.count() == 1, "O01002 must exist exactly once"

# Simulate a newer version of the existing order
updated_order = (
    existing
    .withColumn("sales_channel", F.lit("UPSERT_TEST_UPDATED"))
    .withColumn(
        "updated_at",
        F.expr("updated_at + INTERVAL 1 DAY")
    )
)

# Simulate a completely new order
new_order = (
    existing
    .withColumn("order_id", F.lit("TEST_NEW_ORDER_001"))
    .withColumn("source_record_id", F.lit("TEST_NEW_RECORD_001"))
    .withColumn("sales_channel", F.lit("UPSERT_TEST_NEW"))
    .withColumn(
        "updated_at",
        F.expr("updated_at + INTERVAL 1 DAY")
    )
)

# Combine both records into one incoming batch
incoming_batch = updated_order.unionByName(new_order)

# Create a temporary view for the MERGE operation
incoming_batch.createOrReplaceTempView("incoming_gold_batch")

print("Incoming records:", incoming_batch.count())

display(
    incoming_batch.select(
        "order_id",
        "source_record_id",
        "sales_channel",
        "updated_at",
        "net_revenue"
    )
)

# COMMAND ----------

spark.sql("""
MERGE INTO workspace.default.gold_sales_upsert_test AS target
USING incoming_gold_batch AS source

ON target.order_id = source.order_id

WHEN MATCHED
 AND source.updated_at > target.updated_at
THEN UPDATE SET *

WHEN NOT MATCHED
THEN INSERT *
""")

print("Incremental MERGE completed successfully!")

# COMMAND ----------

result = spark.table(
    "workspace.default.gold_sales_upsert_test"
)

print("Total rows after MERGE:", result.count())

display(
    result.filter(
        F.col("order_id").isin(
            "O01002",
            "TEST_NEW_ORDER_001"
        )
    ).select(
        "order_id",
        "source_record_id",
        "sales_channel",
        "updated_at",
        "net_revenue"
    )
)

# COMMAND ----------

# Record row count before rerunning MERGE
before_count = spark.table(
    "workspace.default.gold_sales_upsert_test"
).count()

# Run the exact same incoming batch again
spark.sql("""
MERGE INTO workspace.default.gold_sales_upsert_test AS target
USING incoming_gold_batch AS source

ON target.order_id = source.order_id

WHEN MATCHED
 AND source.updated_at > target.updated_at
THEN UPDATE SET *

WHEN NOT MATCHED
THEN INSERT *
""")

# Record row count after rerunning MERGE
after_count = spark.table(
    "workspace.default.gold_sales_upsert_test"
).count()

print("Rows before rerun:", before_count)
print("Rows after rerun:", after_count)

assert before_count == after_count, "Idempotency test failed!"

print("SUCCESS: Idempotency verified!")

# COMMAND ----------

# Get the current version of O01002
current = spark.table(
    "workspace.default.gold_sales_upsert_test"
).filter(
    F.col("order_id") == "O01002"
)

# Create an older incoming version
older_batch = (
    current
    .withColumn(
        "updated_at",
        F.expr("updated_at - INTERVAL 2 DAYS")
    )
    .withColumn(
        "sales_channel",
        F.lit("OLD_VERSION_TEST")
    )
)

older_batch.createOrReplaceTempView("older_incoming_batch")

# Perform MERGE
spark.sql("""
MERGE INTO workspace.default.gold_sales_upsert_test AS target
USING older_incoming_batch AS source
ON target.order_id = source.order_id

WHEN MATCHED
 AND source.updated_at > target.updated_at
THEN UPDATE SET *

WHEN NOT MATCHED
THEN INSERT *
""")

# Verify that the latest record remains unchanged
display(
    spark.table("workspace.default.gold_sales_upsert_test")
    .filter(F.col("order_id") == "O01002")
    .select("order_id", "sales_channel", "updated_at")
)

# COMMAND ----------

# Inspect the actual Bronze and Silver tables
bronze_orders = spark.table("workspace.default.bronze_orders")
silver_orders = spark.table("workspace.default.silver_orders")

print("========== BRONZE ORDERS ==========")
print("Rows:", bronze_orders.count())
bronze_orders.printSchema()

print("\n========== SILVER ORDERS ==========")
print("Rows:", silver_orders.count())
silver_orders.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Read actual Bronze and Silver dimension tables
bronze = spark.table("workspace.default.bronze_orders")
customers = spark.table("workspace.default.silver_customers")
products = spark.table("workspace.default.silver_products")

# 2. Convert raw Bronze values into Silver data types
parsed = bronze.select(
    "source_record_id",
    "order_id",
    "customer_id",
    "product_id",

    F.expr("""
        coalesce(
            try_to_date(trim(order_date), 'yyyy-MM-dd'),
            try_to_date(trim(order_date), 'dd-MMM-yyyy'),
            try_to_date(trim(order_date), 'yyyy/MM/dd')
        )
    """).alias("order_date"),

    F.expr("try_cast(trim(quantity) AS INT)").alias("quantity"),
    F.expr("try_cast(trim(unit_price) AS DOUBLE)").alias("unit_price"),
    F.expr("try_cast(trim(discount_pct) AS DOUBLE)").alias("discount_pct"),

    "order_status",

    F.expr("try_to_timestamp(trim(updated_at))").alias("updated_at"),

    "sales_channel",
    "payment_method",

    F.expr("try_cast(trim(shipping_fee) AS DOUBLE)").alias("shipping_fee"),
    F.expr("try_cast(trim(tax_rate) AS DOUBLE)").alias("tax_rate"),

    "promo_code",

    F.expr("""
        coalesce(
            try_to_date(trim(promised_delivery_date), 'yyyy-MM-dd'),
            try_to_date(trim(promised_delivery_date), 'dd-MMM-yyyy'),
            try_to_date(trim(promised_delivery_date), 'yyyy/MM/dd')
        )
    """).alias("promised_delivery_date"),

    F.expr("try_to_timestamp(trim(ingested_at))").alias("ingested_at"),

    "source_schema_version",
    "source_batch_id",
    "_source_file",
    "_source_sheet",
    "_ingested_at_utc",
    "_source_file_sha256",
    "_source_row_number"
)

# 3. Select the latest version of each order
window = Window.partitionBy("order_id").orderBy(
    F.col("updated_at").desc_nulls_last()
)

latest = (
    parsed
    .withColumn("_version_rank", F.row_number().over(window))
    .filter(F.col("_version_rank") == 1)
    .drop("_version_rank")
)

# 4. Check customer and product references
checked = (
    latest.alias("o")
    .join(
        customers.select("customer_id").alias("c"),
        F.col("o.customer_id") == F.col("c.customer_id"),
        "left"
    )
    .join(
        products.select("product_id").alias("p"),
        F.col("o.product_id") == F.col("p.product_id"),
        "left"
    )
    .select(
        "o.*",
        F.col("c.customer_id").alias("_matched_customer"),
        F.col("p.product_id").alias("_matched_product")
    )
)

# 5. Apply validation rules
checked = checked.withColumn(
    "rejection_reasons",
    F.concat_ws(
        "; ",
        F.when(
            F.col("order_id").isNull() | (F.trim("order_id") == ""),
            "Missing order ID"
        ),
        F.when(
            F.col("order_date").isNull(),
            "Missing or invalid order date"
        ),
        F.when(
            F.col("quantity").isNull() | (F.col("quantity") <= 0),
            "Invalid quantity"
        ),
        F.when(
            F.col("unit_price").isNull() | (F.col("unit_price") <= 0),
            "Invalid unit price"
        ),
        F.when(
            F.col("discount_pct").isNull() |
            (F.col("discount_pct") < 0) |
            (F.col("discount_pct") > 1),
            "Invalid discount"
        ),
        F.when(
            F.col("_matched_customer").isNull(),
            "Missing or unknown customer ID"
        ),
        F.when(
            F.col("_matched_product").isNull(),
            "Missing or unknown product ID"
        )
    )
)

# 6. Separate valid and rejected latest records
incoming_valid = (
    checked
    .filter(F.col("rejection_reasons") == "")
    .drop(
        "rejection_reasons",
        "_matched_customer",
        "_matched_product"
    )
)

incoming_rejected = checked.filter(
    F.col("rejection_reasons") != ""
)

# 7. Create temporary views for the next step
incoming_valid.createOrReplaceTempView("production_incoming_valid")
incoming_rejected.createOrReplaceTempView("production_incoming_rejected")

print("Bronze source rows:", bronze.count())
print("Latest order versions:", latest.count())
print("Valid incoming orders:", incoming_valid.count())
print("Rejected incoming orders:", incoming_rejected.count())

display(
    incoming_rejected.groupBy("rejection_reasons")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

from pyspark.sql import functions as F

silver = spark.table("workspace.default.silver_orders")
incoming = spark.table("production_incoming_valid")

# Check that both datasets have the same columns
assert set(silver.columns) == set(incoming.columns), \
    "Column mismatch between incoming data and Silver!"

# Align incoming columns to the Silver table order
incoming_aligned = incoming.select(*silver.columns)

# Check data types
print("Silver schema matches incoming:",
      silver.schema == incoming_aligned.schema)

# Compare records in both directions
missing_from_incoming = silver.exceptAll(incoming_aligned)
extra_in_incoming = incoming_aligned.exceptAll(silver)

missing_count = missing_from_incoming.count()
extra_count = extra_in_incoming.count()

print("Silver records missing from incoming:", missing_count)
print("Extra incoming records:", extra_count)

print("Existing Silver rows:", silver.count())
print("Incoming valid rows:", incoming_aligned.count())

if missing_count == 0 and extra_count == 0:
    print("SUCCESS: Incoming batch exactly matches production Silver!")
else:
    print("WARNING: Differences found. Do not MERGE yet.")

# COMMAND ----------

# Record the production count before MERGE
before_count = spark.table(
    "workspace.default.silver_orders"
).count()

# Incremental upsert into the actual Silver table
spark.sql("""
MERGE INTO workspace.default.silver_orders AS target
USING production_incoming_valid AS source

ON target.order_id = source.order_id

WHEN MATCHED
 AND source.updated_at > target.updated_at
THEN UPDATE SET *

WHEN NOT MATCHED
THEN INSERT *
""")

# Verify the result
after_count = spark.table(
    "workspace.default.silver_orders"
).count()

print("Rows before MERGE:", before_count)
print("Rows after MERGE:", after_count)

assert before_count == after_count == 4732

print("SUCCESS: Production Silver MERGE completed!")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# 1. Read production Silver tables
orders = spark.table("workspace.default.silver_orders")
customers = spark.table("workspace.default.silver_customers")
products = spark.table("workspace.default.silver_products")

# 2. Verify dimension uniqueness before joining
assert customers.groupBy("customer_id").count().filter(
    F.col("count") > 1
).count() == 0, "Duplicate customer IDs found!"

assert products.groupBy("product_id").count().filter(
    F.col("count") > 1
).count() == 0, "Duplicate product IDs found!"

# 3. Join Orders with Customers and Products
joined = (
    orders.alias("o")
    .join(
        customers.alias("c"),
        F.col("o.customer_id") == F.col("c.customer_id"),
        "left"
    )
    .join(
        products.alias("p"),
        F.col("o.product_id") == F.col("p.product_id"),
        "left"
    )
    .select(
        "o.order_id",
        "o.source_record_id",
        "o.order_date",
        "o.customer_id",
        F.col("c.customer_name").alias("customer_name"),
        F.col("c.city").alias("customer_city"),
        F.col("c.country").alias("customer_country"),
        F.col("c.customer_segment").alias("customer_segment"),
        F.col("c.loyalty_tier").alias("loyalty_tier"),
        "o.product_id",
        F.col("p.product_name").alias("product_name"),
        F.col("p.category").alias("product_category"),
        F.col("p.brand").alias("brand"),
        F.col("p.list_price").alias("list_price"),
        F.col("p.unit_cost").alias("unit_cost"),
        "o.quantity",
        "o.unit_price",
        "o.discount_pct",
        "o.order_status",
        "o.sales_channel",
        "o.payment_method",
        "o.promo_code",
        "o.shipping_fee",
        "o.tax_rate",
        "o.updated_at",
        "o.source_batch_id",
        "o._source_file"
    )
)

# 4. Calculate base revenue using decimal arithmetic
gold = joined.withColumn(
    "base_revenue",
    (
        F.col("quantity").cast(DecimalType(18, 4)) *
        F.col("unit_price").cast(DecimalType(18, 4)) *
        (
            F.lit(1).cast(DecimalType(18, 4)) -
            F.col("discount_pct").cast(DecimalType(18, 4))
        )
    ).cast(DecimalType(18, 4))
)

# 5. Calculate additional loyalty discount
loyalty_eligible = (
    F.upper(F.trim(F.col("loyalty_tier"))).isin("GOLD", "PLATINUM") &
    (
        F.col("promo_code").isNull() |
        (F.trim(F.col("promo_code")) == "")
    ) &
    F.col("unit_cost").isNotNull() &
    (F.col("unit_cost") > 0)
)

potential_loyalty_discount = (
    F.col("base_revenue") *
    F.lit(0.02)
)

maximum_cost_discount = F.greatest(
    F.lit(0).cast(DecimalType(18, 4)),
    (
        F.col("quantity").cast(DecimalType(18, 4)) *
        F.col("unit_cost").cast(DecimalType(18, 4))
    )
)

gold = gold.withColumn(
    "loyalty_discount",
    F.when(
        loyalty_eligible,
        F.least(potential_loyalty_discount, maximum_cost_discount)
    ).otherwise(
        F.lit(0).cast(DecimalType(18, 4))
    ).cast(DecimalType(18, 4))
)

# 6. Shipping charges: missing shipping fee = zero
gold = gold.withColumn(
    "shipping_revenue",
    F.coalesce(
        F.col("shipping_fee").cast(DecimalType(18, 4)),
        F.lit(0).cast(DecimalType(18, 4))
    )
)

# 7. Identify revenue-eligible orders
gold = gold.withColumn(
    "revenue_eligible",
    F.upper(F.trim(F.col("order_status"))).isin(
        "COMPLETED", "SHIPPED"
    )
)

# 8. Calculate final net revenue
gold = gold.withColumn(
    "net_revenue",
    F.when(
        F.col("revenue_eligible"),
        F.round(
            F.col("base_revenue")
            - F.col("loyalty_discount")
            + F.col("shipping_revenue"),
            2
        )
    ).otherwise(
        F.lit(0).cast(DecimalType(18, 2))
    ).cast(DecimalType(18, 2))
)

# 9. Verify Gold row count before saving
assert gold.count() == orders.count(), \
    "Gold row count does not match Silver Orders!"

assert gold.select("order_id").distinct().count() == gold.count(), \
    "Duplicate order IDs found in Gold!"

# 10. Save production Gold table
gold.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_sales")

print("SUCCESS: Gold table rebuilt from production Silver!")

# 11. Verify final result
final_gold = spark.table("workspace.default.gold_sales")

print("Silver orders:", orders.count())
print("Gold orders:", final_gold.count())

final_gold.groupBy("order_status").agg(
    F.count("*").alias("total_orders"),
    F.sum("net_revenue").alias("total_net_revenue")
).orderBy("order_status").show()

# COMMAND ----------

from pyspark.sql import functions as F

# Read refreshed production Gold table
gold = spark.table("workspace.default.gold_sales")


# ============================================================
# REPORT 1: DAILY REVENUE
# ============================================================

print("=" * 70)
print("REPORT 1: DAILY REVENUE")
print("=" * 70)

daily_revenue = (
    gold
    .groupBy("order_date")
    .agg(
        F.count("*").alias("total_orders"),
        F.sum("net_revenue").alias("daily_revenue")
    )
    .orderBy("order_date")
)

display(daily_revenue)


# ============================================================
# REPORT 2: CATEGORY-WISE REVENUE
# ============================================================

print("=" * 70)
print("REPORT 2: CATEGORY-WISE REVENUE")
print("=" * 70)

category_revenue = (
    gold
    .groupBy("product_category")
    .agg(
        F.count("*").alias("total_orders"),
        F.sum("net_revenue").alias("total_revenue")
    )
    .orderBy(F.desc("total_revenue"))
)

display(category_revenue)


# ============================================================
# REPORT 3: TOP 10 CUSTOMERS
# ============================================================

print("=" * 70)
print("REPORT 3: TOP 10 CUSTOMERS")
print("=" * 70)

top_customers = (
    gold
    .groupBy(
        "customer_id",
        "customer_name",
        "customer_city"
    )
    .agg(
        F.count("*").alias("total_orders"),
        F.sum("net_revenue").alias("total_revenue")
    )
    .orderBy(F.desc("total_revenue"))
    .limit(10)
)

display(top_customers)


# ============================================================
# REPORT 4: ORDERS BY STATUS
# ============================================================

print("=" * 70)
print("REPORT 4: ORDERS BY STATUS")
print("=" * 70)

orders_by_status = (
    gold
    .groupBy("order_status")
    .agg(
        F.count("*").alias("total_orders"),
        F.sum("net_revenue").alias("total_revenue")
    )
    .orderBy(F.desc("total_orders"))
)

display(orders_by_status)


# ============================================================
# REPORT 5: ORDER PRICE VS PRODUCT LIST PRICE
# ============================================================

print("=" * 70)
print("REPORT 5: ORDER PRICE VS PRODUCT LIST PRICE")
print("=" * 70)

price_comparison = (
    gold
    .groupBy(
        "product_id",
        "product_name",
        "product_category"
    )
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_order_unit_price"),
        F.round(F.avg("list_price"), 2).alias("product_list_price"),
        F.count("*").alias("total_orders")
    )
    .withColumn(
        "price_difference",
        F.round(
            F.col("avg_order_unit_price") -
            F.col("product_list_price"),
            2
        )
    )
    .orderBy("product_id")
)

display(price_comparison)


# ============================================================
# SAVE ALL FIVE REPORTS AS GOLD DELTA TABLES
# ============================================================

reports = {
    "gold_daily_revenue_report": daily_revenue,
    "gold_category_revenue_report": category_revenue,
    "gold_top_customers_report": top_customers,
    "gold_orders_by_status_report": orders_by_status,
    "gold_price_comparison_report": price_comparison
}

for table_name, report_df in reports.items():
    report_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"workspace.default.{table_name}")

    print(f"Saved: workspace.default.{table_name}")


print("=" * 70)
print("ALL FIVE GOLD REPORTS GENERATED AND SAVED SUCCESSFULLY")
print("=" * 70)

# COMMAND ----------

from pyspark.sql import functions as F

gold = spark.table("workspace.default.gold_sales")

eligible = gold.filter(
    F.col("revenue_eligible") == True
)

# REPORT 6: PROMO-CODE USAGE
promo_report = eligible.groupBy(
    F.coalesce(F.col("loyalty_tier"), F.lit("Unknown")).alias("loyalty_tier"),
    F.coalesce(F.col("product_category"), F.lit("Unknown")).alias("product_category")
).agg(
    F.count("*").alias("total_eligible_orders"),
    F.sum(
        F.when(
            F.col("promo_code").isNotNull() &
            (F.trim(F.col("promo_code")) != ""),
            1
        ).otherwise(0)
    ).alias("promo_orders")
).withColumn(
    "promo_usage_percentage",
    F.round(
        F.col("promo_orders") * 100 /
        F.col("total_eligible_orders"),
        2
    )
)

promo_report.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_promo_usage_report")


# REPORT 7: SHIPPING CONTRIBUTION
shipping_report = eligible.groupBy(
    F.coalesce(F.col("customer_city"), F.lit("Unknown")).alias("customer_city"),
    F.coalesce(F.col("product_category"), F.lit("Unknown")).alias("product_category")
).agg(
    F.sum("shipping_revenue").alias("total_shipping_fees"),
    F.sum("net_revenue").alias("total_net_revenue"),
    F.count("*").alias("total_orders")
).withColumn(
    "shipping_contribution_percentage",
    F.when(
        F.col("total_net_revenue") != 0,
        F.round(
            F.col("total_shipping_fees") * 100 /
            F.col("total_net_revenue"),
            2
        )
    ).otherwise(0)
)

shipping_report.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_shipping_contribution_report")

print("Both analytical reports refreshed and saved successfully!")
print("Promo report rows:", promo_report.count())
print("Shipping report rows:", shipping_report.count())

# COMMAND ----------

from pyspark.sql import functions as F

tables = [
    "bronze_orders",
    "bronze_customers",
    "bronze_products",
    "silver_orders",
    "silver_rejected_orders",
    "silver_customers",
    "silver_products",
    "gold_sales",
    "gold_daily_revenue_report",
    "gold_category_revenue_report",
    "gold_top_customers_report",
    "gold_orders_by_status_report",
    "gold_price_comparison_report",
    "gold_promo_usage_report",
    "gold_shipping_contribution_report"
]

print("=" * 65)
print("PHILIPS HACKATHON - FINAL TABLE VALIDATION")
print("=" * 65)

for table_name in tables:
    full_name = f"workspace.default.{table_name}"
    df = spark.table(full_name)

    print(f"{table_name}: {df.count()} rows")

# Validate key business totals
gold = spark.table("workspace.default.gold_sales")

summary = gold.agg(
    F.count("*").alias("gold_orders"),
    F.countDistinct("order_id").alias("unique_orders"),
    F.sum("net_revenue").alias("total_net_revenue"),
    F.sum(
        F.when(F.col("revenue_eligible"), 1).otherwise(0)
    ).alias("eligible_orders")
)

display(summary)

# Reconciliation
assert gold.count() == 4732
assert gold.select("order_id").distinct().count() == 4732

print("SUCCESS: Final Gold validation passed!")

# COMMAND ----------

from pyspark.sql import functions as F

# Read the actual Bronze and production Silver tables
bronze = spark.table("workspace.default.bronze_orders")
silver = spark.table("workspace.default.silver_orders")

# Find the latest raw Bronze version of O01002
base = (
    bronze
    .filter(F.col("order_id") == "O01002")
    .withColumn(
        "_parsed_update",
        F.expr("try_to_timestamp(updated_at)")
    )
    .orderBy(F.col("_parsed_update").desc_nulls_last())
    .limit(1)
    .drop("_parsed_update")
)

assert base.count() == 1, "Could not find O01002 in Bronze"

# Ensure the new demo order ID does not already exist
assert silver.filter(
    F.col("order_id") == "HACKATHON_DEMO_NEW_001"
).count() == 0, "Demo order already exists!"

# Generate a newer timestamp for both incoming records
new_timestamp = F.date_format(
    F.current_timestamp() + F.expr("INTERVAL 1 DAY"),
    "yyyy-MM-dd HH:mm:ss"
)

# Incoming version of existing order
updated_record = (
    base
    .withColumn("source_record_id", F.lit("DEMO_UPDATE_001"))
    .withColumn("updated_at", new_timestamp)
    .withColumn("sales_channel", F.lit("DEMO_INCREMENTAL_UPDATE"))
    .withColumn("source_batch_id", F.lit("DEMO_BATCH_001"))
)

# Incoming new order
new_record = (
    base
    .withColumn(
        "source_record_id",
        F.lit("DEMO_NEW_RECORD_001")
    )
    .withColumn(
        "order_id",
        F.lit("HACKATHON_DEMO_NEW_001")
    )
    .withColumn("updated_at", new_timestamp)
    .withColumn("sales_channel", F.lit("DEMO_NEW_ORDER"))
    .withColumn("source_batch_id", F.lit("DEMO_BATCH_001"))
)

# Combine both records
demo_batch = updated_record.unionByName(new_record)

# Save as a separate Bronze staging table
demo_batch.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.bronze_orders_incremental_demo")

print("Incoming batch created successfully!")
print("Incoming records:", demo_batch.count())

display(
    demo_batch.select(
        "order_id",
        "source_record_id",
        "updated_at",
        "sales_channel",
        "source_batch_id"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

# Read the incoming Bronze staging table
incoming_bronze = spark.table(
    "workspace.default.bronze_orders_incremental_demo"
)

# Convert raw values into Silver data types
incoming_silver = incoming_bronze.select(
    "source_record_id",
    "order_id",
    "customer_id",
    "product_id",

    F.expr("""
        coalesce(
            try_to_date(trim(order_date), 'yyyy-MM-dd'),
            try_to_date(trim(order_date), 'dd-MMM-yyyy'),
            try_to_date(trim(order_date), 'yyyy/MM/dd')
        )
    """).alias("order_date"),

    F.expr("try_cast(trim(quantity) AS INT)").alias("quantity"),
    F.expr("try_cast(trim(unit_price) AS DOUBLE)").alias("unit_price"),
    F.expr("try_cast(trim(discount_pct) AS DOUBLE)").alias("discount_pct"),

    "order_status",

    F.expr("try_to_timestamp(trim(updated_at))").alias("updated_at"),

    "sales_channel",
    "payment_method",

    F.expr("try_cast(trim(shipping_fee) AS DOUBLE)").alias("shipping_fee"),
    F.expr("try_cast(trim(tax_rate) AS DOUBLE)").alias("tax_rate"),

    "promo_code",

    F.expr("""
        coalesce(
            try_to_date(trim(promised_delivery_date), 'yyyy-MM-dd'),
            try_to_date(trim(promised_delivery_date), 'dd-MMM-yyyy'),
            try_to_date(trim(promised_delivery_date), 'yyyy/MM/dd')
        )
    """).alias("promised_delivery_date"),

    F.expr("try_to_timestamp(trim(ingested_at))").alias("ingested_at"),

    "source_schema_version",
    "source_batch_id",
    "_source_file",
    "_source_sheet",
    "_ingested_at_utc",
    "_source_file_sha256",
    "_source_row_number"
)

# Validate incoming records
customers = spark.table("workspace.default.silver_customers")
products = spark.table("workspace.default.silver_products")

incoming_checked = (
    incoming_silver.alias("o")
    .join(
        customers.select("customer_id").alias("c"),
        F.col("o.customer_id") == F.col("c.customer_id"),
        "left"
    )
    .join(
        products.select("product_id").alias("p"),
        F.col("o.product_id") == F.col("p.product_id"),
        "left"
    )
    .select(
        "o.*",
        F.col("c.customer_id").alias("_matched_customer"),
        F.col("p.product_id").alias("_matched_product")
    )
)

incoming_checked = incoming_checked.withColumn(
    "rejection_reasons",
    F.concat_ws(
        "; ",
        F.when(
            F.col("order_id").isNull() | (F.trim("order_id") == ""),
            "Missing order ID"
        ),
        F.when(F.col("order_date").isNull(), "Invalid order date"),
        F.when(
            F.col("quantity").isNull() | (F.col("quantity") <= 0),
            "Invalid quantity"
        ),
        F.when(
            F.col("unit_price").isNull() | (F.col("unit_price") <= 0),
            "Invalid unit price"
        ),
        F.when(
            F.col("discount_pct").isNull() |
            (F.col("discount_pct") < 0) |
            (F.col("discount_pct") > 1),
            "Invalid discount"
        ),
        F.when(
            F.col("_matched_customer").isNull(),
            "Missing or unknown customer"
        ),
        F.when(
            F.col("_matched_product").isNull(),
            "Missing or unknown product"
        )
    )
)

# Keep valid records only
incoming_valid = incoming_checked.filter(
    F.col("rejection_reasons") == ""
).drop(
    "rejection_reasons",
    "_matched_customer",
    "_matched_product"
)

# Verify incoming batch
print("Incoming Bronze records:", incoming_bronze.count())
print("Valid incoming Silver records:", incoming_valid.count())

display(
    incoming_valid.select(
        "order_id",
        "source_record_id",
        "quantity",
        "unit_price",
        "order_date",
        "updated_at",
        "sales_channel"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

# Prepare the incoming data in the same column order as Silver
silver = spark.table("workspace.default.silver_orders")

incoming_for_merge = incoming_valid.select(*silver.columns)

# Prevent duplicate order IDs in the incoming batch
assert incoming_for_merge.groupBy("order_id").count().filter(
    F.col("count") > 1
).count() == 0, "Duplicate order IDs in incoming batch!"

# Verify exactly one update candidate and one insert candidate
existing_ids = silver.select("order_id")

matched = incoming_for_merge.join(
    existing_ids, "order_id", "inner"
)

unmatched = incoming_for_merge.join(
    existing_ids, "order_id", "left_anti"
)

assert matched.count() == 1, "Expected one existing order!"
assert unmatched.count() == 1, "Expected one new order!"

# Record the count before MERGE
before_count = silver.count()

# Production Delta MERGE
incoming_for_merge.createOrReplaceTempView(
    "production_demo_incoming"
)

spark.sql("""
MERGE INTO workspace.default.silver_orders AS target
USING production_demo_incoming AS source
ON target.order_id = source.order_id

WHEN MATCHED
 AND source.updated_at > target.updated_at
THEN UPDATE SET *

WHEN NOT MATCHED
THEN INSERT *
""")

# Verify after MERGE
updated_silver = spark.table(
    "workspace.default.silver_orders"
)

after_count = updated_silver.count()

print("Rows before MERGE:", before_count)
print("Rows after MERGE:", after_count)
print("Existing orders updated:", matched.count())
print("New orders inserted:", unmatched.count())

display(
    updated_silver.filter(
        F.col("order_id").isin(
            "O01002",
            "HACKATHON_DEMO_NEW_001"
        )
    ).select(
        "order_id",
        "source_record_id",
        "sales_channel",
        "updated_at"
    )
)

# COMMAND ----------



from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# 1. Read production Silver tables
orders = spark.table("workspace.default.silver_orders")
customers = spark.table("workspace.default.silver_customers")
products = spark.table("workspace.default.silver_products")

# 2. Check dimension uniqueness
assert customers.groupBy("customer_id").count().filter(
    F.col("count") > 1
).count() == 0

assert products.groupBy("product_id").count().filter(
    F.col("count") > 1
).count() == 0

# 3. Join orders, customers and products
joined = (
    orders.alias("o")
    .join(
        customers.alias("c"),
        F.col("o.customer_id") == F.col("c.customer_id"),
        "left"
    )
    .join(
        products.alias("p"),
        F.col("o.product_id") == F.col("p.product_id"),
        "left"
    )
    .select(
        "o.order_id",
        "o.source_record_id",
        "o.order_date",
        "o.customer_id",
        F.col("c.customer_name").alias("customer_name"),
        F.col("c.city").alias("customer_city"),
        F.col("c.country").alias("customer_country"),
        F.col("c.customer_segment").alias("customer_segment"),
        F.col("c.loyalty_tier").alias("loyalty_tier"),
        "o.product_id",
        F.col("p.product_name").alias("product_name"),
        F.col("p.category").alias("product_category"),
        F.col("p.brand").alias("brand"),
        F.col("p.list_price").alias("list_price"),
        F.col("p.unit_cost").alias("unit_cost"),
        "o.quantity",
        "o.unit_price",
        "o.discount_pct",
        "o.order_status",
        "o.sales_channel",
        "o.payment_method",
        "o.promo_code",
        "o.shipping_fee",
        "o.tax_rate",
        "o.updated_at",
        "o.source_batch_id",
        "o._source_file"
    )
)

# 4. Calculate base revenue
gold = joined.withColumn(
    "base_revenue",
    (
        F.col("quantity").cast(DecimalType(18, 4)) *
        F.col("unit_price").cast(DecimalType(18, 4)) *
        (
            F.lit(1).cast(DecimalType(18, 4)) -
            F.col("discount_pct").cast(DecimalType(18, 4))
        )
    ).cast(DecimalType(18, 4))
)

# 5. Calculate loyalty discount
loyalty_eligible = (
    F.upper(F.trim(F.col("loyalty_tier"))).isin("GOLD", "PLATINUM") &
    (
        F.col("promo_code").isNull() |
        (F.trim(F.col("promo_code")) == "")
    ) &
    F.col("unit_cost").isNotNull() &
    (F.col("unit_cost") > 0)
)

potential_discount = F.col("base_revenue") * F.lit(0.02)

maximum_discount = F.greatest(
    F.lit(0).cast(DecimalType(18, 4)),
    F.col("quantity").cast(DecimalType(18, 4)) *
    F.col("unit_cost").cast(DecimalType(18, 4))
)

gold = gold.withColumn(
    "loyalty_discount",
    F.when(
        loyalty_eligible,
        F.least(potential_discount, maximum_discount)
    ).otherwise(
        F.lit(0).cast(DecimalType(18, 4))
    ).cast(DecimalType(18, 4))
)

# 6. Shipping charges
gold = gold.withColumn(
    "shipping_revenue",
    F.coalesce(
        F.col("shipping_fee").cast(DecimalType(18, 4)),
        F.lit(0).cast(DecimalType(18, 4))
    )
)

# 7. Revenue eligibility
gold = gold.withColumn(
    "revenue_eligible",
    F.upper(F.trim(F.col("order_status"))).isin(
        "COMPLETED", "SHIPPED"
    )
)

# 8. Calculate net revenue
gold = gold.withColumn(
    "net_revenue",
    F.when(
        F.col("revenue_eligible"),
        F.round(
            F.col("base_revenue")
            - F.col("loyalty_discount")
            + F.col("shipping_revenue"),
            2
        )
    ).otherwise(
        F.lit(0).cast(DecimalType(18, 2))
    ).cast(DecimalType(18, 2))
)

# 9. Validate before saving
assert gold.count() == orders.count(), "Row count mismatch!"

assert gold.select("order_id").distinct().count() == gold.count(), \
    "Duplicate order IDs found!"

# 10. Refresh production Gold
gold.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_sales")

print("SUCCESS: Production Gold refreshed!")

# 11. Final verification
final_gold = spark.table("workspace.default.gold_sales")

print("Silver orders:", orders.count())
print("Gold orders:", final_gold.count())

display(
    final_gold.filter(
        F.col("order_id") == "HACKATHON_DEMO_NEW_001"
    ).select(
        "order_id",
        "customer_name",
        "product_name",
        "sales_channel",
        "base_revenue",
        "loyalty_discount",
        "shipping_revenue",
        "net_revenue"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

silver = spark.table("workspace.default.silver_orders")

# Record counts and timestamps before rerun
before_count = silver.count()

before_records = silver.filter(
    F.col("order_id").isin(
        "O01002",
        "HACKATHON_DEMO_NEW_001"
    )
).select("order_id", "updated_at", "sales_channel")

# Prepare the same incoming batch
incoming_for_merge = incoming_valid.select(*silver.columns)

incoming_for_merge.createOrReplaceTempView(
    "production_demo_incoming"
)

# Process the exact same batch again
spark.sql("""
MERGE INTO workspace.default.silver_orders AS target
USING production_demo_incoming AS source
ON target.order_id = source.order_id

WHEN MATCHED
 AND source.updated_at > target.updated_at
THEN UPDATE SET *

WHEN NOT MATCHED
THEN INSERT *
""")

# Verify after rerun
after = spark.table("workspace.default.silver_orders")
after_count = after.count()

print("Rows before rerun:", before_count)
print("Rows after rerun:", after_count)

assert before_count == after_count == 4733

# Check that both records remain unique
for order_id in ["O01002", "HACKATHON_DEMO_NEW_001"]:
    count = after.filter(F.col("order_id") == order_id).count()
    assert count == 1, f"Duplicate or missing order: {order_id}"

print("SUCCESS: Production idempotency verified!")

display(
    after.filter(
        F.col("order_id").isin(
            "O01002",
            "HACKATHON_DEMO_NEW_001"
        )
    ).select(
        "order_id",
        "sales_channel",
        "updated_at"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

silver = spark.table("workspace.default.silver_orders")
gold = spark.table("workspace.default.gold_sales")

print("Silver orders:", silver.count())
print("Gold orders:", gold.count())

print(
    "Unique Gold order IDs:",
    gold.select("order_id").distinct().count()
)

print(
    "New order in Gold:",
    gold.filter(
        F.col("order_id") == "HACKATHON_DEMO_NEW_001"
    ).count()
)

assert silver.count() == 4733
assert gold.count() == 4733
assert gold.select("order_id").distinct().count() == 4733
assert gold.filter(
    F.col("order_id") == "HACKATHON_DEMO_NEW_001"
).count() == 1

print("SUCCESS: Production Silver-to-Gold reconciliation passed!")