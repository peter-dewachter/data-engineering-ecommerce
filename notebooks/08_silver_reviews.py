# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 08 — Silver Order Reviews
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Transform `ecommerce.bronze.order_reviews` into a curated
# MAGIC `ecommerce.silver.order_reviews` table.
# MAGIC
# MAGIC ### Silver design decisions
# MAGIC
# MAGIC - Preserve every Bronze source row.
# MAGIC - Grain: 1 Silver row = 1 Bronze source row.
# MAGIC - Do not deduplicate based on `review_id` or `order_id`.
# MAGIC - Preserve raw source values for investigation.
# MAGIC - Parse valid review scores into an integer.
# MAGIC - Parse valid timestamps into timestamp columns.
# MAGIC - Add explicit data-quality flags.
# MAGIC - Validate structurally valid order references against Silver Orders.
# MAGIC - Preserve naturally sparse review comments.
# MAGIC - Do not treat `review_id` as a unique key.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    lit,
    sum,
    trim,
    when,
    try_to_timestamp
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Bronze Reviews

# COMMAND ----------

reviews_df = spark.table("ecommerce.bronze.order_reviews")

reviews_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Confirm Bronze Row Count
# MAGIC
# MAGIC This should match the profiling result of 104,162 rows.

# COMMAND ----------

bronze_review_count = reviews_df.count()

print(f"Bronze review rows: {bronze_review_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Standardize Raw String Fields
# MAGIC
# MAGIC We trim identifier and textual fields, but preserve their source meaning.
# MAGIC
# MAGIC The original date columns remain as raw strings. Typed timestamp columns
# MAGIC will be created separately.

# COMMAND ----------

reviews_standardized = reviews_df.select(
    trim(col("review_id")).alias("review_id"),
    trim(col("order_id")).alias("order_id"),
    trim(col("review_score")).alias("review_score"),
    trim(col("review_comment_title")).alias("review_comment_title"),
    trim(col("review_comment_message")).alias("review_comment_message"),
    trim(col("review_creation_date")).alias("review_creation_date"),
    trim(col("review_answer_timestamp")).alias("review_answer_timestamp")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Parse Review Score
# MAGIC
# MAGIC A valid review score is exactly 1–5.
# MAGIC
# MAGIC We keep the original `review_score` string and create a typed
# MAGIC `review_score_int` column.
# MAGIC
# MAGIC This allows us to distinguish:
# MAGIC
# MAGIC - missing source values
# MAGIC - valid scores
# MAGIC - malformed source values

# COMMAND ----------

reviews_typed = (
    reviews_standardized
    .withColumn(
        "review_score_int",
        when(
            col("review_score").isin("1", "2", "3", "4", "5"),
            col("review_score").cast("int")
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Parse Review Timestamps
# MAGIC
# MAGIC `try_to_timestamp` is intentionally used instead of `to_timestamp`.
# MAGIC
# MAGIC Some Bronze rows contain malformed values caused by source CSV
# MAGIC column shifting. `try_to_timestamp` converts those values to NULL
# MAGIC instead of failing the entire Spark job.

# COMMAND ----------

reviews_typed = (
    reviews_typed
    .withColumn(
        "review_creation_ts",
        try_to_timestamp(
            col("review_creation_date"),
            lit("yyyy-MM-dd HH:mm:ss")
        )
    )
    .withColumn(
        "review_answer_ts",
        try_to_timestamp(
            col("review_answer_timestamp"),
            lit("yyyy-MM-dd HH:mm:ss")
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Add Identifier Quality Flags
# MAGIC
# MAGIC Structural validation is intentionally separate from referential
# MAGIC integrity.
# MAGIC
# MAGIC For example:
# MAGIC
# MAGIC - a NULL order ID → `missing_order_id`
# MAGIC - text that isn't a 32-character hex ID → `malformed_order_id`
# MAGIC - a structurally valid ID that doesn't exist in Orders →
# MAGIC   `invalid_order_reference`

# COMMAND ----------

reviews_flagged = (
    reviews_typed

    # Review ID
    .withColumn(
        "missing_review_id",
        col("review_id").isNull()
    )
    .withColumn(
        "malformed_review_id",
        (
            col("review_id").isNotNull()
            & ~col("review_id").rlike("^[0-9a-f]{32}$")
        )
    )

    # Order ID
    .withColumn(
        "missing_order_id",
        col("order_id").isNull()
    )
    .withColumn(
        "malformed_order_id",
        (
            col("order_id").isNotNull()
            & ~col("order_id").rlike("^[0-9a-f]{32}$")
        )
    )

    # Review score
    .withColumn(
        "missing_review_score",
        col("review_score").isNull()
    )
    .withColumn(
        "invalid_review_score",
        (
            col("review_score").isNotNull()
            & ~col("review_score").isin("1", "2", "3", "4", "5")
        )
    )

    # Creation timestamp
    .withColumn(
        "missing_review_creation_timestamp",
        col("review_creation_date").isNull()
    )
    .withColumn(
        "invalid_review_creation_timestamp",
        (
            col("review_creation_date").isNotNull()
            & col("review_creation_ts").isNull()
        )
    )

    # Answer timestamp
    .withColumn(
        "missing_review_answer_timestamp",
        col("review_answer_timestamp").isNull()
    )
    .withColumn(
        "invalid_review_answer_timestamp",
        (
            col("review_answer_timestamp").isNotNull()
            & col("review_answer_ts").isNull()
        )
    )

    # Timestamp ordering
    .withColumn(
        "answer_before_creation",
        (
            col("review_creation_ts").isNotNull()
            & col("review_answer_ts").isNotNull()
            & (col("review_answer_ts") < col("review_creation_ts"))
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validate Order Referential Integrity
# MAGIC
# MAGIC Only structurally valid order IDs are checked against Silver Orders.
# MAGIC
# MAGIC Malformed/null IDs are handled by the structural flags above and are
# MAGIC not incorrectly classified as foreign-key violations.

# COMMAND ----------

orders_lookup = (
    spark.table("ecommerce.silver.orders")
    .select("order_id")
    .distinct()
    .withColumn("order_exists", lit(True))
)

reviews_with_order_check = (
    reviews_flagged
    .join(
        orders_lookup,
        on="order_id",
        how="left"
    )
    .withColumn(
        "invalid_order_reference",
        (
            col("order_id").isNotNull()
            & col("order_id").rlike("^[0-9a-f]{32}$")
            & col("order_exists").isNull()
        )
    )
    .drop("order_exists")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Inspect Quality Flags
# MAGIC
# MAGIC Expected based on profiling:
# MAGIC
# MAGIC - malformed/missing order IDs: 4,938
# MAGIC - malformed/missing review IDs: 4,938
# MAGIC - invalid order references: 0
# MAGIC - invalid review scores: 2,558
# MAGIC - missing review scores: 2,380
# MAGIC - answer-before-creation: 0

# COMMAND ----------

reviews_with_order_check.select(
    count("*").alias("total_rows"),

    sum(
        when(col("missing_review_id"), 1).otherwise(0)
    ).alias("missing_review_id"),

    sum(
        when(col("malformed_review_id"), 1).otherwise(0)
    ).alias("malformed_review_id"),

    sum(
        when(col("missing_order_id"), 1).otherwise(0)
    ).alias("missing_order_id"),

    sum(
        when(col("malformed_order_id"), 1).otherwise(0)
    ).alias("malformed_order_id"),

    sum(
        when(col("invalid_order_reference"), 1).otherwise(0)
    ).alias("invalid_order_reference"),

    sum(
        when(col("missing_review_score"), 1).otherwise(0)
    ).alias("missing_review_score"),

    sum(
        when(col("invalid_review_score"), 1).otherwise(0)
    ).alias("invalid_review_score"),

    sum(
        when(col("missing_review_creation_timestamp"), 1).otherwise(0)
    ).alias("missing_review_creation_timestamp"),

    sum(
        when(col("invalid_review_creation_timestamp"), 1).otherwise(0)
    ).alias("invalid_review_creation_timestamp"),

    sum(
        when(col("missing_review_answer_timestamp"), 1).otherwise(0)
    ).alias("missing_review_answer_timestamp"),

    sum(
        when(col("invalid_review_answer_timestamp"), 1).otherwise(0)
    ).alias("invalid_review_answer_timestamp"),

    sum(
        when(col("answer_before_creation"), 1).otherwise(0)
    ).alias("answer_before_creation")
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Validate Review Grain
# MAGIC
# MAGIC The Silver grain is intentionally:
# MAGIC
# MAGIC **one row per Bronze source row**
# MAGIC
# MAGIC We therefore do not enforce uniqueness on `review_id` or `order_id`.

# COMMAND ----------

silver_count = reviews_with_order_check.count()

print(f"Bronze rows: {bronze_review_count}")
print(f"Silver rows: {silver_count}")

assert silver_count == bronze_review_count, (
    "Silver row count does not match Bronze row count."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Validate Typed Review Scores
# MAGIC
# MAGIC Only scores 1–5 should populate `review_score_int`.

# COMMAND ----------

reviews_with_order_check.select(
    count("*").alias("total_rows"),
    count("review_score_int").alias("valid_typed_scores"),
    sum(
        when(
            col("review_score").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_scores"),
    sum(
        when(
            col("review_score").isNotNull()
            & col("review_score_int").isNull(),
            1
        ).otherwise(0)
    ).alias("invalid_scores")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Validate Timestamp Parsing

# COMMAND ----------

reviews_with_order_check.select(
    count("*").alias("total_rows"),

    sum(
        when(
            col("review_creation_ts").isNotNull(),
            1
        ).otherwise(0)
    ).alias("valid_creation_timestamps"),

    sum(
        when(
            col("review_answer_ts").isNotNull(),
            1
        ).otherwise(0)
    ).alias("valid_answer_timestamps"),

    sum(
        when(
            col("answer_before_creation"),
            1
        ).otherwise(0)
    ).alias("answer_before_creation")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Inspect Malformed Source Rows
# MAGIC
# MAGIC This is intentionally kept as an investigation view.
# MAGIC
# MAGIC We do not delete these rows.

# COMMAND ----------

reviews_with_order_check.filter(
    col("malformed_order_id")
    | col("malformed_review_id")
    | col("invalid_review_score")
    | col("invalid_review_creation_timestamp")
    | col("invalid_review_answer_timestamp")
).select(
    "review_id",
    "order_id",
    "review_score",
    "review_comment_title",
    "review_comment_message",
    "review_creation_date",
    "review_answer_timestamp",
    "missing_review_id",
    "malformed_review_id",
    "missing_order_id",
    "malformed_order_id",
    "invalid_order_reference",
    "missing_review_score",
    "invalid_review_score",
    "missing_review_creation_timestamp",
    "invalid_review_creation_timestamp",
    "missing_review_answer_timestamp",
    "invalid_review_answer_timestamp"
).limit(20).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Select Final Silver Columns
# MAGIC
# MAGIC Raw source values are retained alongside typed values and quality flags.

# COMMAND ----------

reviews_silver = reviews_with_order_check.select(
    # Source identifiers
    "review_id",
    "order_id",

    # Raw source score + typed score
    "review_score",
    "review_score_int",

    # Review comments
    "review_comment_title",
    "review_comment_message",

    # Raw source timestamps
    "review_creation_date",
    "review_answer_timestamp",

    # Parsed timestamps
    "review_creation_ts",
    "review_answer_ts",

    # Identifier quality flags
    "missing_review_id",
    "malformed_review_id",
    "missing_order_id",
    "malformed_order_id",
    "invalid_order_reference",

    # Score quality flags
    "missing_review_score",
    "invalid_review_score",

    # Timestamp quality flags
    "missing_review_creation_timestamp",
    "invalid_review_creation_timestamp",
    "missing_review_answer_timestamp",
    "invalid_review_answer_timestamp",
    "answer_before_creation"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Create Silver Schema

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS ecommerce.silver
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. Write Silver Table

# COMMAND ----------

(
    reviews_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("ecommerce.silver.order_reviews")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 17. Reload Persisted Silver Table
# MAGIC
# MAGIC Reloading the table validates the actual persisted Delta table rather
# MAGIC than only the in-memory DataFrame.

# COMMAND ----------

silver_reviews_df = spark.table(
    "ecommerce.silver.order_reviews"
)

silver_reviews_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 18. Final Row-Count Validation

# COMMAND ----------

persisted_count = silver_reviews_df.count()

print(f"Bronze rows: {bronze_review_count}")
print(f"Persisted Silver rows: {persisted_count}")

assert persisted_count == bronze_review_count, (
    "Persisted Silver row count does not match Bronze."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 19. Final Quality Summary

# COMMAND ----------

silver_reviews_df.select(
    count("*").alias("total_rows"),

    sum(
        when(col("missing_review_id"), 1).otherwise(0)
    ).alias("missing_review_id"),

    sum(
        when(col("malformed_review_id"), 1).otherwise(0)
    ).alias("malformed_review_id"),

    sum(
        when(col("missing_order_id"), 1).otherwise(0)
    ).alias("missing_order_id"),

    sum(
        when(col("malformed_order_id"), 1).otherwise(0)
    ).alias("malformed_order_id"),

    sum(
        when(col("invalid_order_reference"), 1).otherwise(0)
    ).alias("invalid_order_reference"),

    sum(
        when(col("missing_review_score"), 1).otherwise(0)
    ).alias("missing_review_score"),

    sum(
        when(col("invalid_review_score"), 1).otherwise(0)
    ).alias("invalid_review_score"),

    sum(
        when(col("missing_review_creation_timestamp"), 1).otherwise(0)
    ).alias("missing_review_creation_timestamp"),

    sum(
        when(col("invalid_review_creation_timestamp"), 1).otherwise(0)
    ).alias("invalid_review_creation_timestamp"),

    sum(
        when(col("missing_review_answer_timestamp"), 1).otherwise(0)
    ).alias("missing_review_answer_timestamp"),

    sum(
        when(col("invalid_review_answer_timestamp"), 1).otherwise(0)
    ).alias("invalid_review_answer_timestamp"),

    sum(
        when(col("answer_before_creation"), 1).otherwise(0)
    ).alias("answer_before_creation")
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 20. Sample Final Silver Data

# COMMAND ----------

silver_reviews_df.show(
    20,
    truncate=False
)