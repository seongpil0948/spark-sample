"""
ETL Pipeline Module
Implements production-ready ETL with data quality checks and optimizations
"""

from typing import Dict, Any, Optional, List
from datetime import datetime
import structlog
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    when, current_date, current_timestamp, hour, date_format,
    collect_list, stddev, mean, lit, coalesce, trim, upper,
    regexp_replace, split, explode, array_contains, size,
    countDistinct, first, last, window, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    DoubleType, TimestampType, MapType, ArrayType
)

logger = structlog.get_logger()


class ETLPipeline:
    """Production ETL Pipeline with optimizations"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._configure_optimizations()
        
    def _configure_optimizations(self):
        """Configure Spark 4.0 optimizations for ETL"""
        # Enable Spark 4.0 Enhanced AQE
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.optimizeSkewedJoin.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
        
        # Spark 4.0 Query optimization
        self.spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "100MB")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1MB")
        self.spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
        
        # Optimize shuffle partitions
        self.spark.conf.set("spark.sql.shuffle.partitions", "200")
        
        # Enable broadcast join threshold
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")
        
        # Spark 4.0 Columnar processing
        self.spark.conf.set("spark.sql.columnVector.offheap.enabled", "true")
        self.spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", "true")
    
    def define_schema(self) -> StructType:
        """Define explicit schema for better performance"""
        return StructType([
            StructField("event_id", StringType(), False),
            StructField("user_id", LongType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("event_type", StringType(), True),  # Allow nulls for data quality testing
            StructField("category", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("properties", MapType(StringType(), StringType()), True),
            StructField("tags", ArrayType(StringType()), True),
            StructField("location", StructType([
                StructField("country", StringType(), True),
                StructField("city", StringType(), True),
                StructField("lat", DoubleType(), True),
                StructField("lon", DoubleType(), True)
            ]), True)
        ])
    
    def run_quality_checks(self, input_path: str, date: str) -> Dict[str, Any]:
        """Comprehensive data quality checks"""
        logger.info("running_data_quality_checks", path=input_path, date=date)
        
        # Read data with schema
        df = self.spark.read.schema(self.define_schema()).parquet(
            f"{input_path}/date={date}/"
        )
        
        total_count = df.count()
        
        # Null checks
        null_counts = {}
        critical_columns = ["event_id", "user_id", "timestamp", "event_type"]
        
        for col_name in critical_columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_counts[col_name] = null_count
            if null_count > 0:
                logger.warning("null_values_found", 
                             column=col_name, 
                             count=null_count,
                             percentage=f"{(null_count/total_count)*100:.2f}%")
        
        # Duplicate checks
        duplicate_count = total_count - df.dropDuplicates(["event_id"]).count()
        
        # Data freshness check
        latest_timestamp = df.agg(spark_max("timestamp").alias("max_ts")).collect()[0]["max_ts"]
        staleness_hours = (datetime.now() - latest_timestamp).total_seconds() / 3600
        
        # Statistical anomaly detection
        if "amount" in df.columns:
            amount_stats = df.filter(col("amount").isNotNull()).agg(
                mean("amount").alias("mean"),
                stddev("amount").alias("std"),
                spark_min("amount").alias("min"),
                spark_max("amount").alias("max")
            ).collect()[0]
            
            # Detect outliers (3 sigma rule)
            outlier_threshold_upper = amount_stats["mean"] + 3 * amount_stats["std"]
            outlier_threshold_lower = amount_stats["mean"] - 3 * amount_stats["std"]
            
            outliers = df.filter(
                (col("amount") > outlier_threshold_upper) | 
                (col("amount") < outlier_threshold_lower)
            ).count()
        else:
            amount_stats = None
            outliers = 0
        
        # Schema compliance
        expected_columns = set([field.name for field in self.define_schema().fields])
        actual_columns = set(df.columns)
        missing_columns = expected_columns - actual_columns
        extra_columns = actual_columns - expected_columns
        
        quality_report = {
            "total_records": total_count,
            "null_counts": null_counts,
            "duplicate_records": duplicate_count,
            "duplicate_percentage": f"{(duplicate_count/total_count)*100:.2f}%",
            "data_staleness_hours": staleness_hours,
            "amount_statistics": amount_stats,
            "outlier_records": outliers,
            "schema_compliance": {
                "missing_columns": list(missing_columns),
                "extra_columns": list(extra_columns),
                "is_compliant": len(missing_columns) == 0
            },
            "quality_score": self._calculate_quality_score(
                total_count, null_counts, duplicate_count, outliers
            )
        }
        
        return quality_report
    
    def _calculate_quality_score(self, total: int, nulls: Dict, 
                                duplicates: int, outliers: int) -> float:
        """Calculate overall data quality score (0-100)"""
        if total == 0:
            return 0.0
            
        # Weighted scoring
        null_penalty = sum(nulls.values()) / total * 30  # 30% weight
        duplicate_penalty = duplicates / total * 20      # 20% weight
        outlier_penalty = outliers / total * 10          # 10% weight
        
        score = 100 - (null_penalty + duplicate_penalty + outlier_penalty)
        return max(0, min(100, score))
    
    def transform_data(self, input_path: str, date: str) -> DataFrame:
        """Main transformation logic with optimizations"""
        logger.info("starting_data_transformation", date=date)
        
        # Read data with Spark 4.0 optimizations
        df = self.spark.read \
            .schema(self.define_schema()) \
            .option("mergeSchema", "false") \
            .option("filterPushdown", "true") \
            .parquet(f"{input_path}/date={date}/")
        
        # Use Spark 4.0 adaptive caching
        df = df.hint("cache_lazy")
        
        # Data cleaning
        cleaned_df = df.filter(col("event_id").isNotNull()) \
            .dropDuplicates(["event_id"]) \
            .withColumn("event_type", upper(trim(col("event_type")))) \
            .withColumn("category", coalesce(col("category"), lit("UNKNOWN"))) \
            .withColumn("amount", coalesce(col("amount"), lit(0.0)))
        
        # Feature engineering
        enriched_df = cleaned_df \
            .withColumn("processed_date", current_date()) \
            .withColumn("processed_timestamp", current_timestamp()) \
            .withColumn("hour", hour("timestamp")) \
            .withColumn("day_of_week", date_format("timestamp", "EEEE")) \
            .withColumn("amount_category", 
                when(col("amount") < 100, "small")
                .when(col("amount") < 1000, "medium")
                .when(col("amount") < 10000, "large")
                .otherwise("very_large")
            ) \
            .withColumn("tag_count", 
                when(col("tags").isNotNull(), size(col("tags")))
                .otherwise(0)
            ) \
            .withColumn("has_properties", 
                when(col("properties").isNotNull() & (size(col("properties")) > 0), True)
                .otherwise(False)
            )
        
        # Business logic transformations with Spark 4.0 filter hints
        final_df = enriched_df \
            .hint("predicate_pushdown") \
            .filter(
                col("event_type").isin("PURCHASE", "ADD_TO_CART", "VIEW", "SEARCH")
            )
        
        # Use Spark 4.0 adaptive partitioning
        return final_df.repartition(col("hour"), col("event_type"))
    
    def generate_summary(self, df: DataFrame) -> DataFrame:
        """Generate aggregated summary with multiple metrics"""
        logger.info("generating_summary_metrics")
        
        # Hourly aggregations
        hourly_summary = df.groupBy("hour", "event_type", "amount_category") \
            .agg(
                count("*").alias("event_count"),
                countDistinct("user_id").alias("unique_users"),
                spark_sum("amount").alias("total_amount"),
                avg("amount").alias("avg_amount"),
                spark_max("amount").alias("max_amount"),
                spark_min("amount").alias("min_amount"),
                avg("tag_count").alias("avg_tags_per_event"),
                spark_sum(when(col("has_properties"), 1).otherwise(0)).alias("events_with_properties")
            )
        
        # Calculate conversion metrics
        summary_with_metrics = hourly_summary \
            .withColumn("revenue_per_user", 
                when(col("unique_users") > 0, col("total_amount") / col("unique_users"))
                .otherwise(0)
            ) \
            .withColumn("properties_usage_rate",
                when(col("event_count") > 0, col("events_with_properties") / col("event_count"))
                .otherwise(0)
            )
        
        return summary_with_metrics
    
    def save_results(self, transformed_df: DataFrame, summary_df: DataFrame, 
                    output_path: str, date: str):
        """Save results with optimizations"""
        logger.info("saving_results", output_path=output_path, date=date)
        
        # Save detailed data with Spark 4.0 write optimizations
        transformed_df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .option("maxRecordsPerFile", "1000000") \
            .option("spark.sql.files.maxPartitionBytes", "128MB") \
            .partitionBy("processed_date", "hour") \
            .parquet(f"{output_path}/detailed/date={date}/")
        
        # Save summary data with Spark 4.0 adaptive coalescing
        summary_df \
            .coalesce(1) \
            .sortWithinPartitions("hour", "event_type") \
            .write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(f"{output_path}/summary/date={date}/")
        
        # Generate and save daily rollup
        daily_rollup = summary_df.groupBy("event_type", "amount_category") \
            .agg(
                spark_sum("event_count").alias("total_events"),
                spark_sum("unique_users").alias("total_unique_users"),
                spark_sum("total_amount").alias("total_revenue"),
                avg("revenue_per_user").alias("avg_revenue_per_user")
            )
        
        daily_rollup.coalesce(1) \
            .write \
            .mode("overwrite") \
            .parquet(f"{output_path}/daily_rollup/date={date}/")
        
        logger.info("results_saved_successfully", 
                   detailed_records=transformed_df.count(),
                   summary_records=summary_df.count())