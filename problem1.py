#!/usr/bin/env python3
"""
Problem 1: Log Level Distribution Analysis
Analyzes the distribution of log levels (INFO, WARN, ERROR, DEBUG) across all log files.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, count, rand, lit, concat_ws
import argparse
import sys
import os
import logging
from typing import Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session(master: str = "local[*]") -> SparkSession:
    """Create and configure Spark session."""
    return SparkSession.builder \
        .appName("LogLevelDistribution") \
        .master(master) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "1") \
        .config("spark.cores.max", "3") \
        .getOrCreate()

def analyze_log_levels(spark: SparkSession, input_path: str) -> Tuple:
    """
    Analyze log levels from Spark log files.
    
    Returns:
        Tuple of (log_level_counts_df, sample_df, total_lines, lines_with_levels)
    """
    # Read all log files
    logger.info(f"Reading log files from: {input_path}")
    # Adjust path pattern based on input type
    if "s3://" in input_path or "s3a://" in input_path:
        file_pattern = f"{input_path}/*/*.log"  # S3 has flatter structure
    else:
        file_pattern = f"{input_path}/*/*.log"
    logs_df = spark.read.text(file_pattern)
    
    # Cache the DataFrame for multiple operations
    logs_df.cache()
    
    # Count total lines
    total_lines = logs_df.count()
    logger.info(f"Total log lines read: {total_lines:,}")
    
    # Extract log levels using regex
    # Pattern matches log levels at the beginning of the log message after timestamp
    logs_with_level = logs_df.select(
        col("value").alias("log_entry"),
        regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)', 1).alias('log_level')
    ).filter(col('log_level') != '')
    
    # Count lines with log levels
    lines_with_levels = logs_with_level.count()
    logger.info(f"Lines with log levels: {lines_with_levels:,}")
    
    # Count log levels
    log_level_counts = logs_with_level.groupBy('log_level') \
        .agg(count('*').alias('count')) \
        .orderBy('count', ascending=False)
    
    # Get sample entries (10 random entries)
    sample_entries = logs_with_level \
        .select('log_entry', 'log_level') \
        .orderBy(rand()) \
        .limit(10)
    
    return log_level_counts, sample_entries, total_lines, lines_with_levels

def save_results(log_level_counts, sample_entries, total_lines, lines_with_levels, output_dir: str):
    """Save analysis results to CSV and text files matching README requirements."""
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Collect data for writing
    counts_collected = log_level_counts.collect()
    sample_collected = sample_entries.collect()
    
    # 1. Save log level counts to CSV file (not Spark directory)
    counts_path = os.path.join(output_dir, "problem1_counts.csv")
    logger.info(f"Saving log level counts to: {counts_path}")
    with open(counts_path, 'w') as f:
        f.write("log_level,count\n")
        for row in counts_collected:
            f.write(f"{row['log_level']},{row['count']}\n")
    
    # 2. Save sample entries to CSV
    sample_path = os.path.join(output_dir, "problem1_sample.csv")
    logger.info(f"Saving sample entries to: {sample_path}")
    with open(sample_path, 'w') as f:
        f.write("log_entry,log_level\n")
        for row in sample_collected:
            # Escape quotes and truncate long entries
            entry = row['log_entry'][:200].replace('"', '""')
            f.write(f'"{entry}",{row["log_level"]}\n')
    
    # 3. Generate and save summary statistics
    summary_path = os.path.join(output_dir, "problem1_summary.txt")
    logger.info(f"Generating summary statistics: {summary_path}")
    
    with open(summary_path, 'w') as f:
        f.write(f"Total log lines processed: {total_lines:,}\n")
        f.write(f"Total lines with log levels: {lines_with_levels:,}\n")
        f.write(f"Unique log levels found: {len(counts_collected)}\n\n")
        f.write("Log level distribution:\n")
        
        for row in counts_collected:
            percentage = (row['count'] / lines_with_levels) * 100
            f.write(f"  {row['log_level']:6s}: {row['count']:10,} ({percentage:6.2f}%)\n")
    
    logger.info("Analysis complete!")

def main():
    """Main function to run log level distribution analysis."""
    parser = argparse.ArgumentParser(description="Analyze log level distribution in Spark logs")
    parser.add_argument("master", nargs='?', default="local[*]", 
                        help="Spark master URL (default: local[*])")
    parser.add_argument("--net-id", required=False, 
                        help="Your net ID for S3 bucket access")
    parser.add_argument("--local", action="store_true",
                        help="Use local sample data instead of S3")
    args = parser.parse_args()
    
    # Determine input path
    if args.local:
        input_path = "data/sample"
        logger.info("Running in LOCAL mode with sample data")
    elif args.net_id:
        input_path = f"s3a://{args.net_id}-assignment-spark-cluster-logs/data"
        logger.info(f"Running in CLUSTER mode with S3 data: {input_path}")
    else:
        # Default to local full dataset if no net-id provided
        input_path = "data/raw"
        logger.info("Running with local full dataset")
    
    # Create Spark session
    spark = create_spark_session(args.master)
    
    try:
        # Run analysis
        log_level_counts, sample_entries, total_lines, lines_with_levels = \
            analyze_log_levels(spark, input_path)
        
        # Save results
        output_dir = "data/output"
        save_results(log_level_counts, sample_entries, total_lines, lines_with_levels, output_dir)
        
    except Exception as e:
        logger.error(f"Error during analysis: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
