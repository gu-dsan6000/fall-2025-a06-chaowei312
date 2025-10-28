#!/usr/bin/env python3
"""
Problem 2: Cluster Usage Analysis
Analyzes cluster usage patterns to understand which clusters are most heavily used over time.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, count, input_file_name, to_timestamp, min, max, datediff
import argparse
import sys
import os
import logging
from typing import Tuple
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.dates as mdates
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session(master: str = "local[*]") -> SparkSession:
    """Create and configure Spark session."""
    return SparkSession.builder \
        .appName("ClusterUsageAnalysis") \
        .master(master) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "1") \
        .config("spark.cores.max", "3") \
        .getOrCreate()

def analyze_cluster_usage(spark: SparkSession, input_path: str):
    """
    Analyze cluster usage patterns from Spark log files.
    
    Returns:
        Tuple of (timeline_df, cluster_summary_df, unique_clusters, total_apps)
    """
    # Read all log files with file path
    logger.info(f"Reading log files from: {input_path}")
    
    # Adjust path pattern based on input type
    if "s3://" in input_path or "s3a://" in input_path:
        file_pattern = f"{input_path}/*/*.log"  # S3 has flatter structure
    else:
        file_pattern = f"{input_path}/*/*.log"
    
    logs_df = spark.read.text(file_pattern) \
        .withColumn('file_path', input_file_name())
    
    # Extract application ID and cluster ID from file path
    # Pattern: application_CLUSTERID_APPNUMBER
    logs_with_ids = logs_df.withColumn(
        'application_id',
        regexp_extract('file_path', r'(application_\d+_\d+)', 1)
    ).withColumn(
        'cluster_id',
        regexp_extract('file_path', r'application_(\d+)_\d+', 1)
    ).withColumn(
        'app_number',
        regexp_extract('file_path', r'application_\d+_(\d+)', 1)
    ).filter(col('application_id') != '')
    
    # Extract timestamps from log entries
    # Pattern: YY/MM/DD HH:MM:SS
    logs_with_time = logs_with_ids.withColumn(
        'timestamp_str',
        regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1)
    ).filter(col('timestamp_str') != '')
    
    # Convert timestamp string to timestamp type
    logs_with_timestamp = logs_with_time.withColumn(
        'timestamp',
        to_timestamp('timestamp_str', 'yy/MM/dd HH:mm:ss')
    ).filter(col('timestamp').isNotNull())
    
    # Get start and end times for each application
    app_timeline = logs_with_timestamp.groupBy(
        'cluster_id', 'application_id', 'app_number'
    ).agg(
        min('timestamp').alias('start_time'),
        max('timestamp').alias('end_time')
    ).orderBy('cluster_id', 'app_number')
    
    # Calculate cluster-level statistics
    cluster_summary = app_timeline.groupBy('cluster_id').agg(
        count('application_id').alias('num_applications'),
        min('start_time').alias('cluster_first_app'),
        max('end_time').alias('cluster_last_app')
    ).orderBy('num_applications', ascending=False)
    
    # Get counts for statistics
    unique_clusters = cluster_summary.count()
    total_apps = app_timeline.count()
    
    logger.info(f"Found {unique_clusters} unique clusters with {total_apps} total applications")
    
    return app_timeline, cluster_summary, unique_clusters, total_apps

def save_results(timeline_df, cluster_summary_df, unique_clusters, total_apps, output_dir: str):
    """Save analysis results to CSV files and generate visualizations."""
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Collect data for saving and visualization
    timeline_data = timeline_df.collect()
    cluster_data = cluster_summary_df.collect()
    
    # Save timeline CSV
    timeline_path = os.path.join(output_dir, "problem2_timeline.csv")
    logger.info(f"Saving timeline data to: {timeline_path}")
    with open(timeline_path, 'w') as f:
        f.write("cluster_id,application_id,app_number,start_time,end_time\n")
        for row in timeline_data:
            f.write(f"{row['cluster_id']},{row['application_id']},{row['app_number']},"
                   f"{row['start_time']},{row['end_time']}\n")
    
    # Save cluster summary CSV
    summary_path = os.path.join(output_dir, "problem2_cluster_summary.csv")
    logger.info(f"Saving cluster summary to: {summary_path}")
    with open(summary_path, 'w') as f:
        f.write("cluster_id,num_applications,cluster_first_app,cluster_last_app\n")
        for row in cluster_data:
            f.write(f"{row['cluster_id']},{row['num_applications']},"
                   f"{row['cluster_first_app']},{row['cluster_last_app']}\n")
    
    # Save statistics
    stats_path = os.path.join(output_dir, "problem2_stats.txt")
    logger.info(f"Generating summary statistics: {stats_path}")
    
    with open(stats_path, 'w') as f:
        f.write(f"Total unique clusters: {unique_clusters}\n")
        f.write(f"Total applications: {total_apps}\n")
        f.write(f"Average applications per cluster: {total_apps/unique_clusters:.2f}\n\n")
        f.write("Most heavily used clusters:\n")
        
        for row in cluster_data[:10]:  # Top 10 clusters
            f.write(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications\n")
    
    # Generate visualizations if we have data
    if timeline_data and cluster_data:
        generate_visualizations(timeline_data, cluster_data, output_dir)
    
    logger.info("Analysis complete!")

def generate_visualizations(timeline_data, cluster_data, output_dir: str):
    """Generate bar chart and density plot visualizations."""
    
    # Convert to pandas DataFrames
    import pandas as pd
    
    # Prepare cluster summary data for bar chart
    cluster_df = pd.DataFrame(cluster_data)
    
    # Create bar chart
    plt.figure(figsize=(12, 6))
    ax = sns.barplot(data=cluster_df, x='cluster_id', y='num_applications', palette='viridis')
    plt.title('Number of Applications per Cluster', fontsize=16)
    plt.xlabel('Cluster ID', fontsize=12)
    plt.ylabel('Number of Applications', fontsize=12)
    plt.xticks(rotation=45)
    
    # Add value labels on top of bars
    for i, v in enumerate(cluster_df['num_applications']):
        ax.text(i, v + 0.5, str(v), ha='center', va='bottom')
    
    plt.tight_layout()
    bar_chart_path = os.path.join(output_dir, "problem2_bar_chart.png")
    plt.savefig(bar_chart_path, dpi=100)
    plt.close()
    logger.info(f"Bar chart saved to: {bar_chart_path}")
    
    # Prepare timeline data for density plot
    timeline_df = pd.DataFrame(timeline_data)
    
    # Convert timestamps to datetime if they're strings
    timeline_df['start_time'] = pd.to_datetime(timeline_df['start_time'])
    timeline_df['end_time'] = pd.to_datetime(timeline_df['end_time'])
    
    # Calculate job duration in minutes
    timeline_df['duration_minutes'] = (timeline_df['end_time'] - timeline_df['start_time']).dt.total_seconds() / 60
    
    # Find the largest cluster
    largest_cluster = cluster_df.nlargest(1, 'num_applications')['cluster_id'].values[0]
    largest_cluster_data = timeline_df[timeline_df['cluster_id'] == largest_cluster]
    
    # Create density plot for the largest cluster
    plt.figure(figsize=(10, 6))
    
    # Filter out any invalid durations
    valid_durations = largest_cluster_data[largest_cluster_data['duration_minutes'] > 0]['duration_minutes']
    
    if len(valid_durations) > 0:
        # Create histogram with KDE overlay
        ax = plt.gca()
        ax.hist(valid_durations, bins=30, density=True, alpha=0.7, color='skyblue', edgecolor='black')
        valid_durations.plot.kde(ax=ax, color='red', linewidth=2, label='KDE')
        
        plt.xscale('log')  # Log scale for better visualization of skewed data
        plt.xlabel('Job Duration (minutes, log scale)', fontsize=12)
        plt.ylabel('Density', fontsize=12)
        plt.title(f'Job Duration Distribution for Cluster {largest_cluster} (n={len(valid_durations)})', fontsize=14)
        plt.grid(True, alpha=0.3)
        plt.legend()
    else:
        plt.text(0.5, 0.5, 'No valid duration data available', ha='center', va='center')
        plt.title(f'Job Duration Distribution for Cluster {largest_cluster}', fontsize=14)
    
    plt.tight_layout()
    density_plot_path = os.path.join(output_dir, "problem2_density_plot.png")
    plt.savefig(density_plot_path, dpi=100)
    plt.close()
    logger.info(f"Density plot saved to: {density_plot_path}")

def main():
    """Main function to run cluster usage analysis."""
    parser = argparse.ArgumentParser(description="Analyze cluster usage patterns in Spark logs")
    parser.add_argument("master", nargs='?', default="local[*]", 
                        help="Spark master URL (default: local[*])")
    parser.add_argument("--net-id", required=False, 
                        help="Your net ID for S3 bucket access")
    parser.add_argument("--local", action="store_true",
                        help="Use local sample data instead of S3")
    parser.add_argument("--skip-spark", action="store_true",
                        help="Skip Spark processing and only regenerate visualizations from existing CSVs")
    args = parser.parse_args()
    
    output_dir = "data/output"
    
    # If skip-spark is set, just regenerate visualizations
    if args.skip_spark:
        logger.info("Skipping Spark processing, regenerating visualizations from existing CSVs...")
        
        # Load existing CSVs
        timeline_path = os.path.join(output_dir, "problem2_timeline.csv")
        summary_path = os.path.join(output_dir, "problem2_cluster_summary.csv")
        
        if not os.path.exists(timeline_path) or not os.path.exists(summary_path):
            logger.error("CSV files not found. Please run Spark processing first.")
            sys.exit(1)
        
        # Read CSVs
        import pandas as pd
        timeline_df = pd.read_csv(timeline_path)
        cluster_df = pd.read_csv(summary_path)
        
        # Convert to list of dicts for compatibility with visualization function
        timeline_data = timeline_df.to_dict('records')
        cluster_data = cluster_df.to_dict('records')
        
        generate_visualizations(timeline_data, cluster_data, output_dir)
        logger.info("Visualizations regenerated successfully!")
        return
    
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
        timeline_df, cluster_summary_df, unique_clusters, total_apps = \
            analyze_cluster_usage(spark, input_path)
        
        # Save results and generate visualizations
        save_results(timeline_df, cluster_summary_df, unique_clusters, total_apps, output_dir)
        
    except Exception as e:
        logger.error(f"Error during analysis: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
