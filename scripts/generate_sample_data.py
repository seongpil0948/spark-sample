#!/usr/bin/env python3
"""
Generate sample data for testing the Spark application with proper timestamp types
"""

import os
import json
import random
import argparse
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3


def generate_event_data(date_str, num_records=50000):
    """Generate sample event data for a specific date"""

    # Event types
    event_types = ["VIEW", "SEARCH", "ADD_TO_CART", "PURCHASE", "REMOVE_FROM_CART"]
    categories = [
        "Electronics",
        "Clothing",
        "Books",
        "Home",
        "Sports",
        "Toys",
        "Food",
        "Beauty",
    ]
    devices = ["mobile", "desktop", "tablet"]
    browsers = ["Chrome", "Firefox", "Safari", "Edge"]
    referrers = ["google", "facebook", "instagram", "direct", "email"]
    countries = ["US", "UK", "CA", "DE", "FR", "JP", "AU", "BR", "IN", "CN"]

    # Parse the date
    date = datetime.strptime(date_str, "%Y-%m-%d")

    events = []

    for i in range(num_records):
        # Generate timestamp within the date
        hours = random.randint(0, 23)
        minutes = random.randint(0, 59)
        seconds = random.randint(0, 59)

        timestamp = date + timedelta(hours=hours, minutes=minutes, seconds=seconds)

        # Generate event
        event_type = random.choice(event_types)

        # Amount is only for purchase events
        amount = None
        if event_type == "PURCHASE":
            amount = round(random.uniform(10.0, 1000.0), 2)

        # Generate properties as a dictionary
        properties = {
            "session_id": f"session_{random.randint(1000, 9999)}",
            "device_type": random.choice(devices),
            "browser": random.choice(browsers),
            "referrer": random.choice(referrers),
        }

        # Generate tags as a list
        tags = []
        if random.random() > 0.5:
            tags.append("new_user")
        if random.random() > 0.3:
            tags.append("returning")
        if random.random() > 0.8:
            tags.append("vip")
        if event_type == "PURCHASE" and random.random() > 0.6:
            tags.append("discount")
        if date.month in [6, 7, 8, 11, 12]:
            tags.append("seasonal")

        # Generate location as a nested structure
        country = random.choice(countries)
        location = {
            "country": country,
            "city": f"{country}_City_{random.randint(1, 10)}",
            "lat": round(random.uniform(-90, 90), 6),
            "lon": round(random.uniform(-180, 180), 6),
        }

        # Add data quality issues for testing
        # 2% missing event_type (null values)
        if random.random() < 0.02:
            event_type = None

        # 1% duplicate events
        duplicate_count = 1
        if random.random() < 0.01:
            duplicate_count = 2

        for _ in range(duplicate_count):
            event = {
                "event_id": f"evt_{date_str}_{i}_{_}",
                "user_id": random.randint(1, 10000),
                "timestamp": timestamp,
                "event_type": event_type,
                "category": random.choice(categories),
                "amount": amount,
                "properties": properties,
                "tags": tags,
                "location": location,
            }
            events.append(event)

    return events


def save_event_data(events, output_path, use_s3=False, s3_bucket=None, s3_prefix=None):
    """Save events to Parquet format with proper timestamp type
    
    Args:
        events: List of event dictionaries
        output_path: Local or S3 path for output
        use_s3: Whether to save to S3
        s3_bucket: S3 bucket name (if use_s3=True)
        s3_prefix: S3 prefix path (if use_s3=True)
    """

    # Convert to DataFrame
    df = pd.DataFrame(events)

    # Convert timestamp to proper datetime type with UTC timezone
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    # Ensure timestamp is rounded to milliseconds for Spark compatibility
    df["timestamp"] = df["timestamp"].dt.floor("ms")

    # Create PyArrow schema with proper types
    schema = pa.schema(
        [
            ("event_id", pa.string()),
            ("user_id", pa.int64()),
            (
                "timestamp",
                pa.timestamp("ms", tz="UTC"),
            ),  # Explicit timestamp with millisecond precision
            ("event_type", pa.string()),
            ("category", pa.string()),
            ("amount", pa.float64()),
            ("properties", pa.map_(pa.string(), pa.string())),
            ("tags", pa.list_(pa.string())),
            (
                "location",
                pa.struct(
                    [
                        ("country", pa.string()),
                        ("city", pa.string()),
                        ("lat", pa.float64()),
                        ("lon", pa.float64()),
                    ]
                ),
            ),
        ]
    )

    # Split into multiple files for parallel processing
    num_files = 6
    chunk_size = len(df) // num_files

    if use_s3:
        # Initialize S3 client
        session = boto3.Session(profile_name=os.environ.get('AWS_PROFILE', 'toy-root'))
        s3_client = session.client('s3')
        
        for i in range(num_files):
            start_idx = i * chunk_size
            if i == num_files - 1:
                end_idx = len(df)
            else:
                end_idx = (i + 1) * chunk_size

            chunk_df = df.iloc[start_idx:end_idx]
            table = pa.Table.from_pandas(chunk_df, schema=schema, preserve_index=False)
            
            # Write to temporary local file first
            temp_file = f"/tmp/part_{i:04d}.parquet"
            pq.write_table(
                table,
                temp_file,
                compression="snappy",
                version="2.6",
                flavor="spark",
                use_deprecated_int96_timestamps=False,
            )
            
            # Upload to S3
            s3_key = f"{s3_prefix}/{output_path}/part_{i:04d}.parquet"
            s3_client.upload_file(temp_file, s3_bucket, s3_key)
            os.remove(temp_file)  # Clean up temp file
            
        print(f"Saved {len(df)} events to s3://{s3_bucket}/{s3_prefix}/{output_path}")
    else:
        # Create directory if not exists
        os.makedirs(output_path, exist_ok=True)
        
        for i in range(num_files):
            start_idx = i * chunk_size
            if i == num_files - 1:
                end_idx = len(df)
            else:
                end_idx = (i + 1) * chunk_size

            chunk_df = df.iloc[start_idx:end_idx]
            table = pa.Table.from_pandas(chunk_df, schema=schema, preserve_index=False)
            
            # Write as Parquet with Spark-compatible settings
            output_file = os.path.join(output_path, f"part_{i:04d}.parquet")
            pq.write_table(
                table,
                output_file,
                compression="snappy",
                version="2.6",
                flavor="spark",
                use_deprecated_int96_timestamps=False,
            )
        
        print(f"Saved {len(df)} events to {output_path}")


def generate_training_data(use_s3=False, s3_bucket=None, s3_prefix=None):
    """Generate training data for ML pipeline
    
    Args:
        use_s3: Whether to save to S3
        s3_bucket: S3 bucket name (if use_s3=True)
        s3_prefix: S3 prefix path (if use_s3=True)
    """

    print("Generating training data...")

    # Generate feature data
    num_train = 80000
    num_test = 20000

    # Features
    features = []
    for i in range(num_train + num_test):
        feature = {
            "id": i,
            "feature1": random.uniform(0, 100),
            "feature2": random.uniform(0, 100),
            "feature3": random.uniform(0, 100),
            "label": random.randint(0, 1),
        }
        features.append(feature)

    # Split into train and test
    train_df = pd.DataFrame(features[:num_train])
    test_df = pd.DataFrame(features[num_train:])

    # Save as Parquet
    if use_s3:
        # Initialize S3 client
        session = boto3.Session(profile_name=os.environ.get('AWS_PROFILE', 'toy-root'))
        s3_client = session.client('s3')
        
        # Save training data
        train_temp = "/tmp/train_data.parquet"
        train_df.to_parquet(train_temp, engine="pyarrow", compression="snappy", index=False)
        s3_client.upload_file(train_temp, s3_bucket, f"{s3_prefix}/training/train/data.parquet")
        os.remove(train_temp)
        
        # Save test data
        test_temp = "/tmp/test_data.parquet"
        test_df.to_parquet(test_temp, engine="pyarrow", compression="snappy", index=False)
        s3_client.upload_file(test_temp, s3_bucket, f"{s3_prefix}/training/test/data.parquet")
        os.remove(test_temp)
        
        print(f"Saved {len(train_df)} training records and {len(test_df)} test records to s3://{s3_bucket}/{s3_prefix}/training/")
    else:
        train_path = "data/input/training/train"
        test_path = "data/input/training/test"
        
        os.makedirs(train_path, exist_ok=True)
        os.makedirs(test_path, exist_ok=True)
        
        train_df.to_parquet(
            os.path.join(train_path, "data.parquet"),
            engine="pyarrow",
            compression="snappy",
            index=False,
        )
        test_df.to_parquet(
            os.path.join(test_path, "data.parquet"),
            engine="pyarrow",
            compression="snappy",
            index=False,
        )
        
        print(f"Saved {len(train_df)} training records and {len(test_df)} test records to {train_path}")


def generate_streaming_data():
    """Generate streaming event data"""

    print("Generating streaming events...")

    events = []
    base_time = datetime.now()

    for i in range(10000):
        timestamp = base_time + timedelta(seconds=i)
        event = {
            "event_id": f"stream_{i}",
            "timestamp": timestamp.isoformat(),
            "event_type": random.choice(["VIEW", "CLICK", "PURCHASE"]),
            "value": random.uniform(0, 100),
        }
        events.append(json.dumps(event))

    # Save as newline-delimited JSON
    output_path = "data/input/streaming"
    os.makedirs(output_path, exist_ok=True)

    with open(os.path.join(output_path, "events.json"), "w") as f:
        f.write("\n".join(events))

    print(f"Generated {len(events)} streaming events")


def main():
    """Generate all sample data"""
    
    parser = argparse.ArgumentParser(description='Generate sample data for Spark application')
    parser.add_argument('--s3', action='store_true', help='Save data to S3 instead of local filesystem')
    parser.add_argument('--bucket', type=str, default='theshop-lake-dev', help='S3 bucket name')
    parser.add_argument('--prefix', type=str, default='spark/input', help='S3 prefix path')
    parser.add_argument('--dates', nargs='+', default=["2024-01-01", "2024-01-02", "2024-01-03", "2025-06-21"], 
                       help='Dates to generate data for')
    args = parser.parse_args()
    
    # Set AWS profile
    os.environ['AWS_PROFILE'] = os.environ.get('AWS_PROFILE', 'toy-root')
    
    # Generate event data for multiple dates
    for date in args.dates:
        print(f"Generating data for {date}...")
        events = generate_event_data(date)
        save_event_data(events, f"date={date}", use_s3=args.s3, s3_bucket=args.bucket, s3_prefix=args.prefix)

    # Generate training data
    generate_training_data(use_s3=args.s3, s3_bucket=args.bucket, s3_prefix=args.prefix)

    # Generate streaming data (local only for now)
    if not args.s3:
        generate_streaming_data()
    else:
        print("Skipping streaming data generation for S3 mode")

    if args.s3:
        print(f"Sample data generation complete! Data saved to s3://{args.bucket}/{args.prefix}/")
    else:
        print("Sample data generation complete!")


if __name__ == "__main__":
    main()
