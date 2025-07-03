#!/usr/bin/env python3
"""
DynamoDB Table Copier

A reusable script for copying data between DynamoDB tables with configurable parameters.
Supports cross-account, cross-region, and local-to-cloud migrations.

Features:
- Schema-preserving table creation
- Configurable batch operations
- Progress tracking with tqdm
- Safe table deletion with confirmation
- Data validation with item counting
- Flexible endpoint configuration

Usage:
  python copy_table.py --source-table SOURCE --dest-table DESTINATION [OPTIONS]

Example:
  python copy_table.py \
    --source-table dev-orders \
    --dest-table prod-orders \
    --source-endpoint http://localhost:8000 \
    --dest-endpoint https://dynamodb.us-west-2.amazonaws.com \
    --batch-size 25
"""

import argparse
import boto3
import time
import logging
import sys
from botocore.exceptions import ClientError, WaiterError
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def get_table_schema(client, table_name):
    """Fetch table schema including key schema and attribute definitions"""
    try:
        response = client.describe_table(TableName=table_name)
        return {
            'KeySchema': response['Table']['KeySchema'],
            'AttributeDefinitions': response['Table']['AttributeDefinitions'],
            'BillingMode': response['Table'].get('BillingMode', 'PAY_PER_REQUEST'),
            'GlobalSecondaryIndexes': response['Table'].get('GlobalSecondaryIndexes', []),
            'LocalSecondaryIndexes': response['Table'].get('LocalSecondaryIndexes', [])
        }
    except ClientError as e:
        logger.error(f"Error describing table {table_name}: {e.response['Error']['Message']}")
        sys.exit(1)

def create_destination_table(client, table_name, schema):
    """Create destination table with schema from source"""
    try:
        logger.info(f"Creating table {table_name}...")
        client.create_table(
            TableName=table_name,
            KeySchema=schema['KeySchema'],
            AttributeDefinitions=schema['AttributeDefinitions'],
            BillingMode=schema['BillingMode'],
            GlobalSecondaryIndexes=schema['GlobalSecondaryIndexes'],
            LocalSecondaryIndexes=schema['LocalSecondaryIndexes']
        )
        
        # Wait for table to become active
        waiter = client.get_waiter('table_exists')
        waiter.wait(TableName=table_name, WaiterConfig={'Delay': 1, 'MaxAttempts': 30})
        logger.info(f"Table {table_name} created and active")
    except ClientError as e:
        logger.error(f"Error creating table: {e.response['Error']['Message']}")
        sys.exit(1)

def copy_table_data(source_client, dest_client, source_table, dest_table, batch_size=25):
    """Copy data between tables with batch processing"""
    source = boto3.resource('dynamodb', client=source_client).Table(source_table)
    dest = boto3.resource('dynamodb', client=dest_client).Table(dest_table)
    
    paginator = source_client.get_paginator('scan')
    page_iterator = paginator.paginate(
        TableName=source_table,
        PaginationConfig={'PageSize': 1000}
    )
    
    total_items = 0
    processed_items = 0
    
    # Get initial count for progress bar
    try:
        count_response = source_client.describe_table(TableName=source_table)
        total_items = count_response['Table']['ItemCount']
    except ClientError:
        logger.warning("Couldn't get item count, progress bar will be indeterminate")
    
    with tqdm(total=total_items, desc="Copying items", unit="item") as pbar:
        with dest.batch_writer() as batch:
            for page in page_iterator:
                for item in page['Items']:
                    try:
                        batch.put_item(Item=item)
                        processed_items += 1
                        pbar.update(1)
                    except ClientError as e:
                        logger.error(f"Error copying item {item.get('id', 'unknown')}: {e.response['Error']['Message']}")
                    
                    # Throttling control
                    if processed_items % 100 == 0:
                        time.sleep(0.01)
    
    return processed_items

def validate_copy(source_client, dest_client, source_table, dest_table):
    """Compare item counts between source and destination tables"""
    try:
        source_count = source_client.describe_table(TableName=source_table)['Table']['ItemCount']
        dest_count = dest_client.describe_table(TableName=dest_table)['Table']['ItemCount']
        
        logger.info(f"Source table items: {source_count}")
        logger.info(f"Destination table items: {dest_count}")
        
        if source_count != dest_count:
            logger.warning(f"Count mismatch: {abs(source_count - dest_count)} items difference")
            return False
        return True
    except ClientError as e:
        logger.error(f"Validation failed: {e.response['Error']['Message']}")
        return False

def main():
    parser = argparse.ArgumentParser(description='DynamoDB Table Copier')
    parser.add_argument('--source-table', required=True, help='Source table name')
    parser.add_argument('--dest-table', required=True, help='Destination table name')
    parser.add_argument('--source-endpoint', default='http://localhost:8000', 
                        help='Source endpoint URL (default: http://localhost:8000)')
    parser.add_argument('--dest-endpoint', default='http://localhost:8000', 
                        help='Destination endpoint URL (default: http://localhost:8000)')
    parser.add_argument('--source-region', default='us-west-2', help='Source AWS region')
    parser.add_argument('--dest-region', default='us-west-2', help='Destination AWS region')
    parser.add_argument('--source-profile', help='AWS profile for source table')
    parser.add_argument('--dest-profile', help='AWS profile for destination table')
    parser.add_argument('--overwrite-dest', action='store_true', 
                        help='Overwrite existing destination table')
    parser.add_argument('--validate', action='store_true', 
                        help='Validate item counts after copy')
    parser.add_argument('--batch-size', type=int, default=25, 
                        help='Batch size for writes (default: 25)')
    parser.add_argument('--delete-source', action='store_true', 
                        help='Delete source table after copy (with confirmation)')
    parser.add_argument('--log-level', default='INFO', 
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help='Logging level')
    
    args = parser.parse_args()
    logger.setLevel(args.log_level)

    # Configure AWS clients
    source_session = boto3.Session(
        profile_name=args.source_profile,
        region_name=args.source_region
    )
    dest_session = boto3.Session(
        profile_name=args.dest_profile,
        region_name=args.dest_region
    )
    
    source_client = source_session.client(
        'dynamodb', 
        endpoint_url=args.source_endpoint,
        aws_access_key_id=os.getenv('SOURCE_AWS_ACCESS_KEY_ID', 'dummy'),
        aws_secret_access_key=os.getenv('SOURCE_AWS_SECRET_ACCESS_KEY', 'dummy')
    )
    
    dest_client = dest_session.client(
        'dynamodb', 
        endpoint_url=args.dest_endpoint,
        aws_access_key_id=os.getenv('DEST_AWS_ACCESS_KEY_ID', 'dummy'),
        aws_secret_access_key=os.getenv('DEST_AWS_SECRET_ACCESS_KEY', 'dummy')
    )

    # Check destination table existence
    dest_tables = dest_client.list_tables().get('TableNames', [])
    if args.dest_table in dest_tables:
        if args.overwrite_dest:
            logger.warning(f"Deleting existing table {args.dest_table}...")
            dest_client.delete_table(TableName=args.dest_table)
            try:
                waiter = dest_client.get_waiter('table_not_exists')
                waiter.wait(TableName=args.dest_table)
            except WaiterError:
                logger.error("Timed out waiting for table deletion")
                sys.exit(1)
        else:
            logger.info(f"Destination table {args.dest_table} already exists")

    # Create destination table if needed
    if args.dest_table not in dest_client.list_tables().get('TableNames', []):
        schema = get_table_schema(source_client, args.source_table)
        create_destination_table(dest_client, args.dest_table, schema)

    # Perform data copy
    logger.info("Starting data copy...")
    copied_count = copy_table_data(
        source_client,
        dest_client,
        args.source_table,
        args.dest_table,
        args.batch_size
    )
    logger.info(f"Copied {copied_count} items to {args.dest_table}")

    # Validation
    if args.validate:
        logger.info("Validating copy...")
        if validate_copy(source_client, dest_client, args.source_table, args.dest_table):
            logger.info("Validation successful!")
        else:
            logger.warning("Validation issues detected")

    # Source table deletion
    if args.delete_source:
        confirm = input(f"Delete source table {args.source_table}? (y/n): ").lower()
        if confirm == 'y':
            source_client.delete_table(TableName=args.source_table)
            logger.info(f"Source table {args.source_table} deleted")
        else:
            logger.info("Source table preserved")

if __name__ == '__main__':
    main()