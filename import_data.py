#!/usr/bin/env python3
"""
DynamoDB JSON Data Importer

A reusable script for importing JSON data into AWS DynamoDB with configurable parameters.
Supports both local and remote DynamoDB instances.

Features:
- Configurable via command-line arguments
- Environment variable support for credentials
- Dynamic batch processing
- Comprehensive error handling
- UUID generation for missing IDs
- Progress tracking

Usage:
  python dynamodb_importer.py --table-name TABLE_NAME --data-dir PATH [OPTIONS]

Example:
  python dynamodb_importer.py \
    --table-name my-table \
    --data-dir ./data \
    --endpoint-url http://localhost:8000 \
    --region us-west-2 \
    --batch-size 25
"""

import argparse
import boto3
import json
import os
import glob
import sys
import logging
from uuid import uuid4
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def convert_value(value):
    """Recursively convert Python types to DynamoDB attribute types"""
    if isinstance(value, str):
        return {'S': value}
    elif isinstance(value, bool):
        return {'BOOL': value}
    elif isinstance(value, (int, float)):
        return {'N': str(value)}
    elif value is None:
        return {'NULL': True}
    elif isinstance(value, list):
        return {'L': [convert_value(v) for v in value]}
    elif isinstance(value, dict):
        # Check if already in DynamoDB format
        if any(key in value for key in ('S', 'N', 'B', 'BOOL', 'NULL', 'M', 'L')):
            return value
        return {'M': {k: convert_value(v) for k, v in value.items()}}
    else:
        return {'S': str(value)}

def process_file(file_path, table_name, client, batch_size=25):
    """Process a single JSON file and import its contents"""
    with open(file_path, 'r') as f:
        batch = []
        item_count = 0
        batch_count = 0
        error_count = 0

        for line_number, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            try:
                item = json.loads(line)
                item_count += 1

                # Convert to DynamoDB format
                converted_item = {}
                for key, value in item.items():
                    converted_item[key] = convert_value(value)

                # Ensure ID exists
                if 'id' not in converted_item or not converted_item.get('id'):
                    converted_item['id'] = {'S': str(uuid4())}

                batch.append({'PutRequest': {'Item': converted_item}})

                # Process batch when full
                if len(batch) >= batch_size:
                    processed, errors = write_batch(client, table_name, batch)
                    batch_count += processed
                    error_count += errors
                    batch = []

            except json.JSONDecodeError as e:
                logger.error(f"JSON error in {file_path}:{line_number} - {str(e)}")
                error_count += 1
            except Exception as e:
                logger.error(f"Unexpected error in {file_path}:{line_number} - {str(e)}")
                error_count += 1

        # Process final batch
        if batch:
            processed, errors = write_batch(client, table_name, batch)
            batch_count += processed
            error_count += errors

    return item_count, batch_count, error_count

def write_batch(client, table_name, batch):
    """Write a batch of items and handle unprocessed items"""
    try:
        response = client.batch_write_item(RequestItems={table_name: batch})
        unprocessed = response.get('UnprocessedItems', {}).get(table_name, [])
        
        if unprocessed:
            logger.warning(f"Unprocessed items: {len(unprocessed)}")
            # Retry logic could be added here
        
        return 1, len(unprocessed)  # (batches processed, errors)
    except ClientError as e:
        logger.error(f"AWS Client Error: {e.response['Error']['Message']}")
        return 0, len(batch)
    except Exception as e:
        logger.error(f"Batch write failed: {str(e)}")
        return 0, len(batch)

def main():
    parser = argparse.ArgumentParser(description='DynamoDB JSON Data Importer')
    parser.add_argument('--table-name', required=True, help='DynamoDB table name')
    parser.add_argument('--data-dir', required=True, help='Directory containing JSON files')
    parser.add_argument('--endpoint-url', default='http://localhost:8000', 
                        help='DynamoDB endpoint URL (default: http://localhost:8000)')
    parser.add_argument('--region', default='us-west-2', help='AWS region (default: us-west-2)')
    parser.add_argument('--batch-size', type=int, default=25, 
                        help='Batch size for writes (1-25, default: 25)')
    parser.add_argument('--log-level', default='INFO', 
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help='Logging level')
    
    args = parser.parse_args()
    logger.setLevel(args.log_level)
    
    # Validate batch size
    if not 1 <= args.batch_size <= 25:
        logger.error("Batch size must be between 1 and 25")
        sys.exit(1)

    # Configure AWS credentials
    session = boto3.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'dummy'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'dummy'),
        region_name=args.region
    )
    
    client = session.client('dynamodb', endpoint_url=args.endpoint_url)
    
    # Find JSON files
    files = glob.glob(os.path.join(args.data_dir, '*.json'))
    if not files:
        logger.warning(f"No JSON files found in {args.data_dir}")
        return
    
    logger.info(f"Found {len(files)} JSON files in {args.data_dir}")
    
    total_items = 0
    total_errors = 0
    
    for i, file_path in enumerate(files, 1):
        logger.info(f"Processing file {i}/{len(files)}: {os.path.basename(file_path)}")
        item_count, batch_count, error_count = process_file(
            file_path,
            args.table_name,
            client,
            args.batch_size
        )
        
        total_items += item_count
        total_errors += error_count
        
        logger.info(f"  Items: {item_count} | Batches: {batch_count} | Errors: {error_count}")
    
    logger.info(f"Import completed! Total items: {total_items} | Total errors: {total_errors}")
    if total_errors:
        logger.warning(f"{total_errors} errors encountered during import")

if __name__ == '__main__':
    main()