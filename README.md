# DynamoDB Data Commander 🚀

![DynamoDB Logo](https://icon.icepanel.io/AWS/svg/Database/DynamoDB.svg)

A Python CLI toolkit for DynamoDB data operations. Import JSON datasets and copy tables with simple commands.

## Features

- **Bulk JSON Importer**:  
  Import millions of JSON records into DynamoDB with automatic type conversion
- **Table-to-Table Copier**:  
  Copy data between DynamoDB tables (same or cross-account)
- **Dynamic Configuration**:  
  Support for local/remote DynamoDB endpoints
- **Robust Error Handling**:  
  Detailed logging and error recovery
- **Batch Processing**:  
  Optimized 25-item batch writes with retry logic

## Requirements

- Python 3.8+
- **Dependencies**:  
  ```text
  boto3==1.34.0
  tqdm==4.65.0  # For progress bars (optional)

## Installation
```bash
git clone https://github.com/your-username/dynamodb-data-commander.git
cd dynamodb-data-commander
pip install -r requirements.txt
```

## Tools
- **JSON Importer (import_data.py)**:  
Import JSON files into DynamoDB:
```bash
python import_data.py \
  --table-name MyTable \
  --data-dir ./datasets \
  --endpoint-url http://localhost:8000 \
  --batch-size 25
```
| Argument         | Description                     | Default               |
|------------------|----------------------------------|------------------------|
| `--table-name`   | Target DynamoDB table name       | Required               |
| `--data-dir`     | Directory with JSON files        | Required               |
| `--endpoint-url` | DynamoDB endpoint URL            | http://localhost:8000  |
| `--region`       | AWS region name                  | us-west-2              |
| `--batch-size`   | Write batch size (1-25)          | 25                     |
| `--log-level`    | Logging level (DEBUG/INFO/ERROR) | INFO                   |

- **Table Copier (copy_table.py)**:
Copy between DynamoDB tables:
```bash
python copy_table.py \
  --source-table TableA \
  --dest-table TableB \
  --source-endpoint http://localhost:8000 \
  --dest-endpoint https://dynamodb.us-west-2.amazonaws.com \
  --batch-size 25 \
  --overwrite-dest \
  --validate
```
| Argument           | Description                          | Default                 |
|--------------------|--------------------------------------|-------------------------|
| `--source-table`   | Source table name                    | Required                |
| `--dest-table`     | Destination table name               | Required                |
| `--source-endpoint`| Source endpoint URL                  | `http://localhost:8000` |
| `--dest-endpoint`  | Destination endpoint URL             | `http://localhost:8000` |
| `--source-region`  | Source AWS region                    | `us-west-2`             |
| `--dest-region`    | Destination AWS region               | `us-west-2`             |
| `--overwrite-dest` | Overwrite existing destination table | `False`                 |
| `--validate`       | Validate item counts after copy      | `False`                 |
| `--batch-size`     | Batch write size                     | `25`                    |
| `--delete-source`  | Delete source table after copy       | `False`                 |

## Configurations
Set credentials via environment variables:
```bash
# For local DynamoDB
export AWS_ACCESS_KEY_ID=dummy
export AWS_SECRET_ACCESS_KEY=dummy

# For AWS environments
export AWS_PROFILE=production

# For cross-account copying
export SOURCE_AWS_ACCESS_KEY_ID=source_key
export SOURCE_AWS_SECRET_ACCESS_KEY=source_secret
export DEST_AWS_ACCESS_KEY_ID=dest_key
export DEST_AWS_SECRET_ACCESS_KEY=dest_secret
```

## Sample Workflow
1-Import test data to local DynamoDB:
```bash
python import_data.py \
  --table-name dev-orders \
  --data-dir ./testdata \
  --endpoint-url http://localhost:8000
```
1-Copy table to production:
```bash
python copy_table.py \
  --source-table dev-orders \
  --dest-table prod-orders \
  --source-endpoint http://localhost:8000 \
  --dest-endpoint https://dynamodb.us-west-2.amazonaws.com \
  --source-profile dev \
  --dest-profile prod \
  --overwrite-dest \
  --validate
```
