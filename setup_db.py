#!/usr/bin/env python3

import boto3
from botocore.exceptions import ClientError
import click

from config import Config


def create_dynamodb_table():
    dynamodb = boto3.client(
        'dynamodb',
        region_name=Config.AWS_REGION,
        aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY
    )
    
    table_name = Config.DYNAMODB_TABLE
    
    try:
        response = dynamodb.describe_table(TableName=table_name)
        click.echo(f"✅ Table {table_name} already exists")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] != 'ResourceNotFoundException':
            click.echo(f"❌ Error checking table: {e}")
            return False
    
    # Создание таблицы
    table_definition = {
        'TableName': table_name,
        'KeySchema': [
            {'AttributeName': 'pk', 'KeyType': 'HASH'},
            {'AttributeName': 'sk', 'KeyType': 'RANGE'}
        ],
        'AttributeDefinitions': [
            {'AttributeName': 'pk', 'AttributeType': 'S'},
            {'AttributeName': 'sk', 'AttributeType': 'S'},
            {'AttributeName': 'entity_type', 'AttributeType': 'S'},
            {'AttributeName': 'symbol', 'AttributeType': 'S'}
        ],
        'BillingMode': 'PAY_PER_REQUEST',
        'GlobalSecondaryIndexes': [
            {
                'IndexName': 'EntityTypeIndex',
                'KeySchema': [
                    {'AttributeName': 'entity_type', 'KeyType': 'HASH'},
                    {'AttributeName': 'sk', 'KeyType': 'RANGE'}
                ],
                'Projection': {'ProjectionType': 'ALL'}
            },
            {
                'IndexName': 'SymbolIndex',
                'KeySchema': [
                    {'AttributeName': 'symbol', 'KeyType': 'HASH'},
                    {'AttributeName': 'sk', 'KeyType': 'RANGE'}
                ],
                'Projection': {'ProjectionType': 'ALL'}
            }
        ]
    }
    
    try:
        response = dynamodb.create_table(**table_definition)
        click.echo(f"✅ Table {table_name} created successfully")
        
        # Включение TTL
        try:
            dynamodb.update_time_to_live(
                TableName=table_name,
                TimeToLiveSpecification={
                    'AttributeName': 'ttl',
                    'Enabled': True
                }
            )
            click.echo("✅ TTL enabled")
        except Exception as e:
            click.echo(f"⚠️ Warning: Could not enable TTL: {e}")
        
        return True
        
    except ClientError as e:
        click.echo(f"❌ Error creating table: {e}")
        return False

@click.command()
def setup():
    """Создать таблицу DynamoDB"""
    click.echo("Setting up DynamoDB table...")
    
    if create_dynamodb_table():
        click.echo("🎉 Database setup completed successfully!")
    else:
        click.echo("💥 Database setup failed!")
        exit(1)

if __name__ == "__main__":
    setup()