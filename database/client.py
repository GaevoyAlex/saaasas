import boto3
from botocore.exceptions import ClientError
from typing import Optional, List, Dict, Any
import structlog
from config import Config

logger = structlog.get_logger()

class DynamoDBClient:
    def __init__(self):
        self.dynamodb = boto3.resource(
            'dynamodb',
            region_name=Config.AWS_REGION,
            aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY
        )
        self.table = self.dynamodb.Table(Config.DYNAMODB_TABLE)
        self.logger = logger.bind(component="DynamoDBClient")
        
    def put_item(self, item: Dict[str, Any]) -> bool:
        try:
            self.table.put_item(Item=item)
            return True
        except ClientError as e:
            self.logger.error("Error saving item", error=str(e), pk=item.get('pk'))
            return False
    
    def batch_write(self, items: List[Dict[str, Any]]) -> bool:
        try:
            with self.table.batch_writer() as batch:
                for item in items:
                    batch.put_item(Item=item)
            return True
        except ClientError as e:
            self.logger.error("Error in batch write", error=str(e), items_count=len(items))
            return False
    
    def get_item(self, pk: str, sk: str) -> Optional[Dict[str, Any]]:
        try:
            response = self.table.get_item(Key={'pk': pk, 'sk': sk})
            return response.get('Item')
        except ClientError as e:
            self.logger.error("Error getting item", error=str(e), pk=pk, sk=sk)
            return None
    
    def query_by_pk(self, pk: str, limit: int = 10) -> List[Dict[str, Any]]:
        try:
            response = self.table.query(
                KeyConditionExpression='pk = :pk',
                ExpressionAttributeValues={':pk': pk},
                ScanIndexForward=False,
                Limit=limit
            )
            return response.get('Items', [])
        except ClientError as e:
            self.logger.error("Error querying by pk", error=str(e), pk=pk)
            return []
    
    def query_range(self, pk: str, start_sk: str, end_sk: str) -> List[Dict[str, Any]]:
        try:
            response = self.table.query(
                KeyConditionExpression='pk = :pk AND sk BETWEEN :start_sk AND :end_sk',
                ExpressionAttributeValues={
                    ':pk': pk,
                    ':start_sk': start_sk,
                    ':end_sk': end_sk
                }
            )
            return response.get('Items', [])
        except ClientError as e:
            self.logger.error("Error querying range", error=str(e), pk=pk)
            return []