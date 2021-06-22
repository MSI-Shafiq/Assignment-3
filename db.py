from tkinter.constants import NO
import boto3
import random
from decimal import Decimal, localcontext
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError
from urllib3.util import retry

# User-defined Modules
from config import Config

class DynamoDB(object):
    def __init__(
        self, table_name: str, region: str, aws_access_id: str, aws_secret: str
    ) -> None:
        self.table_name = table_name
        self.region = region
        self.aws_access_id = aws_access_id
        self.aws_secret = aws_secret
        self.client = boto3.client(
            service_name = 'dynamodb',
            region_name = self.region,
            aws_access_key_id = self.aws_access_id,
            aws_secret_access_key = self.aws_secret
        )
    
    def _serializer(self, item: dict) -> dict:
        try:
            serialzer = TypeSerializer()
            return {key: serialzer.serialize(value=value) for key, value in item.items()}
        except Exception as e:
            raise Exception(e)
    
    def _deserializer(self, item: dict) -> dict:
        try:
            deserializer = TypeDeserializer()
            return {key: deserializer.deserialize(value=value) for key, value in item.items()}
        except Exception as e:
            raise Exception(e)
        
    def _update_serializer(self, item: dict) -> dict:
        try:
            serialzer = TypeSerializer()
            return {key: {'Value': serialzer.serialize(value=value), 'Action': 'PUT'} for key, value in item.items()}
        except Exception as e:
            raise Exception(e)

    def insert(self, item: dict) -> dict:
        try:
            response = self.client.put_item(
                TableName = self.table_name,
                Item = self._serializer(item),
                ConditionExpression = 'attribute_not_exists(id)'
            )
            return response
        except self.client.exceptions.ConditionalCheckFailedException as e:
            response = dict()
            response['status'] = 'failure'
            response['msg'] = 'id Already Exists'
            return response
        except Exception as e:
            response = dict()
            response['status'] = 'failure'
            response['msg'] = str(e)
            return response
    
    def get(self, item: dict) -> dict:
        try:
            response = self.client.get_item(
                TableName = self.table_name,
                Key = self._serializer(item=item)
            )
            res = dict()
            res['status'] = 'success'
            if response.get('Item') is None:
                res['Item'] = None
                return res
            res['Item'] = self._deserializer(item=response.get('Item'))
            return res
        except Exception as e:
            response = dict()
            response['status'] = 'failure'
            response['msg'] = str(e)
            return response
    
    def update(self, id: str, item: dict) ->dict:
        try:
            response = self.client.update_item(
                TableName = self.table_name,
                Key = self._serializer(item={'id': id}),
                AttributeUpdates = self._update_serializer(item=item),
                Expected = {'id': {'Value': {'S': id}, 'Exists': True}}
            )
            return response
        except self.client.exceptions.ConditionalCheckFailedException as e:
            response = dict()
            response['status'] = 'failure'
            response['msg'] = f'id : {id} not found'
            return response
        except Exception as e:
            response = dict()
            response['status'] = 'failure'
            response['msg'] = str(e)
            return response

    def delete(self, id: str) -> dict:
        try:
            response = self.client.delete_item(
                TableName = self.table_name,
                Key = self._serializer(item={'id': id}),
                Expected = {'id': {'Value': {'S': id}, 'Exists': True}}
            )
            return response
        except self.client.exceptions.ConditionalCheckFailedException as e:
            response = dict()
            response['status'] = 'failure'
            response['msg'] = f'id : {id} not found'
            return response
        except Exception as e:
            response = dict()
            response['status'] = 'failure'
            response['msg'] = str(e)
            return response
    
    def query(self, filters: dict) -> dict:
        try:
            response = self.client.scan(
                TableName = self.table_name,
                ScanFilter = filters
            )
            result = dict()
            result['status'] = 'success'
            result['Items'] = [self._deserializer(item=Item) for Item in response.get('Items')]
            return result
        except Exception as e:
            response = dict()
            response['status'] = 'failure'
            response['msg'] = str(e)
            return response

dynamoDb = DynamoDB(
    table_name = Config.DATABASE_NAME,
    region = Config.REGION,
    aws_access_id = Config.ACCESS_ID,
    aws_secret = Config.SECRET
)

# Create a new Entry
# item = {
#     'id': str(random.randint(a=100000, b=999999)),
#     'product': 'Notebooks',
#     'stock': int(250),
#     'price': Decimal('78.00')
# }
# print(dynamoDb.insert(item))

# Get
# get_item = {'id': '799764'}
# print(dynamoDb.get(item=get_item))

# Update
# update = {'stock': int(30)}
# print(dynamoDb.update(id='799764', item=update))

# Delete
# print(dynamoDb.delete(id='861564'))

# query
# filters = {
#     'product': {
#         'AttributeValueList': [{'S': 'Pens'}],
#         'ComparisonOperator': 'EQ'
#     }
# }
# print(dynamoDb.query(filters=filters))