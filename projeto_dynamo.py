import boto3
import decimal
import requests
import json

aws_access_key_id='crendencias'
aws_secret_access_key='credencias'
aws_session_token='crendeciais'

resource_s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id,
                               aws_secret_access_key=aws_secret_access_key,
                               aws_session_token=aws_session_token, use_ssl=False)

name = "name-bucket"
resource_s3.create_bucket(Bucket=name)
bucket = resource_s3.Bucket('name-bucket')



#Criando tabelas
dynamodb = boto3.resource('dynamodb', aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        aws_session_token=aws_session_token
                       ,region_name = "us-east-1", use_ssl=False)



#tabela capsule
table_name = 'capsule' 
params = {
        'TableName': 'capsule',
        
        'KeySchema': [
            {'AttributeName': 'capsule_serial', 'KeyType': 'HASH'}, 
            {'AttributeName': 'capsule_id', 'KeyType': 'RANGE'} 
        ],
        
        'AttributeDefinitions': [ 
            {'AttributeName': 'capsule_serial', 'AttributeType': 'S'}, 
            {'AttributeName': 'capsule_id', 'AttributeType': 'S'} 
        ],
        
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 3,
            'WriteCapacityUnits': 3
        }
    }
dynamodb.create_table(**params)

#tabela rocket
table_name = 'rocket' 
params = {
        'TableName': 'rocket',
        
        'KeySchema': [
            {'AttributeName': 'id', 'KeyType': 'HASH'}
            
        ],
        
        'AttributeDefinitions': [ 
            {'AttributeName': 'id', 'AttributeType': 'N'}
            
        ],
        
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 3,
            'WriteCapacityUnits': 3
        }
    }
dynamodb.create_table(**params.astype(decimal))

#tabela dragon
table_name = 'dragon' 
params = {
        'TableName': 'dragon',
        
        'KeySchema': [
            {'AttributeName': 'id', 'KeyType': 'HASH'}
            
        ],
        
        'AttributeDefinitions': [ 
            {'AttributeName': 'id', 'AttributeType': 'S'}
            
        ],
        
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 3,
            'WriteCapacityUnits': 3
        }
    }
dynamodb.create_table(**params)

# Inserindo dados na tabela 'capsule'
response = requests.get('https://api.spacexdata.com/v3/capsules')
data = response.json()
table = dynamodb.Table('capsule')
for item in data:
    table.put_item(Item=item)
    bucket.put_object(Key='capsule/json/capsule.json', Body=response.content)


#Inserindo dados na tabela 'rocket'
rockets = dynamodb.Table('rocket')
response = requests.get('https://api.spacexdata.com/v3/rockets')
js_rocket = response.json()
dados_rocket = json.loads(json.dumps(js_rocket), parse_float=decimal.Decimal)
for i in dados_rocket:
  rockets.put_item(Item=i)
  bucket.put_object(Key='rocket/json/rocket.json', Body=response.content)


# Inserindo dados na tabela 'dragon'
dragon = dynamodb.Table('dragon')
response = requests.get('https://api.spacexdata.com/v3/dragons')
dragon_js = response.json()
dados = json.loads(json.dumps(dragon_js), parse_float=decimal.Decimal)
for i in dados:
  dragon.put_item(Item=i)
  #Inserindo dragon.json no s3
bucket.put_object(Key='dragon/json/dragon.json', Body=response.content)