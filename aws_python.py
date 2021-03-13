import boto3
import csv

s3 = boto3.resource('s3',
    aws_access_key_id='AKIA6EYCMYMG4WP5UCEA',
    aws_secret_access_key='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' )

try:
    s3.create_bucket(Bucket='quinnbucket', CreateBucketConfiguration={
        'LocationConstraint': 'us-east-2'})
except:
    print("this may already exist")

bucket = s3.Bucket("quinnbucket")
bucket.Acl().put(ACL='public-read')

body = open('IMG_2167.JPG', 'rb')

o = s3.Object('quinnbucket', 'test').put(Body=body )
s3.Object('quinnbucket', 'test').Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb',
    region_name='us-east-2',
    aws_access_key_id='KEY HERE',
    aws_secret_access_key='KEY HERE'
)

try:
    table = dyndb.create_table(
        TableName='DataTable',
        KeySchema=[
            {
                'AttributeName': 'PartitionKey',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'RowKey',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'PartitionKey',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'RowKey',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except:
    table = dyndb.Table("DataTable")
    print("hello")

table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

print(table.item_count)

with open('experiments.csv', 'r') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    next(csvf)
    for item in csvf:
        print(item)
        body = open(item[4], 'rb')
        s3.Object('quinnbucket', item[4]).put(Body=body )
        md = s3.Object('quinnbucket', item[4]).Acl().put(ACL='public-read')

        url = " https://s3-us-east-2.amazonaws.com/datacont-name/" + item[4]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
                'description' : item[3], 'date' : item[2], 'url':url}
        try:
            table.put_item(Item=metadata_item)
        except:
            print("item may already be there or another failure")

response = table.get_item(
    Key={
        'PartitionKey': 'experiment2',
        'RowKey': 'data2'
    }
)
item = response['Item']
print(item)
