import boto3
import csv
import sys

# get a handle on s3
s3 = boto3.resource(u's3')

# get dynamodb connections
dynamodb = boto3.client('dynamodb')
headers = None


def insertDynamoItem(tablename, item_lst):
    item_dict = {}
    item_dict[headers[0]] = {'N': item_lst[0]}
    item_dict[headers[1]] = {'S': item_lst[1]}
    item_dict[headers[2]] = {'SS': [item_lst[2]]}

    response = dynamodb.put_item(
        TableName=tablename,
        Item=item_dict
    )


def lambda_handler(event, context):
    global headers
    bucket = s3.Bucket(event['Records'][0]['s3']['bucket']['name'])
    obj = bucket.Object(key=event['Records'][0]['s3']['object']['key'])

    # get the object
    response = obj.get()

    # read the contents of the file and split it into a list of lines

    data = response[u'Body'].read().decode('utf-8-sig').splitlines()
    lines = csv.reader(data)
    headers = next(lines)

    for line in lines:
        # Upload Content to DynamoDB Table
        insertDynamoItem('Order1234', line)
