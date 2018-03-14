import boto3
from botocore.exceptions import ClientError


def lambda_handler(event, context):

    # Connect to DynamoDB using boto
    dynamo_db = boto3.resource('dynamodb', region_name='eu-west-1')

    # Connect to the DynamoDB table
    table = dynamo_db.Table('Naptan')

    stop_id = event["stopid"]

    try:
        response = table.get_item(
            Key={
                'StopId': stop_id
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']
