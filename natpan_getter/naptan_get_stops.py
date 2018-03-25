import boto3
REGION = 'eu-west-1'
TABLE_NAME = 'Naptan'


def lambda_handler(event, context):

    # Connect to DynamoDB using boto
    dynamo_db = boto3.resource('dynamodb', region_name=REGION)

    # Connect to the DynamoDB table
    table = dynamo_db.Table(TABLE_NAME)

    stop_id = event["stopid"]

    response = table.get_item(
        Key={
            'StopId': stop_id
        }
    )
    return response

