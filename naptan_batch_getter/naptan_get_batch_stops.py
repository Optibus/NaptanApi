import boto3
from botocore.exceptions import ClientError

TABLE_NAME = 'Naptan'


def lambda_handler(event, context):

    # Connect to DynamoDB using boto
    dynamo_db = boto3.resource('dynamodb', region_name='eu-west-1')
    stop_ids = event["stop_ids"]
    stops_ids_list = stop_ids.split(',')

    if len(stops_ids_list) > 100:
        print('Cannot support batch get item for more then 100 items')
        raise ClientError
    batch_items_to_list = [dict(StopId=stops_id) for stops_id in stops_ids_list]
    response = dynamo_db.batch_get_item(
        RequestItems={
            TABLE_NAME: {
                'Keys': batch_items_to_list,
            },
        },
    )
    return response
