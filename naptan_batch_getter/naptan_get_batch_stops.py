import boto3
TABLE_NAME = 'Naptan'
MAX_BATCH_SIZE = 100
REGION = 'eu-west-1'


class BatchSizeError(Exception):
    pass


def lambda_handler(event, context):

    # Connects to DynamoDB using boto library
    dynamo_db = boto3.resource('dynamodb', region_name=REGION)
    stop_ids = event["stop_ids"]
    stops_ids_list = stop_ids.split(',')
    requested_batch_size = len(stops_ids_list)
    if requested_batch_size > MAX_BATCH_SIZE:
        err_msg = 'Max batch size in 100 (batch given is of size {}'.format(requested_batch_size)
        print(err_msg)
        raise BatchSizeError(err_msg)
    batch_items_to_list = [dict(StopId=stops_id) for stops_id in stops_ids_list]
    response = dynamo_db.batch_get_item(
        RequestItems={
            TABLE_NAME: {
                'Keys': batch_items_to_list,
            },
        },
    )
    return response
