from io import BytesIO

import codecs
import csv
import boto3
from zipfile import ZipFile
from urllib import urlopen

# dynamodb columns name
from botocore.exceptions import ClientError

from naptan_create.create_table import create_stop

NAPTAN_STOPS_URL = "http://naptan.app.dft.gov.uk/DataRequest/Naptan.ashx?format=csv"
REGION = 'eu-west-1'
TABLE_NAME = 'Naptan'


def lambda_handler():
    url = urlopen(NAPTAN_STOPS_URL)

    # download and unzip file, save Stops.csv in tmp file
    try:
        with ZipFile(BytesIO(url.read())) as my_zip_file:
            for contained_file in my_zip_file.namelist():
                with open("/tmp/stops_temp_file.csv", "wb") as output:
                    if contained_file == 'Stops.csv':
                        for line in my_zip_file.open(contained_file).readlines():
                            output.write(line)
                        break
    except Exception:
        print("cannot download Naptan data from url")
        raise

    # Connect to DynamoDB using boto
    dynamo_db = boto3.resource('dynamodb', region_name=REGION)

    # Connect to the DynamoDB table
    table = dynamo_db.Table(TABLE_NAME)

    # get latest modification date
    try:
        response = table.get_item(
            Key={
                'StopId': 'ModificationDate'
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
        raise
    else:
        modification_date = response['Item']['ModificationDate']

    try:
        with codecs.open("/tmp/stops_temp_file.csv", 'r', encoding='ascii', errors='ignore') as stops_csv:
            reader = csv.DictReader(stops_csv)
            for idx, row in enumerate(reader):
                # if stop modification date
                if row['ModificationDateTime'] >= modification_date:
                    # todo verify that delete_item is safe to in case item does not exists
                    # try to delete the item, assuming that if item does not exists, action will just do nothing
                    table.delete_item(Key={'StopId': row['ATCOCode']})
                    stop = create_stop(row)
                    # put updated stop in DB
                    table.put_item(Item=stop)
    except Exception:
        print("cannot read from temp file")
        raise

