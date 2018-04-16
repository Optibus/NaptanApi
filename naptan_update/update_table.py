from io import BytesIO

import codecs
import csv
import boto3
from zipfile import ZipFile
from urllib import urlopen
from botocore.exceptions import ClientError
from naptan_create.create_table import create_stop
from naptan_update.exceptions import CannotGetModificationDate, CannotReadFromFile

TMP_FILE_DESTINATION = "/tmp"
NAPTAN_STOPS_URL = "http://naptan.app.dft.gov.uk/DataRequest/Naptan.ashx?format=csv"
REGION = 'eu-west-1'
TABLE_NAME = 'Naptan'
ERROR_MESSAGE = 'Cannot get Naptan last modificition date. Since this row is entered on creation and update in each ' \
                'update, Naptan table is corrupted. Please run create_table method in order to create new table'


def lambda_handler():
    url = urlopen(NAPTAN_STOPS_URL)

    # Downloads and unzips file, saves stops.csv into tmp file
    try:
        with ZipFile(BytesIO(url.read())) as my_zip_file:
            stops_file = my_zip_file.extract('Stops.csv', TMP_FILE_DESTINATION)
    except Exception:
        print("cannot download Naptan data from url")
        raise

    # Connects to DynamoDB using boto library
    dynamo_db = boto3.resource('dynamodb', region_name=REGION)

    # Connects to the DynamoDB table
    table = dynamo_db.Table(TABLE_NAME)

    # Gets latest modification date
    try:
        response = table.get_item(
            Key={
                'StopId': 'ModificationDate'
            }
        )
    except ClientError as e:
        print(ERROR_MESSAGE)
        raise CannotGetModificationDate('{} {}'.format(e.response['Error']['Message'], ERROR_MESSAGE))
    else:
        current_date = response['Item']['ModificationDateTime']
    try:
        with table.batch_writer() as batch:
            with codecs.open(stops_file, 'r', encoding='ascii', errors='ignore') as stops_csv:
                reader = csv.DictReader(stops_csv)
                for idx, row in enumerate(reader):
                    if row['ModificationDateTime'] >= current_date:

                        stop = create_stop(row)
                        # Adds updated stop to DB. If the stop already exists this action will overwrite it
                        batch.put_item(Item=stop)
    except IOError:
        raise CannotReadFromFile("Could not read file: {}".format(stops_file))