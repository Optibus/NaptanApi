from io import BytesIO
import codecs
import csv
import boto3
from zipfile import ZipFile
from urllib import urlopen
from datetime import datetime


NAPTAN_STOPS_URL = "http://naptan.app.dft.gov.uk/DataRequest/Naptan.ashx?format=csv"
REGION = 'eu-west-1'


def create_naptan_table():
    url = urlopen(NAPTAN_STOPS_URL)

    # Downloads and unzips file, saves stops.csv into tmp file

    with ZipFile(BytesIO(url.read())) as zip_file:
        for contained_file in zip_file.namelist():
            with open("/tmp/stops_temp_file.csv", "wb") as output:
                if contained_file == 'Stops.csv':
                    for line in zip_file.open(contained_file).readlines():
                        output.write(line)
                    break

    # Connects to DynamoDB using boto library
    dynamo_db = boto3.resource('dynamodb', region_name=REGION)

    # Connects to the DynamoDB table
    table = dynamo_db.Table('Naptan')
    current_date = datetime.now()
    # Reads temp file and adds each item to dynamodb
    with table.batch_writer() as batch:
        # For the errors='ignore', open using codecs library
        batch.put_item(Item={'StopId': 'ModificationDate', 'ModificationDateTime': current_date})
        with codecs.open("/tmp/stops_temp_file.csv", 'r', encoding='ascii', errors='ignore') as stops_csv:
            reader = csv.DictReader(stops_csv)
            for idx, row in enumerate(reader):
                stop = create_stop(row)
                batch.put_item(Item=stop)


def create_stop(row):
    # By default, convert empty strings to null
    stop = {column: value if value != '' else 'null' for column, value in row.iteritems()}
    stop_id = row['ATCOCode']
    stop.update({'StopId': stop_id})
    return stop
