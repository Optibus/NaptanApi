from io import BytesIO

import codecs
import csv
import boto3
from zipfile import ZipFile
from urllib import urlopen
from botocore.exceptions import ClientError
from naptan_create.create_table import create_stop
from naptan_update.exceptions import CannotGetModificationDate, CannotReadFromFile

TMP_FILE_PATH = "/tmp/stops_temp_file.csv"
NAPTAN_STOPS_URL = "http://naptan.app.dft.gov.uk/DataRequest/Naptan.ashx?format=csv"
REGION = 'eu-west-1'
TABLE_NAME = 'Naptan'


def lambda_handler():
    url = urlopen(NAPTAN_STOPS_URL)

    # Download and unzip file, save Stops.csv in tmp file
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

    # Get latest modification date
    try:
        response = table.get_item(
            Key={
                'StopId': 'ModificationDate'
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
        raise CannotGetModificationDate(e.response['Error']['Message'])
    else:
        modification_date = response['Item']['ModificationDateTime']

    try:

        with codecs.open(TMP_FILE_PATH, 'r', encoding='ascii', errors='ignore') as stops_csv:
            reader = csv.DictReader(stops_csv)
            for idx, row in enumerate(reader):
                # If stop modification date
                if row['ModificationDateTime'] >= modification_date:
                    stop = create_stop(row)

                    # Put updated stop in DB. In case stop already exists this action will overwrite it
                    table.put_item(Item=stop)
    except Exception:
        raise CannotReadFromFile("cannot read from temp file" + TMP_FILE_PATH)

