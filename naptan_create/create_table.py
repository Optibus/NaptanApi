from io import BytesIO
import codecs
import csv
import boto3
from zipfile import ZipFile
from urllib import urlopen

# dynamodb columns name
COLUMNS = ['ATCOCode', 'NaptanCode', 'PlateCode', 'CleardownCode',
           'CommonName', 'CommonNameLang', 'ShortCommonName', 'ShortCommonNameLang',
           'Landmark', 'LandmarkLang', 'Street', 'StreetLang', 'Crossing',
           'CrossingLang', 'Indicator', 'IndicatorLang', 'Bearing', 'NptgLocalityCode',
           'LocalityName', 'ParentLocalityName', 'GrandParentLocalityName', 'Town',
           'TownLang', 'Suburb', 'SuburbLang', 'LocalityCentre', 'GridType', 'Easting',
           'Northing', 'Longitude', 'Latitude', 'StopType', 'BusStopType',
           'TimingStatus', 'DefaultWaitTime', 'Notes', 'NotesLang',
           'AdministrativeAreaCode', 'CreationDateTime', 'ModificationDateTime',
           'RevisionNumber', 'Modification', 'Status']


def lambda_handler():
    url = urlopen("http://naptan.app.dft.gov.uk/DataRequest/Naptan.ashx?format=csv")

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
    dynamo_db = boto3.resource('dynamodb', region_name='eu-west-1')

    # Connect to the DynamoDB table
    table = dynamo_db.Table('Naptan')

    # read temp file and add each item to dynamodb
    with table.batch_writer() as batch:
        try:
            with codecs.open("/tmp/stops_temp_file.csv", 'r', encoding='ascii', errors='ignore') as stops_csv:
                reader = csv.DictReader(stops_csv)
                for idx, row in enumerate(reader):
                    # by default, convert empty strings to null
                    stop = create_stop(row)
                    batch.put_item(Item=stop)
        except Exception:
            print("cannot read from temp file")
            raise


def create_stop(row):
    stop = {column: row[column] if row[column] != '' else 'null' for column in COLUMNS}
    stop_id = row['ATCOCode']
    stop.update({'StopId': stop_id})
    return stop

