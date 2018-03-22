
import boto3
from urllib import urlopen


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

import csv

def get_stops():
    stops = ['370021822', '1000DHSR3494', '1000DBHR3769', '1000DBHR3770', '370030941', '370030938', '370021266',
             '370027235', '370027175', '370035494', '370031069', '1000DHCR3701', '370030955', '370030250', '370022585',
             '370026682', '1000DBHR3771', '1000DBHR3772', '370041168', '370045202', '370045471', '370031070',
             '370022430', '370022431', '370023194', '370023193', '370021581', '370022318', '370021586', '370022327',
             '370022326', '370023640', '370023700', '370030988', '370031045', '370035277', '370021630', '370035298',
             '370030252', '370031257', '370022266']

    stops1 = ['370021364', '370023377', '370021471', '370021470', '370022880', '370030819', '370030892', '370020846',
              '370035082', '370021689', '370023382', '370035085', '370022871', '370021360', '370022509', '370023285',
              '370030741', '370027009', '370027297', '1000DKMS8135', '1000DKMS8136', '370035897', '370045635',
              '370045636', '1000DHCR3736', '1000DHCR3737', '370020704', '370020500', '370010225', '370010228',
              '370010229', '370010227', '370020475', '370020481', '370021429', '370023754', '370030861', '370021993',
              '370021831', '370036033', '370036032']


    csv_file = csv.reader(open('../Stops.csv', "rb"), delimiter=",")
    stops_data = []

    for row in csv_file:
        for idx, stop in enumerate(stops1):
            if stop in row:
                stops_data.append(row)
    return stops_data


def lambda_handler():
    url = urlopen("http://naptan.app.dft.gov.uk/DataRequest/Naptan.ashx?format=csv")
    dynamo_db = boto3.resource('dynamodb', region_name='eu-west-1')

    # Connect to the DynamoDB table
    table = dynamo_db.Table('Naptan')
    stops = get_stops()
    for stop in stops:
        expression_atribute_values = {column: stop[idx] if stop[idx] != '' else 'null' for idx, column in
                                      enumerate(COLUMNS)}
        expression_atribute_values['StopId'] = stop[0]
        table.put_item(
            Item=expression_atribute_values
        )


lambda_handler()
