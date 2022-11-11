import csv
import mysql.connector

from dateutil.rrule import rrule, MONTHLY
from datetime import date, datetime, timedelta

from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, MetadataReader
from oaipmh import error as oaipmhError

from csvwriter import CsvWriter

URL = 'http://oai.narcis.nl/oai'

CSV_MAX_RECORD_FILE = 25000  # Max record per CSV file

CSV_FIELDS = {'doi': None, 'identifier': None, 'datestamp': None, 'deleted': False, 'type': None,
              'identifiers': None, 'date': None, 'source': None, 'rights': None,
              'partof': None, 'creator': None, 'title': None}

CSV_DIR = 'harvest' # DIR should exists

# Get oaipmh client
def get_client():
    registry = MetadataRegistry()

    # instantiate the oai_Reader with the right metadata
    oai_dc_reader = MetadataReader(
        fields={
            'title': ('textList', 'oai_dc:dc/dc:title/text()'),
            'creator': ('textList', 'oai_dc:dc/dc:creator/text()'),
            'subject': ('textList', 'oai_dc:dc/dc:subject/text()'),
            'description': ('textList', 'oai_dc:dc/dc:description/text()'),
            'publisher': ('textList', 'oai_dc:dc/dc:publisher/text()'),
            'contributor': ('textList', 'oai_dc:dc/dc:contributor/text()'),
            'date': ('textList', 'oai_dc:dc/dc:date/text()'),  # publicatie datum voor de instelling
            'type': ('textList', 'oai_dc:dc/dc:type/text()'),
            'format': ('textList', 'oai_dc:dc/dc:format/text()'),
            'identifier': ('textList', 'oai_dc:dc/dc:identifier/text()'),
            'source': ('textList', 'oai_dc:dc/dc:source/text()'),
            'language': ('textList', 'oai_dc:dc/dc:language/text()'),
            'relation': ('textList', 'oai_dc:dc/dc:relation/text()'),
            'coverage': ('textList', 'oai_dc:dc/dc:coverage/text()'),
            'rights': ('textList', 'oai_dc:dc/dc:rights/text()'),
            'ispartof': ('textList', 'oai_dc:dc/dc:isPartOf/text()')
        },
        namespaces={
            'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
            'dc': 'http://purl.org/dc/elements/1.1/'}
    )

    registry.registerReader('oai_dc', oai_dc_reader)
    client = Client(URL, registry)
    return client


def harvest_data(start_itr, end_itr):
    print('# Processing dates: %s to %s' % (
        start_itr.strftime('%Y%m%d'), end_itr.strftime('%Y%m%d')))

    client = get_client()

    writer = CsvWriter(start_itr, end_itr, CSV_FIELDS, CSV_MAX_RECORD_FILE, CSV_DIR)

    try:
        records = client.listRecords(metadataPrefix='oai_dc', from_=start_itr, until=end_itr, set='publication')
        for num, record in enumerate(records):
            # if record[0].isDeleted() is False and record[1] is not None:
            rd = get_record_data(record)

            # continue if pubtype is not present or if it's an article
            # if not rd['type'] or rd['type'] == 'info:eu-repo/semantics/article':

            # SAVE only if pubdate is 2021
            # if rd['date'].split("-")[0] == '2021':
            writer.write_record_to_csv(rd)



    except oaipmhError.NoRecordsMatchError as e:
        print("     !!!!! Exception: ", e)
    except TypeError:
        print(type(record))

    writer.close_file()


def get_record_data(record):
    # make a copy without reference
    csv_rec = dict(CSV_FIELDS)

    header = record[0]
    csv_rec['datestamp'] = header.datestamp()
    csv_rec['identifier'] = header.identifier()

    deleted = header.isDeleted()

    if deleted:
        csv_rec['deleted'] = True

    else:

        fields = record[1].getMap()

        csv_rec['type'] = fields['type'][0] if type(fields['type']) is list and len(fields['type']) > 0 else ''

        csv_rec['identifiers'] = list_2_string(fields['identifier'])
        csv_rec['date'] = fix_item_date(fields['date'])

        doi = ''
        for id in fields['identifier']:
            if id.startswith('10.'):
                doi = id
            if len(doi) > 254:
                print('DOI too long: ', doi)
                doi = doi[:254]
        csv_rec['doi'] = doi

        csv_rec['source'] = list_2_string(fields['source'])
        csv_rec['rights'] = list_2_string(fields['rights'])
        csv_rec['partof'] = list_2_string(fields['ispartof'])
        csv_rec['creator'] = list_2_string(fields['creator'])
        csv_rec['title'] = list_2_string(fields['title'])

    return csv_rec


def write_record_to_mysql(rd):
    mydb = mysql.connector.connect(
        host="localhost",
        port="3305",
        user="root",
        password="stocazzo",
        database="ukbasis"
    )

    mycursor = mydb.cursor()

    sql = "INSERT INTO narcis (doi, type, datestamp, identifiers, date, title) VALUES (%s, %s, %s, %s, %s, %s)"
    val = (rd['doi'], rd['type'], rd['datestamp'], rd['identifiers'], rd['date'], rd['title'])
    mycursor.execute(sql, val)

    mydb.commit()


def fix_item_date(date):
    if type(date) is not list or len(date) == 0:
        return ''

    date_string = date[0]
    date_parts = date_string.split("-")
    year = date_parts[0]
    month = date_parts[1].zfill(2) if 1 < len(date_parts) else '01'
    day = date_parts[2].zfill(2) if 2 < len(date_parts) else '01'
    return year + "-" + month + "-" + day


def list_2_string(lst):
    if len(lst) == 1:
        return lst[0]
    return ','.join(f'"{w}"' for w in lst)


# This function will harvest everything between two dates
# dats should be in iso format: 2020-01-01
def monthly_harvest(start, end):
    from_date = datetime.fromisoformat(start)
    until_date = datetime.fromisoformat(end)

    # Set start cycle date for the initial iteration
    start_itr = from_date

    for end_itr in rrule(freq=MONTHLY, dtstart=from_date, until=until_date)[1:]:
        harvest_data(start_itr, end_itr)

        # change start iteration date to the current end iteration date
        start_itr = end_itr

    print('## END PROCESSING', from_date, until_date)


def initial_harvest(date_from):
    yesterday = date.today() - timedelta(days=1)
    first_day_month = yesterday.strftime('%Y-%m-01')

    # monthly harvest until beginning of current month
    monthly_harvest(date_from, first_day_month)

    # harvest of the current month
    month_start = datetime.fromisoformat(first_day_month)
    yesterday_datetime = datetime.fromisoformat(yesterday.strftime('%Y-%m-%d'))
    harvest_data(month_start, yesterday_datetime)


def daily_harvest():
    monthly_start = '2017-01-01'
    yesterday = date.today() - timedelta(days=1)
    monthly_end = yesterday.strftime('%Y-%m-01')
    monthly_harvest(monthly_start, monthly_end)


def recurring_harvest(hours):
    # from_date = '2022-07-13T22:00:00Z'
    # from_date = datetime.strptime(from_date, "%Y-%m-%dT%H:%M:%SZ")
    until_date = datetime.now()
    # from_date = until_date - timedelta(hours=hours)
    from_date = datetime.fromisoformat('2022-09-01')

    harvest_data(from_date, until_date)


if __name__ == '__main__':
    # daily_harvest()
    # recurring_harvest(12)
    # monthly_harvest('2020-04-01', '2020-06-01')
    initial_harvest('2022-06-01')
