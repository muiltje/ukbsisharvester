import sys
from dateutil.rrule import rrule, MONTHLY
from datetime import date, datetime, timedelta
from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, MetadataReader
from oaipmh import error as oaipmhError
from csvwriter import CsvWriter
from os.path import exists
import csv
import logging
import config

API_URL = 'http://oai.narcis.nl/oai'

# Fields of the CSV file
CSV_FIELDS = {'doi': None, 'identifier': None, 'datestamp': None, 'deleted': False, 'type': None,
              'identifiers': None, 'date': None, 'source': None, 'rights': None,
              'partof': None, 'creator': None, 'title': None}

FILE_NAME = 'data.csv'

"""
Logging
"""
logfile = config.LOGFILE_DIR + date.today().strftime('%Y-%m-%d_') + config.LOGFILE_SUFFIX
log_format = "%(asctime)s - %(levelname)-8s - %(name)s | %(message)s"
logging.basicConfig(filename=logfile, filemode='a', format=log_format, level=config.LOG_LEVEL)
logger = logging.getLogger('Harvester')

consoleHandler = logging.StreamHandler(sys.stdout)
logFormatter = logging.Formatter(log_format)
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)


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
    client = Client(API_URL, registry)
    return client


def harvest_data(start_itr, end_itr, count_only=False, initial=False):
    if count_only:
        return count_data(start_itr, end_itr)

    logger.info('# Harvesting data between from %s until %s',
                start_itr.strftime('%Y%m%d'), end_itr.strftime('%Y%m%d'))

    client = get_client()

    writer = CsvWriter(start_itr, end_itr, CSV_FIELDS, config.MAX_CSV_ROWS, config.OUTPUT_DIR)

    try:
        records = client.listRecords(metadataPrefix='oai_dc', from_=start_itr, until=end_itr, set='publication')
        for num, record in enumerate(records):
            if initial and record[0].isDeleted() is True:  # and record[1] is None:
                continue

            rd = get_record_data(record)
            # continue if pubtype is not present or if it's an article
            # if not rd['type'] or rd['type'] == 'info:eu-repo/semantics/article':
            writer.write_record_to_csv(rd)

    except oaipmhError.NoRecordsMatchError as e:
        logger.info("No records found for the period %s - %s", start_itr.strftime('%Y%m%d'), end_itr.strftime('%Y%m%d'))
    except TypeError:
        logger.error(type(record))

    writer.close_file()


counter = {
    # '2017': {'doi': 0, 'nodoi': 0, 'total': 0},
    '2018': {'doi': 0, 'nodoi': 0, 'total': 0},
    '2019': {'doi': 0, 'nodoi': 0, 'total': 0},
    '2020': {'doi': 0, 'nodoi': 0, 'total': 0},
    '2021': {'doi': 0, 'nodoi': 0, 'total': 0},
    '2022': {'doi': 0, 'nodoi': 0, 'total': 0},
}


def count_data(start_itr, end_itr):
    logger.info('# Counting dates: %s to %s',
                start_itr.strftime('%Y%m%d'), end_itr.strftime('%Y%m%d'))

    # counter = dict(count_totals)
    target_years = counter.keys()

    client = get_client()

    try:
        records = client.listRecords(metadataPrefix='oai_dc', from_=start_itr, until=end_itr, set='publication')
        for num, record in enumerate(records):
            # if record[0].isDeleted() is False and record[1] is not None:
            rd = get_record_data(record)

            # continue if pubtype it's an article
            if rd['deleted'] is False and rd['type'] == 'info:eu-repo/semantics/article':
                # SAVE only if pubdate is 2021
                year = rd['date'].split("-")[0]
                if year in target_years:
                    counter[year]['total'] += 1
                    if rd['doi'] == '':
                        counter[year]['nodoi'] += 1
                    else:
                        counter[year]['doi'] += 1

    except oaipmhError.NoRecordsMatchError as e:
        logger.debug("     !!!!! Exception: %s", e)
    except TypeError:
        logger.debug(type(record))

    logger.info(counter)
    write_totals(counter, start_itr, end_itr)


def write_totals(counter, from_date, to_date):
    write_header = not exists(FILE_NAME)

    with open(FILE_NAME, 'a', newline='') as file:
        fieldnames = ['from', 'to', 'year', 'doi', 'nodoi', 'total']
        writer = csv.DictWriter(file, fieldnames=fieldnames, delimiter=';', quotechar='"')
        if write_header:
            writer.writeheader()

        for k in counter:
            f = counter[k]
            writer.writerow({'from': from_date, 'to': to_date, 'year': k, 'doi': f['doi'], 'nodoi': f['nodoi'],
                             'total': f['total']})

    file.close()


def get_record_data(record):
    """
    Extracts data from api record
    """
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
                logger.debug('DOI too long: ', doi)
                doi = doi[:254]
        csv_rec['doi'] = doi

        csv_rec['source'] = list_2_string(fields['source'])
        csv_rec['rights'] = list_2_string(fields['rights'])
        csv_rec['partof'] = list_2_string(fields['ispartof'])
        csv_rec['creator'] = list_2_string(fields['creator'])
        csv_rec['title'] = list_2_string(fields['title'])

    return csv_rec


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


def monthly_harvest(start, end, count_only=False, initial=True):
    """
    This function will harvest everything between two dates until first day of the mont of the end date
    dats should be in iso format: 2020-01-01
    """
    from_date = datetime.fromisoformat(start)
    until_date = datetime.fromisoformat(end)

    logger.info('## START MONTHLY PROCESSING from %s until %s', from_date, until_date)

    # Set start cycle date for the initial iteration
    start_itr = from_date

    for end_itr in rrule(freq=MONTHLY, dtstart=from_date, until=until_date)[1:]:
        harvest_data(start_itr, end_itr, count_only, initial)

        # change start iteration date to the current end iteration date
        start_itr = end_itr

    logger.info('## END MONTHLY PROCESSING ##')


def initial_harvest(date_from, count_only=False):
    """
    Harvest of all historical data
    consists in a monthly harvest until current month and then a normal harvest until yesterday
    """
    logger.info('#### START INITIAL HARVEST from %s, count is %s ####', date_from, count_only)

    today = date.today()
    yesterday = today - timedelta(days=1)
    first_day_current_month = yesterday.strftime('%Y-%m-01')

    # monthly harvest until beginning of current month
    monthly_harvest(date_from, first_day_current_month, count_only, initial=True)

    # harvest of the current month until yesterday
    month_start = datetime.fromisoformat(first_day_current_month)
    today_datetime = datetime.fromisoformat(today.strftime('%Y-%m-%d'))
    harvest_data(month_start, today_datetime, count_only, initial=True)

    # harvest today
    # until_date = datetime.now()
    # harvest_data(today_datetime, until_date, count_only)


def harvest_one_day(iso_date):
    """
    Harvest only one day
    """
    logger.info('#### Harvest one day from: %s', iso_date)
    from_date = datetime.fromisoformat(iso_date)
    until_date = from_date + timedelta(days=1)
    harvest_data(from_date, until_date)


def harvest_yesterday():
    """
    Harvest yesterdays data
    """
    until_date = datetime.now()
    from_date = until_date - timedelta(days=1)
    logger.info('#### Harvest yesterday data from %s to %s', from_date.strftime('%Y-%m-%d'),
                until_date.strftime('%Y-%m-%d'))
    harvest_data(from_date, until_date)


def harvest_from_date(iso_date, count_only=False):
    """
    Harvesting from a specific date
    """
    logger.info('#### Logging from date %s', iso_date)
    from_date = datetime.fromisoformat(iso_date)
    until_now = datetime.now()

    harvest_data(from_date, until_now, count_only)


if __name__ == '__main__':
    # count_only = True
    # if count_only:
    #     FILE_NAME = config.OUTPUT_DIR + date.today().strftime('/%Y-%m-%d_count.csv')
    #
    # initial_harvest('2020-05-01', count_only)

    # get_narcis_total()
    #

    harvest_one_day('2022-11-22')
