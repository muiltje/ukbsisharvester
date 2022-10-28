import csv

from dateutil.rrule import rrule, MONTHLY
from datetime import datetime, timedelta

from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, MetadataReader

URL = 'http://oai.narcis.nl/oai'

MAX_RECORD_FILE = 10000 # Max record per CSV file
CSV = {"file": False, "writer": None, "file_count": 1}


def get_csv(file_count, from_date, until_date, new=True):
    print(' > Creating file', file_count, from_date, until_date)
    # file name
    file_name = create_file_name(file_count, from_date, until_date)
    # create a new file
    f = open(file_name, 'w', newline='')
    fieldnames = ['doi', 'institute', 'datestamp', 'type', 'identifiers', 'date', 'source', 'rights', 'partof',
                  'creators', 'title']
    writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='$', quotechar='"')
    writer.writeheader()
    return {"file": f, "writer": writer}


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


# This function will harvest everything between two dates
def initial_harvest():
    from_date = datetime.fromisoformat('2018-01-01')
    until_date = datetime.fromisoformat('2020-01-01')

    max_write_count = 10000

    client = get_client()

    # Set start cycle date for the initial iteration
    start_itr = from_date

    for end_itr in rrule(freq=MONTHLY, dtstart=from_date, until=until_date)[1:]:
        print('Processing dates: ', start_itr, end_itr)
        if CSV["file"] is not False:
            CSV["file"].close()
            CSV["file"] = False
            CSV["writer"] = False
            CSV["file_count"] = 1

        try:
            records = client.listRecords(metadataPrefix='oai_dc', from_=start_itr, until=end_itr, set='publication')
            write_count = 1  # Counts the number of records written in one file - for file splittion
            file_count = 1  # Counts the number of files written in a two date period - for file naming

            for num, record in enumerate(records):

                if record[0].isDeleted() is True:

                    # if record[1] is not None:
                    if write_record_to_csv(record, end_itr, start_itr, write_count) is True:
                        write_count += 1

                # break for testing
                if num == 100:
                    CSV["file"].close()
                    exit()
                    break

        except Exception as e:
            print("!!!!! Exception:", e)

        # change start iteration date to the current end iteration date close current file
        start_itr = end_itr

        print(' >>> written %d records between %s and %s' % (write_count - 1, start_itr, end_itr))

    print('## END PROCESSING', from_date, until_date)


def get_file_writer(end_itr, start_itr, write_count):

    file = CSV["file"]
    file_count = CSV["file_count"]

    # Create the file
    if write_count == MAX_RECORD_FILE:
        # Get another file
        file.close()
        file_count += 1
        write_count = 1

    if (file is False or file.closed) and write_count == 1:
        csv_file = get_csv(file_count, start_itr, end_itr, True)
        CSV["writer"] = csv_file["writer"]
        CSV["file"] = csv_file["file"]

    return  CSV["writer"]


def create_file_name(file_count, from_date, until_date):
    file_name = 'harvest/deleted_%s-%s_%d.csv' % (
        from_date.strftime('%Y%m%d'), until_date.strftime('%Y%m%d'), file_count
    )
    return file_name



def write_record_to_csv(record,end_itr, start_itr, write_count):
    """
    Write a record to CSV, if the record is written return true
    """
    header = record[0]
    datestamp = header.datestamp()
    institute = header.identifier()
    # fields = record[1].getMap()
    # pub_type = fields['type']
    # # continue if pubtype is not present or if it's an article
    # if not pub_type or pub_type[0] == 'info:eu-repo/semantics/article':
    #     identifiers = fields['identifier']
    #     doi = ''
    #     for id in identifiers:
    #         if id.startswith('10.'):
    #             doi = id
    #
    #     itemdate = fields['date']
    #     source = fields['source']
    #     rights = fields['rights']
    #     partof = fields['ispartof']
    #     creator = fields['creator']
    #     title = fields['title']

    # Get file if exists or create e new one
    writer = get_file_writer(end_itr, start_itr, write_count)

    writer.writerow({'doi': "", 'institute': institute, 'datestamp': datestamp, 'type': "",
                     'identifiers': "", 'date': "", 'source': "", 'rights': "",
                     'partof': "", 'creators': "", 'title': ""})
    return True

    # return False


if __name__ == '__main__':
    # recurring_harvest()
    initial_harvest()
