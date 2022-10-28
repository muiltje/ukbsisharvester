import csv

from dateutil.rrule import rrule, MONTHLY
from datetime import datetime

from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, oai_dc_reader

URL = 'http://oai.narcis.nl/oai'


def count_articles():
    registry = MetadataRegistry()

    start = datetime.fromisoformat('2022-01-01')
    end = datetime.fromisoformat('2022-10-01')

    print(">>>> START >>>> Counting records ", start.strftime('%d-%m-%Y'), end.strftime('%d-%m-%Y'))

    registry.registerReader('oai_dc', oai_dc_reader)
    client = Client(URL, registry)

    with open('_count.csv', 'w', newline='') as f:
        fieldnames = ['from', 'to', 'deleted', 'count']
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';', quotechar='"')
        # writer.writeheader()

        prev = start
        for dt in rrule(freq=MONTHLY, dtstart=start, until=end)[1:]:
            deleted = 0
            count = 0
            print("-> Getting records:", prev.strftime('%d-%m-%Y'), dt.strftime('%d-%m-%Y'), deleted, count)

            try:
                records = client.listRecords(metadataPrefix='oai_dc', from_=prev, until=dt, set='publication')

                # total = sum(1 for _ in records)
                # return (total)

                for num, record in enumerate(records):
                    if record[0].isDeleted():
                        deleted = deleted + 1
                    elif not record[1] is None:
                        count = count + 1
                    else:
                        print("something strange")
            except:
                print(">> no records found")

            print("    - Found records", deleted, count)

            writer.writerow(
                {'from': prev.strftime('%d-%m-%Y'), 'to': dt.strftime('%d-%m-%Y'), 'deleted': deleted, 'count': count})

            prev = dt


def simple_count():
    start = datetime.fromisoformat('2018-01-01')
    end = datetime.fromisoformat('2019-10-01')

    registry = MetadataRegistry()
    registry.registerReader('oai_dc', oai_dc_reader)
    client = Client(URL, registry)

    records = client.listIdentifiers(metadataPrefix='oai_dc', from_=start, until=end, set='publication')

    deleted = 0
    count = 0

    for num, record in enumerate(records):
        if record.isDeleted():
            deleted += 1
        else:
            count += 1

    print(">>>> START >>>> Counting records ", start.strftime('%d-%m-%Y'), end.strftime('%d-%m-%Y'))
    print(">> Count: ", count, " - Deleted: ", deleted)


if __name__ == '__main__':
    simple_count()
