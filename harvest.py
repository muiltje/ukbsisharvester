import datetime
import csv

from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, oai_dc_reader


def main():
    URL = 'http://oai.narcis.nl/oai'

    # from_date = '2022-07-13T22:00:00Z'
    # from_date = datetime.datetime.strptime(from_date, "%Y-%m-%dT%H:%M:%SZ")
    until_date = datetime.datetime.now()
    from_date = until_date - datetime.timedelta(hours=6)
    registry = MetadataRegistry()
    registry.registerReader('oai_dc', oai_dc_reader)
    client = Client(URL, registry)

    with open('harvest.csv', 'w', newline='') as f:
        fieldnames = ['identifiers', 'date', 'source', 'rights', 'partof', 'creators', 'title']
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=' ', quotechar='"')
        writer.writeheader()

        records = client.listRecords(metadataPrefix='oai_dc', from_=from_date, until=until_date, set='publication')
        # to make it more robust, we would have to check if the list isn't empty
        for num, record in enumerate(records):
            print('%0.6d %s' % (num, record[0].identifier()))
            # check if item has been deleted
            # deleted items have 'header status=deleted' and no metadata
            # if not deleted, get metadata: title, creator, date, type, source, identifier
            # field 'ispartof' is not included in the module, so we hacked it in

            if record[0].isDeleted():
                print("item deleted")
            elif record[1] is not None:
                fields = record[1].getMap()
                pubtype = fields['type']
                # continue if pubtype is not present or if it's an article
                if not pubtype or pubtype[0] == 'info:eu-repo/semantics/article':
                    identifiers = fields['identifier']
                    itemdate = fields['date']
                    source = fields['source']
                    rights = fields['rights']
                    partof = fields['ispartof']
                    creator = fields['creator']
                    title = fields['title']
                    print(identifiers)
                    print(" by ")
                    print(creator)
                    print(" is part of")
                    print(partof)
                    writer.writerow({'identifiers': identifiers, 'date': itemdate, 'source': source,
                                     'rights': rights, 'partof': partof, 'creators': creator,
                                     'title': title})


if __name__ == '__main__':
    main()
