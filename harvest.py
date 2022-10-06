import datetime
import csv

from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, MetadataReader


def main():
    URL = 'http://oai.narcis.nl/oai'

    # from_date = '2022-07-13T22:00:00Z'
    # from_date = datetime.datetime.strptime(from_date, "%Y-%m-%dT%H:%M:%SZ")
    until_date = datetime.datetime.now()
    from_date = until_date - datetime.timedelta(hours=16)
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
            'date': ('textList', 'oai_dc:dc/dc:date/text()'), # publicatie datum voor de instelling
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

    with open('harvest.csv', 'w', newline='') as f:
        fieldnames = ['doi', 'institute', 'datestamp', 'type', 'identifiers', 'date','source', 'rights', 'partof', 'creators', 'title']
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='$', quotechar='"')
        writer.writeheader()

        records = client.listRecords(metadataPrefix='oai_dc', from_=from_date, until=until_date, set='publication')
        # to make it more robust, we would have to check if the list isn't empty

        for num, record in enumerate(records):
            # print('%0.6d %s' % (num, record[0].identifier()))

            # break for testing
            # if num == 100:
            #     break

            # check if item has been deleted
            # deleted items have 'header status=deleted' and no metadata
            # if not deleted, get metadata: title, creator, date, type, source, identifier
            # field 'ispartof' is not included in the module, so we hacked it in

            if record[0].isDeleted():
                print("item deleted")
            elif record[1] is not None:
                header = record[0]
                datestamp = header.datestamp()
                institute = header.identifier()

                fields = record[1].getMap()
                pubtype = fields['type']
                # continue if pubtype is not present or if it's an article
                if not pubtype or pubtype[0] == 'info:eu-repo/semantics/article':
                    identifiers = fields['identifier']
                    doi = ''
                    for id in identifiers:
                        if id.startswith('10.'):
                            doi = id

                    itemdate = fields['date']
                    source = fields['source']
                    rights = fields['rights']
                    partof = fields['ispartof']
                    creator = fields['creator']
                    title = fields['title']
                    print(doi)
                    # print(" by ")
                    # print(creator)
                    # print(" is part of")
                    # print(partof)
                    writer.writerow({'doi': doi, 'institute': institute, 'datestamp': datestamp,'type': pubtype,
                                     'identifiers': identifiers, 'date': itemdate, 'source': source,'rights': rights,
                                     'partof': partof, 'creators': creator, 'title': title})


if __name__ == '__main__':
    main()
