import urllib.request, json
import csv
"""

"""
with urllib.request.urlopen("https://doi.crossref.org/getPrefixPublisher/**") as url:
    data = json.load(url)
    # now we will open a file for writing
    data_file = open('crossref_piblishers_prefix.csv', 'w')
    # create the csv writer object
    csv_writer = csv.writer(data_file, delimiter=";", quotechar='"')

    csv_writer.writerow(['prefixes', 'name', 'memberId'])

    for pub in data:
        # Writing data of CSV file
        pref = ', '.join(pub['prefixes'])
        csv_writer.writerow([pref, pub['name'], pub['memberId']])

    data_file.close()
