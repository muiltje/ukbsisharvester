from datetime import datetime
from bs4 import BeautifulSoup
import requests
import re

"""
Extrapolate  article count from Narcis Website
"""

def get_narcis_total():
    url = 'https://www.narcis.nl/search/coll/publication/Language/EN/genre/article'

    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '3600',
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
    }

    req = requests.get(url, headers)
    soup = BeautifulSoup(req.content, 'html.parser')
    # bra = soup.select("div.list-block:nth-of-type(1) ul.link-list a")
    bra = soup.find(class_='list-block').select('a')
    for link in bra:
        # print (link.get_text())
        x = re.search("^(?P<year>[\d]+) \((?P<count>[\d]+)\)", link.get_text())
        if x is not None:
            # print(x['year'],  x['count'])
            print("Narcis total;%s;%s;;;%s" % (datetime.now(), x['year'],  x['count']))

if __name__ == '__main__':
    get_narcis_total()