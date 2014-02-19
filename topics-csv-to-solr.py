import sys
from lib.wikis import csv_to_solr


with open(sys.argv[1], 'r') as fl:
    print csv_to_solr(fl).content
