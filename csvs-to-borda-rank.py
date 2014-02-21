import sys
import requests
from collections import defaultdict


def borda(list_of_lists):
    dct = defaultdict(int)
    [dct.__setitem__(val, rank + dct[val]) for li in list_of_lists
     for enumed in list(enumerate(li)) for rank, val in enumed]
    return map(lambda y: y[0], sorted(dct.items(), key=lambda x: x[1]))


def wiki_data_for_ids(ids):
    ids_to_data = {}
    for i in range(0, len(ids), 20):
        ids_to_data.update(requests.get('http://www.wikia.com/api/v1/Wikis/Details',
                                        params={'ids': ','.join(ids[i:i+20])}).json().get('items', {}))
    return ids_to_data


def main():
    doc_to_recs = defaultdict(list)
    for arg in sys.argv[1:]:
        with open(arg, 'r') as fl:
            for line in fl:
                cols = line.strip().split(',')
                doc_to_recs[cols[0]] = cols[1:]
    borda_recs = [[doc_id] + borda(recs) for doc_id, recs in doc_to_recs.items()]
    with open('borda_ids.csv', 'w') as fl:
        fl.writelines([','.join(row) for row in borda_recs])
    wiki_data = wiki_data_for_ids(list(set([col for row in borda_recs for col in row])))
    with open('borda_names.csv', 'w') as fl:
        fl.writelines([','.join([wiki_data.get(x).get('title', '?') for x in row]) for row in borda_recs])
    with open('borda_urls.csv', 'w') as fl:
        fl.writelines([','.join([wiki_data.get(x).get('url', '?') for x in row]) for row in borda_recs])


if __name__ == '__main__':
    main()