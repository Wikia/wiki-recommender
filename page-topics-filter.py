import requests
import sys
from multiprocessing import Pool

all_ids = []

print 'getting wids...'
with open(sys.argv[2], 'r') as widsfile:
    wids = [str(int(line)) for line in widsfile]
    
print 'extracting doc ids per wid'

def do(wid):
    ids = []
    start = 0
    numFound = None
    while numFound is None or numFound > start:
        params = {'wt': 'json', 'q': 'wid:'+str(wid), 'fl': 'id', 'rows': 5000, 'start': start, 'sort': 'doc_id desc', 'start': start}
        response = requests.get('http://localhost:8983/solr/main/select', params=params).json()
        ids += [doc['id'] for doc in response['response']['docs']]
        numFound = response['response']['numFound']
        start += 5000
    return ids

for response in Pool(processes=6).map(do, wids):
    all_ids += response

print 'writing new file'

id_hash = dict([(id, True) for id in all_ids])

with open(sys.argv[1], 'r') as readfile:
    with open(sys.argv[1]+'.filtered', 'w') as writefile:
        for line in readfile:
            if line.split(',')[0] not in id_hash:
                writefile.write(line)
