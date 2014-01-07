import requests
import sys
import json
import random
from multiprocessing import Pool

def runit(tup):
    i, outof = tup
    params = {'fl': 'id,title_en,url,wid,wam,backlinks,views,wikititle_en', 'wt': 'json', 'rows': 500}
    docs = {}
    print len(lines)
    with open(sys.argv[1], 'r') as fl:
        print "%d / %d" % (i, outof)
        post_docs = []
        try:
            params['q'] = ' OR '.join(['id:'+line.split(',')[0] for line in lines[i:i+100] if line.split(',')[0] is not ''])
            response = requests.get('http://search-s10:8983/solr/main/select', params=params)
            docs = dict([(doc['id'], doc) for doc in response.json()['response']['docs']])
        except:
            print sys.exc_info()
        for line in lines[i:i+100]:
            ploded = line[:-1].split(',')
            doc_id = ploded[0]
            if doc_id in docs:
                doc = docs.get(doc_id, {'id': doc_id})
            else:
                print "missing", doc_id
                continue
            for i in range(0, 999):
                doc['topic_%s_tf' % i] = 0
            for grouping in ploded[1:]:
                topic, value = grouping.split('-')
                doc['topic_%s_tf' % topic] = {'set': value}
            post_docs += [doc]
        try:
            print requests.post('http://dev-search:8983/solr/main/update',
                            data=json.dumps(post_docs),
                            headers={'Content-type': 'application/json'}).content
        except:
            print 'problem'



with open(sys.argv[1], 'r') as fl:
    lines = [line for line in fl]
    random.shuffle(lines)
    outof = len(lines)
    Pool(processes=16).map(runit, [(i, outof) for i in range(0, outof, 100)])
