import requests
import sys
import json

docs = []
with open(sys.argv[1], 'r') as fl:
    for line in fl:
        ploded = line[:-1].split(',')
        wid = ploded[0]
        doc = dict(id=wid, has_topics_b=True)
        #reinitialize
        for i in range(0, 999):
            doc['topic_%d_tf' % i] = 0
        for grouping in ploded[1:]:
            topic, value = grouping.split('-')
            doc['topic_%s_tf' % topic] = {'set': value}
        docs += [doc]

print requests.post('http://dev-search:8983/solr/xwiki/update?commit=true',
                    data=json.dumps(docs),
                    headers={'Content-type': 'application/json'}).content
