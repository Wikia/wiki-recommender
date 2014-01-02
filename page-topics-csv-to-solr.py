import requests
import sys
import json

params = {'fl': 'id,title_en,url,wid,wam,backlinks', 'wt': 'json'}

docs = []
with open(sys.argv[1], 'r') as fl:
    for i in range(0, len(fl), 100):
        print "%d / %d" % (i, len(fl))
        post_docs = []
        try:
            params['q'] = ' OR '.join(['id:'+line.split(',')[0] for line in fl[i:i+100]])
            docs = dict(
                [(doc['id'], doc)
                 for doc in requests.get('http://search-s10:8983/solr/main/select', params=params)
                                    .json()['response']['docs']
                 ]
            )
        except (ValueError, KeyError) as e:
            print e
        for line in fl[i:i+100]:

            ploded = line[:-1].split(',')
            doc_id = ploded[0]
            doc = docs.get(doc_id, {'id': doc_id})
            for grouping in ploded[1:]:
                topic, value = grouping.split('-')
                doc['topic_%s_tf' % topic] = {'set': value}
        post_docs += [doc]
        print requests.post('http://dev-search:8983/solr/main/update?commit=true',
                            data=json.dumps(post_docs),
                            headers={'Content-type': 'application/json'}).content
