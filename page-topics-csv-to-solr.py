import requests
import sys
import json

params = {'fl': 'id,title_en,url,wid,wam,backlinks', 'wt': 'json'}

docs = {}
with open(sys.argv[1], 'r') as fl:
    lines = [line for line in fl]
    for i in range(0, len(lines), 100):
        print "%d / %d" % (i, len(lines))
        post_docs = []
        try:
            params['q'] = ' OR '.join(['id:'+line.split(',')[0] for line in lines[i:i+100] if line.split(',')[0] is not ''])
            response = requests.get('http://search-s10:8983/solr/main/select', params=params)
            print response.content
            docs = dict([(doc['id'], doc) for doc in response.json()['response']['docs']])
        except (ValueError, KeyError) as e:
            print sys.exc_info()
        for line in lines[i:i+100]:
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
