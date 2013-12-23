import requests, sys, json

etl = len(sys.argv) > 2

params = {'fl': 'id,title_en,url,wid,wam,backlinks', 'wt': 'json', 'rows': '1' }

docs = []
with open(sys.argv[1], 'r') as fl:
    for line in fl:
        ploded = line[:-1].split(',')
        doc_id = ploded[0]
        try:
            if not etl:
                raise ValueError()
            params['q'] = 'id:'+doc_id
            doc = requests.get('http://search-s10:8983/solr/main/select', params=params).json()['response']['docs']['0']
        except:
            doc = dict(id=doc_id)
        for grouping in ploded[1:]:
            topic, value = grouping.split('-')
            doc['topic_%s_tf' % topic] = {'set': value}
        docs += [doc]

print requests.post('http://dev-search:8983/solr/main/update?commit=true', data=json.dumps(docs), headers={'Content-type': 'application/json'}).content
