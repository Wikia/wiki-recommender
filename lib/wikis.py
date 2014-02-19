import requests
import json
from .filters import get_topics_sorted_keys


def get_wikis_with_topics(solr_url='http://dev-search:8983/solr/xwiki/select', fields='id'):
    docs = []
    params = {'q': 'has_topics_b:true', 'fl': fields, 'sort': 'wam_i desc', 'start': 0, 'rows': 500}
    while True:
        response = requests.get(solr_url, params=params).json().get('response', {})
        docs += response['docs']
        if params['start'] >= response['numFound']:
            return docs
        params['start'] += params['rows']


def as_euclidean(doc_id, solr_url='http://dev-search:8983/solr/xwiki/select'):
    doc_response = requests.get('%s/select/' % solr_url, params=dict(rows=1, q='id:%s' % doc_id, wt='json')).json()
    doc = doc_response.get('response', {}).get('docs', [None])[0]
    if doc is None:
        return None, []  # same diff

    keys = get_topics_sorted_keys(doc)

    if not keys:
        return {}, []

    sort = 'dist(2, vector(%s), vector(%s))' % (", ".join(keys), ", ".join(['%.8f' % doc[key] for key in keys]))

    params = {'wt': 'json',
              'q': '-id:%s AND (%s)' % (doc['id'], " OR ".join(['(%s:*)' % key for key in keys])),
              'sort': sort + ' asc',
              'rows': 20,
              'fl': 'id,sitename_txt,topic_*,wam_i,url,'+sort}

    docs = requests.get('%s/select/' % solr_url, params=params).json().get('response', {}).get('docs', [])
    map(lambda x: x.__setitem__('score', x[sort]), docs)

    return doc, docs


def reinitialize_topics():
    reinitialize_docs = []
    for doc in get_wikis_with_topics(fields='id,topic_*'):
        doc_id = doc['id']
        doc = dict([(key, {'set': None}) for key in doc.items()])
        doc['id'] = doc_id

    return requests.post('http://dev-search:8983/solr/xwiki/update?commit=true',
                         data=json.dumps(reinitialize_docs),
                         headers={'Content-type': 'application/json'})


def csv_to_solr(fl, num_topics=999):
    update_docs = []
    for line in fl:
        ploded = line[:-1].split(',')
        wid = ploded[0]
        doc = dict(id=wid, has_topics_b=True)
        #set as zero first
        for i in range(0, num_topics):
            doc['topic_%d_tf' % i] = 0
        for grouping in ploded[1:]:
            topic, value = grouping.split('-')
            doc['topic_%s_tf' % topic] = {'set': value}
        update_docs += [doc]

    reinitialize = reinitialize_topics()

    update = requests.post('http://dev-search:8983/solr/xwiki/update?commit=true',
                           data=json.dumps(update_docs),
                           headers={'Content-type': 'application/json'})

    return {'reinitialize': reinitialize.content, 'update': update.content}
