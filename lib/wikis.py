import requests
import json
from .querying import as_euclidean  # backwards compatibility


def get_wikis_with_topics(solr_url='http://dev-search:8983/solr/xwiki/select', fields='id'):
    docs = []
    params = {'q': 'has_topics_b:true', 'fl': fields, 'sort': 'wam_i desc', 'start': 0, 'rows': 500, 'wt': 'json'}
    while True:
        response = requests.get(solr_url, params=params).json().get('response', {})
        docs += response['docs']
        if params['start'] >= response['numFound']:
            return docs
        params['start'] += params['rows']


def reinitialize_topics():
    reinitialize_docs = []
    for doc in get_wikis_with_topics(fields='id,topic_*'):
        doc_id = doc['id']
        doc = dict([(key, {'set': None}) for key in doc.items()])
        doc['id'] = doc_id

    return requests.post('http://dev-search:8983/solr/xwiki/update?commit=true',
                         data=json.dumps(reinitialize_docs),
                         headers={'Content-type': 'application/json'})


def wiki_data_for_ids(ids):
    return requests.get('http://www.wikia.com/api/v1/Wikis/Details',
                        params={'ids': ','.join(ids)}).json().get('items', {})
