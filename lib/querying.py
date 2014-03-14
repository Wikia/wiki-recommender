import requests
from lib.filters import get_topics_sorted_keys


def get_by_id(doc_id, endpoint='http://dev-search:8983/solr/xwiki'):
    doc_response = requests.get('%s/select/' % endpoint, params=dict(rows=1, q='id:%s' % doc_id, wt='json')).json()
    return doc_response.get('response', {}).get('docs', [None])[0]


def as_euclidean(doc_id, solr_url='http://dev-search:8983/solr/',
                 core='xwiki', requested_fields='id,sitename_txt,topic_*,wam_i,url,'):
    endpoint = solr_url+core
    doc = get_by_id(doc_id, endpoint)

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
              'fl': requested_fields+sort}

    docs = requests.get('%s/select/' % solr_url, params=params).json().get('response', {}).get('docs', [])
    map(lambda x: x.__setitem__('score', x[sort]), docs)

    return doc, docs


class QueryIterator(object):
    def __init__(self, query=None, server='dev-search', core='xwiki', params={}):
        if core not in ('xwiki', 'main'):
            raise ValueError('core must be "xwiki" or "main"')

        self.host = 'http://%s:8983/solr/%s' % (server, core)
        self.query = 'has_topics_b:true'
        if query is not None:
            self.query = '%s AND ' % query + self.query
        self.params = params
        if 'start' not in params:
            self.params['start'] = 0
        if 'rows' not in params:
            self.params['rows'] = 1000
        if 'fl' not in params:
            self.params['fl'] = 'id'
        self.params['wt'] = 'json'
        self.params['q'] = self.query
        self.docs = []
        self.num_found = None
        self.at = 0

        self.get_more_docs()

    def __iter__(self):
        return self

    def get_more_docs(self):
        if self.num_found is not None and self.params['start'] >= self.num_found:
            raise StopIteration
        response = requests.get(self.host + '/select',
                                params=self.params).json()
        self.num_found = response['response']['numFound']
        self.docs = response['response']['docs']
        self.params['start'] += self.params['rows']
        return True

    def next(self):
        if not self.docs:
            self.get_more_docs()
        self.at += 1
        try:
            return self.docs.pop()
        except ValueError as e:
            print e
            return None


if __name__ == '__main__':
    pass
