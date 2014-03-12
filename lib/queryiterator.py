import requests
from wikis import as_euclidean


class QueryIterator(object):
    def __init__(self, query=None, server='dev-search', core='xwiki'):
        if core not in ('xwiki', 'main'):
            raise ValueError('core must be "xwiki" or "main"')

        self.host = 'http://%s:8983/solr/%s' % (server, core)
        self.query = 'has_topics_b:true'
        if query is not None:
            self.query = '%s AND ' % query + self.query
        self.start = 0
        self.rows = 100
        self.docs = []
        self.num_found = None
        self.at = 0

        self.get_more_docs()

    def __iter__(self):
        return self

    def get_params(self):
        return {
            'q': self.query,
            'fl': 'id',
            'start': self.start,
            'rows': self.rows,
            'wt': 'json'
            }

    def get_more_docs(self):
        if self.num_found is not None and self.start >= self.num_found:
            raise StopIteration
        response = requests.get(self.host + '/select',
                                params=self.get_params(), timeout=300).json()
        self.num_found = response['response']['numFound']
        self.docs = response['response']['docs']
        self.start += self.rows
        return True

    def next(self):
        if (len(self.docs) == 0):
            self.get_more_docs()
        self.at += 1
        try:
            doc, recommendations = as_euclidean(self.docs.pop()['id'], self.host)
            doc['recommendation_ids'] = [x.get('id', '?') for x in recommendations]
            doc['recommendation_urls'] = [x.get('url', '?') for x in recommendations]
            doc['recommendation_sitenames'] = [' '.join(x.get('sitename_txt', ['?'])) for x in recommendations]
            if 'url' not in doc:
                return None
            return doc
        except ValueError as e:
            print e
            return None
        #return self.docs.pop()

if __name__ == '__main__':
    q = QueryIterator('lang_s:en')
    for doc in q:
        print '%s, %s' % (doc['sitename_txt'][0], doc['recommendation_sitenames'])
