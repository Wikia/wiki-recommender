import requests
#from multiprocessing import Process, Queue
from multiprocessing import Process
from multiprocessing.queues import SimpleQueue as Queue
from wikis import as_euclidean


class Query(object):

    rows = 100
    workers = 8

    def __init__(self, query=None, server='dev-search', core='xwiki'):
        if core not in ('xwiki', 'main'):
            raise ValueError('core must be "xwiki" or "main"')

        self.host = 'http://%s:8983/solr/%s' % (server, core)
        self.query = 'has_topics_b:true'
        if query is not None:
            self.query = '%s AND ' % query + self.query

        self.query_queue = Queue()
        self.doc_queue = Queue()
        self.recommendation_queue = Queue()

    def get_params(self, start):
        return {
            'q': self.query,
            'fl': 'id',
            'start': start,
            'rows': Query.rows,
            'wt': 'json'
            }

    def get_docs_from_query(self, start):
        params = self.get_params(start)
        response = requests.get(self.host + '/select', params=params,
                                timeout=300).json()
        #self.num_found = response['response']['numFound']
        return response['response']['docs']

    def get_docs_from_query_worker(self):
        for start in iter(self.query_queue.get, None):
            for doc in self.get_docs_from_query(start):
                #print 'Putting %s' % doc['id']
                self.doc_queue.put(doc)

    def get_recommendations_from_doc(self, doc):
        try:
            wiki_doc, recommendations = as_euclidean(doc['id'], self.host)
            wiki_doc['recommendation_ids'] = [x.get('id', '?') for x in recommendations]
            wiki_doc['recommendation_urls'] = [x.get('url', '?') for x in recommendations]
            wiki_doc['recommendation_sitenames'] = [' '.join(x.get('sitename_txt', ['?'])) for x in recommendations]
            print '%s, %s' % (wiki_doc['sitename_txt'][0], wiki_doc['recommendation_sitenames'])

            if 'url' not in wiki_doc:
                return None
            return wiki_doc
        except ValueError as e:
            print e
            return None

    def get_recommendations_from_doc_worker(self):
        for doc in iter(self.doc_queue.get, None):
            wiki_doc = self.get_recommendations_from_doc(doc)
            self.recommendation_queue.put(wiki_doc)

    def get_recommendations_from_query(self):
        params = self.get_params(0)
        response = requests.get(self.host + '/select', params=params,
                                timeout=300).json()
        num_found = response['response']['numFound']
        for start in range(0, num_found, Query.rows):
            self.query_queue.put(start)

        doc_processes = [Process(target=self.get_docs_from_query_worker) for n in range(Query.workers)]

        for doc_process in doc_processes:
            doc_process.start()
        #for doc_process in doc_processes:
        #    doc_process.join()

        print '# found: %d' % num_found
        #print 'size of doc queue: %d' % len(self.doc_queue)

        #for doc in self.doc_queue:
        #    print doc

        #count = 0
        ##while True:
        ##    count += 1
        ##    print count
        ##    print self.doc_queue.get()
        ##    #if self.doc_queue.empty():
        ##    #    raise StopIteration

        #for doc in iter(self.doc_queue.get, None):
        #    count += 1
        #    print count
        #    print doc
        #    if doc is None:
        #        raise StopIteration


        ##while not self.doc_queue.empty():
        ##    print self.doc_queue.get()

        rec_processes = [Process(target=self.get_recommendations_from_doc_worker) for n in range(Query.workers)]

        for rec_process in rec_processes:
            rec_process.start()

        #for doc in q:
        #    print '%s, %s' % (doc['sitename_txt'][0], doc['recommendation_sitenames'])

#class QueryIterator(object):
#    def __init__(self, query=None, server='dev-search', core='xwiki'):
#        if core not in ('xwiki', 'main'):
#            raise ValueError('core must be "xwiki" or "main"')
#
#        self.host = 'http://%s:8983/solr/%s' % (server, core)
#        self.query = 'has_topics_b:true'
#        if query is not None:
#            self.query = '%s AND ' % query + self.query
#        self.start = 0
#        self.rows = 100
#        self.docs = []
#        self.num_found = None
#        self.at = 0
#
#        self.query_queue = Queue()
#        self.doc_queue = Queue()
#        self.recommendation_queue = Queue()
#
#        self.get_more_docs()
#
#    def __iter__(self):
#        return self
#
#    def get_params(self):
#        return {
#            'q': self.query,
#            'fl': 'id',
#            'start': self.start,
#            'rows': self.rows,
#            'wt': 'json'
#            }
#
#    def get_more_docs(self):
#        if self.num_found is not None and self.start >= self.num_found:
#            raise StopIteration
#        response = requests.get(self.host + '/select',
#                                params=self.get_params(), timeout=300).json()
#        self.num_found = response['response']['numFound']
#        self.docs = response['response']['docs']
#        self.start += self.rows
#        return True
#
#    def next(self):
#        if (len(self.docs) == 0):
#            self.get_more_docs()
#        self.at += 1
#        try:
#            doc, recommendations = as_euclidean(self.docs.pop()['id'], self.host)
#            doc['recommendation_ids'] = [x.get('id', '?') for x in recommendations]
#            doc['recommendation_urls'] = [x.get('url', '?') for x in recommendations]
#            doc['recommendation_sitenames'] = [' '.join(x.get('sitename_txt', ['?'])) for x in recommendations]
#            if 'url' not in doc:
#                return None
#            return doc
#        except ValueError as e:
#            print e
#            return None
#        #return self.docs.pop()

if __name__ == '__main__':
    #q = QueryIterator('lang_s:en')
    #q = QueryIterator('wid:831 AND iscontent:true', core='main')
    #for doc in q:
    #    print '%s, %s' % (doc['sitename_txt'][0], doc['recommendation_sitenames'])

    q = Query()
    q.get_recommendations_from_query()
