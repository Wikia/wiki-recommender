import requests
import json
from multiprocessing import Pool



def send_update_docs_to_endpoint(endpoint, update_docs):
    requests.post('%s/update' % endpoint,
                  data=json.dumps(update_docs),
                  headers={'Content-type': 'application/json'})


def csv_to_solr(fl, endpoint='http://dev-search:8983/solr/main', num_topics=999, reset_callback=None):

    if reset_callback:
        print "Resetting (no way back now!)"
        reset_callback()

    print 'generating update docs'
    update_docs = []
    counter = 0
    initialize_doc = dict([('topic_%d_tf' % i, {'set': 0}) for i in range(1, num_topics)])
    p = Pool(processes=8)
    for line in fl:
        ploded = line[:-1].split(',')
        wid = ploded[0]
        # initialize with no topics
        doc = dict(id=wid, has_topics_b={'set': True})
        doc.update(initialize_doc)
        # overwrite with topics
        doc.update(dict([('topic_%s_tf' % topic, {'set': value})
                         for topic, value in
                         [tuple(grouping.split('-')) for grouping in ploded[1:]]]))
        update_docs.append(doc)
        if len(update_docs) >= 10000:
            counter += len(update_docs)
            print counter
            p.apply_async(send_update_docs_to_endpoint, update_docs)
            update_docs = []

    requests.post('%s/update?commit=true' % endpoint,
                  data=json.dumps(update_docs),
                  headers={'Content-type': 'application/json'})

    return True