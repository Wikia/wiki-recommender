import requests
import json


def csv_to_solr(fl, endpoint='http://dev-search:8983/solr/main', num_topics=999, reset_callback=None):
    update_docs = []

    if reset_callback:
        print "Resetting (no way back now!)"
        reset_callback()

    print 'generating update docs'
    counter = 0
    for line in fl:
        ploded = line[:-1].split(',')
        wid = ploded[0]
        # initialize with no topics
        doc = dict([('topic_%d_tf' % i, {'set': 0}) for i in range(1, 1000)])
        doc.update(dict(id=wid, has_topics_b={'set': True}))
        # overwrite with topics
        doc.update(dict([('topic_%s_tf' % topic, {'set': value})
                         for topic, value in
                         [tuple(grouping.split('-')) for grouping in ploded[1:]]]))
        update_docs.append(doc)
        if len(update_docs) >= 10000:
            counter += len(update_docs)
            print counter
            requests.post('%s/update?commit=true' % endpoint,
                          data=json.dumps(update_docs),
                          headers={'Content-type': 'application/json'})
            update_docs = []

    requests.post('%s/update?commit=true' % endpoint,
                  data=json.dumps(update_docs),
                  headers={'Content-type': 'application/json'})

    return True