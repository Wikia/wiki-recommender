import requests
import json


def csv_to_solr(fl, endpoint='http://dev-search:8983/solr/main', num_topics=999, reset_callback=None):
    update_docs = []
    for line in fl:
        ploded = line[:-1].split(',')
        wid = ploded[0]
        doc = dict(id=wid, has_topics_b={'set': True})
        # initialize
        for i in range(0, num_topics):
            doc['topic_%d_tf' % i] = {'set': 0}
        doc.update(dict([('topic_%s_tf' % topic, {'set': value})
                         for topic, value in
                         [tuple(grouping.split('-')) for grouping in ploded[1:]]]))
        update_docs.append(doc)

    if reset_callback:
        reset_callback()

    requests.post('%s/update?commit=true' % endpoint,
                  data=json.dumps(update_docs),
                  headers={'Content-type': 'application/json'})

    return True