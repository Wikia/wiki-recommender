import requests
import json
from multiprocessing import Pool, Value


def process_linegroup(tup):
    endpoint, shared_counter, initialize_doc, linegroup = tup

    update_docs = []
    for line in linegroup:
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

    shared_counter.value += len(update_docs)
    print shared_counter.value

    return requests.post('%s/update' % endpoint,
                         data=json.dumps(update_docs),
                         headers={'Content-type': 'application/json'})


def csv_to_solr(fl, endpoint='http://dev-search:8983/solr/main', num_topics=999, reset_callback=None):

    if reset_callback:
        print "Resetting (no way back now!)"
        reset_callback()

    print 'generating updates'
    initialize_doc = dict([('topic_%d_tf' % i, {'set': 0}) for i in range(1, num_topics)])
    p = Pool(processes=8)
    counter = Value('i', 0)
    lines = [line for line in fl]
    groupings = [(endpoint, counter, initialize_doc, lines[i:i+10000]) for i in range(0, len(lines), 10000)]
    del lines  # save dat memory my g

    print 'processing line groups'
    p.map_async(process_linegroup, groupings)

    print "Committing..."
    requests.post('%s/update?commit=true' % endpoint)

    return True