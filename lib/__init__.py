import requests
import json
from multiprocessing import Pool


def process_linegroup(tup):
    endpoint, initialize_doc, linegroup = tup

    print endpoint

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

    response = requests.post('%s/update' % endpoint,
                             data=json.dumps(update_docs),
                             headers={'Content-type': 'application/json'})
    print response.content

    return response


def csv_to_solr(fl, endpoint='http://dev-search:8983/solr/main', num_topics=999, reset_callback=None):

    if reset_callback is not None:
        print "Resetting (no way back now!)"
        reset_callback()

    print endpoint
    print 'generating updates'
    initialize_doc = dict([('topic_%d_tf' % i, {'set': 0}) for i in range(1, num_topics)])
    p = Pool(processes=8)
    lines = [line for line in fl]
    groupings = [(endpoint, initialize_doc, lines[i:i+10000]) for i in range(0, len(lines), 10000)]
    del lines  # save dat memory my g

    print 'processing line groups'
    print p.map_async(process_linegroup, groupings).wait()

    print "Committing..."
    requests.post('%s/update?commit=true' % endpoint)

    return True