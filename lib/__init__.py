import requests
import json
from multiprocessing import Pool


def process_linegroup(tup):
    endpoint, initialize_doc, linegroup = tup

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

        if len(update_docs == 1000):
            print "Posting to Solr"
            response = requests.post('%s/update' % endpoint,
                                     data=json.dumps(update_docs),
                                     headers={'Content-type': 'application/json'})
            print response.content
            update_docs = []

    if update_docs:
        response = requests.post('%s/update' % endpoint,
                                 data=json.dumps(update_docs),
                                 headers={'Content-type': 'application/json'})

    return response


def csv_to_solr(fl, endpoint='http://dev-search:8983/solr/main', num_topics=999, reset_callback=None):

    if reset_callback is not None:
        print "Resetting (no way back now!)"
        reset_callback()

    print 'generating updates'
    initialize_doc = dict([('topic_%d_tf' % i, {'set': 0}) for i in range(1, num_topics)])
    p = Pool(processes=4)
    line_groupings = [[]]
    grouping_counter = 0
    total_lines = 0
    for line in fl:
        line_groupings[grouping_counter].append(line)
        if len(line_groupings[grouping_counter]) >= 10000:
            if grouping_counter == 3:
                curr_lines = sum(map(len, line_groupings))
                total_lines += curr_lines
                print 'processing line groups for', curr_lines, 'lines', total_lines, 'total'
                groupings = [(endpoint, initialize_doc, line_groupings[i]) for i in range(0, len(line_groupings))]
                print p.map_async(process_linegroup, groupings).get()
                grouping_counter = 0
                line_groupings = [[]]
            else:
                grouping_counter += 1
                line_groupings.append([])

    groupings = [(endpoint, initialize_doc, line_groupings[i]) for i in range(0, len(line_groupings))]
    print p.map_async(process_linegroup, groupings).get()

    print "Committing..."
    requests.post('%s/update?commit=true' % endpoint, headers={'Content-type': 'application/json'})

    return True