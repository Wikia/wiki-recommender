import sys
import requests
import numpy


SOLR_URL = 'http://dev-search:8983/solr/xwiki'


def wikis_for_topics_bf(topics, current_wikis):

    q = '-(%s)' % (' AND '.join(['topic_%s_tf:0' % topic.split('=')[1] for topic in topics]))
    q += '+(%s)' % (' OR '.join(['topic_%s_tf:*' % topic.split('=')[1] for topic in topics]))
    q += ' AND -(%s) ' % (' OR '.join(['url:http\://%s.wikia.com/' % (w[0].replace('s1=_', '')) for w in current_wikis[:10]]))

    params = {'wt': 'json',
              'q': q,
              'rows': 5,
              'bf': ['topic_%s_tf^100' % topic.split('=')[1] for topic in topics] + ['wam_i'],
              'fl': 'id,sitename_txt,topic_*,wam_i,url'
    }

    response = requests.get('%s/select/' % SOLR_URL, params=params)
    try:
        docs = response.json().get('response', {}).get('docs', [])
    except:
        docs = []

    return docs


def wikis_for_topics_cosine(topics, current_wikis):

    topics = [topic.split('=')[1] for topic in topics]

    q = '-(%s)' % (' AND '.join(['topic_%s_tf:0' % topic for topic in topics]))
    q += '+(%s)' % (' OR '.join(['topic_%s_tf:*' % topic for topic in topics]))
    q += ' AND -(%s) ' % (' OR '.join(['url:http\://%s.wikia.com/' % (w[0].replace('s1=_', '')) for w in current_wikis[:10]]))

    sort = 'dist(2, vector(%s), vector(%s)) asc' % (
        ', '.join(['topic_%s_tf' % topic for topic in topics]),
        ','.join(['1' for topic in topics])
    )

    params = {'wt': 'json',
              'q': q,
              'rows': 5,
              'sort': sort,
              'fl': 'id,sitename_txt,topic_*,wam_i,url'
    }

    response = requests.get('%s/select/' % SOLR_URL, params=params)
    try:
        docs = response.json().get('response', {}).get('docs', [])
    except:
        docs = []

    return docs


def wikis_for_topics_cosine_from_wikis(topics, current_wikis):

    wikis = [w[0].replace('s1=_', '') for w in current_wikis]

    #probably need to iterate over wikis, since we're not getting good ones

    params = {
        'wt': 'json',
        'q': ' OR '.join(['url:http\://%s.wikia.com/' % w for w in wikis[:50]]),
        'rows': 20,
        'fl': 'topic_*'
    }
    response = requests.get('%s/select/' % SOLR_URL, params=params)
    wikidocs = response.json().get('response', {}).get('docs', [])
    topic_store = dict()

    if len(wikidocs) == 0:
        return []

    for doc in wikidocs:
        for field in doc:
            if doc[field] > 0:
                topic_store[field] = topic_store.get(field, []) + [doc[field]]

    q = '-(%s)' % (' AND '.join(['%s:0' % topic for topic in topic_store]))
    q += '+(%s)' % (' OR '.join(['%s:*' % topic for topic in topic_store]))

    sort = 'sqedist(vector(%s),%s) asc' % (
        ','.join(['%s' % topic for topic in topic_store]),
        ','.join(['vector(%s)' % ', '.join(['%.6f' % doc.get(field, 0) for field in topic_store]) for doc in [wikidocs[0]]])
    )

    params = {'wt': 'json',
              'q': q,
              'rows': 5,
              'sort': sort,
              'fl': 'id,sitename_txt,topic_*,wam_i,url'
    }

    response = requests.get('%s/select/' % SOLR_URL, params=params)
    try:
        docs = response.json().get('response', {}).get('docs', [])
    except:
        docs = []

    return docs


lineitem_to_data = dict()
lineitem_current_wikidomains = dict()

with open(sys.argv[1], 'r') as datafl:
    for line in ''.join(datafl.readlines()).split('\r'):
        vals = line.split(',')
        if float(vals[-1].strip('%')) > 0:
            lineitem_to_data[vals[0]] = lineitem_to_data.get(vals[0], []) + [vals[1:]]

with open(sys.argv[2], 'r') as wikifl:
    for line in ''.join(wikifl.readlines()).split('\r'):
        vals = line.split(',')
        if float(vals[-1].strip('%')) > 0:
            lineitem_current_wikidomains[vals[0]] = lineitem_current_wikidomains.get(vals[0], []) + [vals[1:]]

lineitem_recommended_wikis = dict()

for lineitem in lineitem_to_data:
    by_ctr = sorted(lineitem_to_data[lineitem], key=lambda x: float(x[-1].strip('%')), reverse=True)
    best_topics = [a[0] for a in by_ctr[:3]]  # for now
    print lineitem, "recommendations"
    for wiki in wikis_for_topics_cosine_from_wikis(best_topics, lineitem_current_wikidomains[lineitem]):
        print "\t", wiki.get('sitename_txt', wiki.get('url'))