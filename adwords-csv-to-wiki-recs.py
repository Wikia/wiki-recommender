import sys
import requests
import numpy
import xlwt


SOLR_URL = 'http://dev-search:8983/solr/xwiki'
TOPIC_WIKIS = None
WORKSHEET_COUNTERS = dict()


def get_all_wikis_for_topics(topics):
    q = '+(%s)' % ' OR '.join(['topic_%s_tf:*' % i for i in topics])
    params = {'wt': 'json', 'q': q, 'rows': 500, 'start': 0}
    wikis = []
    while True:
        response = requests.get('%s/select/' % SOLR_URL, params=params).json().get('response')
        wikis += response['docs']
        params['start'] += 500
        if response['numFound'] > params['start']:
            break
    return wikis


def wikis_for_topics_bf(topics, current_wikis):

    q = '-(%s)' % (' AND '.join(['topic_%s_tf:0' % topic.split('=')[1] for topic in topics]))
    q += ' +(%s)' % (' OR '.join(['topic_%s_tf:*' % topic.split('=')[1] for topic in topics]))

    params = {'wt': 'json',
              'q': q,
              'rows': 20,
              'bf': ['topic_%s_tf^100' % topic.split('=')[1] for topic in topics] + ['wam_i'],
              'fl': 'id,sitename_txt,topic_*,wam_i,url'
    }

    response = requests.get('%s/select/' % SOLR_URL, params=params)
    try:
        docs = response.json().get('response', {}).get('docs', [])
    except:
        docs = []

    return docs


def wikis_for_topics_euclidean(topics, current_wikis):

    topics = [topic.split('=')[1] for topic in topics]

    q = '-(%s)' % (' AND '.join(['topic_%s_tf:0' % topic for topic in topics]))
    q += '+(%s)' % (' OR '.join(['topic_%s_tf:*' % topic for topic in topics]))

    sort = 'dist(2, vector(%s), vector(%s)) asc' % (
        ', '.join(['topic_%s_tf' % topic for topic in topics]),
        ','.join(['1' for topic in topics])
    )

    params = {'wt': 'json',
              'q': q,
              'rows': 20,
              'sort': sort,
              'fl': 'id,sitename_txt,topic_*,wam_i,url'
    }

    response = requests.get('%s/select/' % SOLR_URL, params=params)
    try:
        docs = response.json().get('response', {}).get('docs', [])
    except:
        docs = []

    return docs


def wikis_for_topics_sorted_top_wikis(topics, current_wikis):

    topics = [topic.split('=')[1] for topic in topics]
    wikis = get_all_wikis_for_topics(topics)
    wikis_averages = []
    for wiki in wikis:
        wikis_averages.append((wiki, numpy.mean([wiki.get('topic_%s_tf' % tpc, 0) for tpc in topics])))

    return [w[0] for w in sorted(wikis_averages, key=lambda x: x[1], reverse=True)[:20]]


def wikis_for_topics_cosine_from_wikis(topics, current_wikis):

    wikis = [w[0].replace('s1=_', '') for w in current_wikis]

    #probably need to iterate over wikis, since we're not getting good ones
    wikidocs = []

    for i in range(0, min([len(wikis), 50])):
        params = {
            'wt': 'json',
            'q': 'url:http\://%s.wikia.com/' % wikis[i],
            'rows': 20,
            'fl': 'topic_*'
        }
        response = requests.get('%s/select/' % SOLR_URL, params=params)
        wikidocs += response.json().get('response', {}).get('docs', [])
    topic_store = dict()
    topicdocs = []

    for doc in wikidocs:
        for field in doc:
            if doc[field] > 0:
                topic_store[field] = topic_store.get(field, []) + [doc[field]]
                if doc not in topicdocs:
                    topicdocs.append(doc)

    if len(topicdocs) == 0:
        return []

    q = '-(%s)' % (' AND '.join(['%s:0' % topic for topic in topic_store]))
    q += '+(%s)' % (' OR '.join(['%s:*' % topic for topic in topic_store]))

    sort = 'div(sum(%s), %d) asc' % (
        ",".join([
            "dist(2, vector(%s),vector(%s))" % (
                ','.join(['%s' % topic for topic in topic_store]),
                ','.join(['%.6f' % doc.get(field, 0) for field in topic_store])
            )
            for doc in topicdocs[:5]
        ]),
        len(topicdocs)
    )

    params = {'wt': 'json',
              'q': q,
              'rows': 20,
              'sort': sort,
              'fl': 'id,sitename_txt,topic_*,wam_i,url'
    }

    response = requests.get('%s/select' % SOLR_URL, params=params)
    try:
        docs = response.json().get('response', {}).get('docs', [])
    except:
        docs = []

    return docs


def initialize_worksheet(workbook, title):
    worksheet = workbook.add_sheet(title)
    worksheet.write(0, 0, 'Line Item Name')
    worksheet.write(0, 1, 'Wiki ID')
    worksheet.write(0, 2, 'URL')
    worksheet.write(0, 3, 'Name')
    return worksheet


def write_to_worksheet(worksheet, lineitem_name, docs):
    global WORKSHEET_COUNTERS
    start = WORKSHEET_COUNTERS.get(worksheet, 0)
    for i in range(0, len(docs)):
        row = start+i + 1
        doc = docs[i]
        worksheet.write(row, 0, lineitem_name)
        worksheet.write(row, 1, doc.get('id', '?'))
        worksheet.write(row, 2, doc.get('url', '?'))
        worksheet.write(row, 3, doc.get('sitename_txt', ['?'])[0])
        WORKSHEET_COUNTERS[worksheet] = row


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

my_workbook = xlwt.Workbook()
top_topics_worksheet = my_workbook.add_sheet("Top Topics")
top_topics_worksheet.write(0, 0, 'Line Item Name')
[top_topics_worksheet.write(0, i, "Topic %d" % i) for i in range(1, 6)]
worksheets = [initialize_worksheet(my_workbook, char)
              for char in ['Algo A Recs', 'Algo B Recs', 'Algo C Recs', 'Algo D Recs']]
topics_counter = 1
for lineitem in lineitem_to_data:
    by_ctr = sorted(lineitem_to_data[lineitem], key=lambda x: float(x[-1].strip('%')), reverse=True)
    best_topics = [a[0] for a in by_ctr[:3]]  # for now
    top_topics_worksheet.write(topics_counter, 0, lineitem)
    [top_topics_worksheet.write(topics_counter, i, by_ctr[i-1][0]) for i in range(1, 6)]
    topics_counter += 1
    if len(best_topics) == 0:
        continue
    print lineitem, "recommendations"

    print "\t=== from topics bf ==="
    bf_topic_wikis = wikis_for_topics_bf(best_topics, lineitem_current_wikidomains[lineitem])
    write_to_worksheet(worksheets[0], lineitem, bf_topic_wikis)
    for wiki in bf_topic_wikis[:5]:
        print "\t", wiki.get('sitename_txt', wiki.get('url'))

    print "\t=== from topics euclidean ==="
    wft_euclidean_wikis = wikis_for_topics_euclidean(best_topics, lineitem_current_wikidomains[lineitem])
    write_to_worksheet(worksheets[1], lineitem, wft_euclidean_wikis)
    for wiki in wft_euclidean_wikis[:5]:
        print "\t", wiki.get('sitename_txt', wiki.get('url'))

    print "\t=== from wikis ==="
    wftc_wikis = wikis_for_topics_cosine_from_wikis(best_topics, lineitem_current_wikidomains[lineitem])
    write_to_worksheet(worksheets[2], lineitem, wftc_wikis)
    for wiki in wftc_wikis[:5]:
        print "\t", wiki.get('sitename_txt', wiki.get('url'))

    print "\t=== from wiki topic averages ==="

    wfts_wikis = wikis_for_topics_sorted_top_wikis(best_topics, lineitem_current_wikidomains[lineitem])
    write_to_worksheet(worksheets[3], lineitem, wfts_wikis)
    for wiki in wfts_wikis[:5]:
        print "\t", wiki.get('sitename_txt', wiki.get('url'))

my_workbook.save('recommendations.xls')
