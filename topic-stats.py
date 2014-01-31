import sys
import requests
import xlwt
import re



SOLR_URL = 'http://dev-search:8983/solr/xwiki'


def get_topics_sorted_keys(doc):
    return sorted([key for key in doc.keys() if re.match(r'topic_\d+_tf', key) is not None and doc[key] > 0],
                  reverse=True, key=lambda x: x[1])


def get_doc(wiki_id):
    try:
        return requests.get('%s/select/' % SOLR_URL,
                             params=dict(rows=1, q='id:%s' % wiki_id, wt='json')
                             ).json().get('response', {}).get('docs', [{}]).pop()
    except IndexError:
        return {}


topic_counts = [topic
                for topics in map(get_topics_sorted_keys,
                                  map(get_doc, [ln.strip() for ln in open(sys.argv[1], 'r')]))
                for topic in topics
                ]

gotcha = sorted([(topic, topic_counts.count(topic)) for topic in set(topic_counts)], key=lambda x: x[1], reverse=True)
for topic, count in gotcha:
    print topic, "\t", count

print len(gotcha), "topics"