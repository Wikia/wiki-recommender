from flask import Flask, request, render_template
from lib.filters import get_topics_sorted_keys, intersection_count, get_topics_sorted, strip_file, append
from lib.querying import get_by_id
from argparse import ArgumentParser
import requests
import re
import random


SOLR_URL = None
app = Flask(__name__)


def get_args():
    ap = ArgumentParser()
    ap.add_argument('--solr_url', dest='solr_url', default='http://dev-search:8983/solr/main')
    return ap.parse_args()


def get_random_grouping():
    params = dict(rows=25, q='is_video:true AND recommendations_ss:*', sort='views desc',
                  wt='json', fl='id,title_en,topic_*,url,wid,wikititle_en,recommendations_ss')
    params['start'] = request.args.get('start', int(random.randint(0, 50000)))
    docs = requests.get('%s/select/' % SOLR_URL, params=params).json().get('response', {}).get('docs', [])
    return docs


def get_for_ids(docids):
    query = " OR ".join(["id:%s" % docid for docid in docids])
    params = dict(rows=25, query=query, wt='json', fl='id,title_en,url,wid,wikititle_en')
    docs = requests.get('%s/select/' % SOLR_URL, params=params).json().get('response', {}).get('docs', [])
    hashed = dict([(doc['id'], doc) for doc in docs])
    docs_sorted = [hashed[docid] for docid in docids if docid in hashed]
    return docs_sorted


@app.route('/')
def index():
    global SOLR_URL
    query = request.args.get('id')
    queried_doc = None
    if query is not None:
        doc = get_by_id(query, endpoint=SOLR_URL)
        print doc
        docs = get_for_ids(doc['recommendations_ss'])
    else:
        docs = get_random_grouping()

    details = {}
    no_image_url = ("http://slot1.images.wikia.nocookie.net/"
                    + "__cb62407/common/extensions/wikia/Search/images/wiki_image_placeholder.png")

    items = requests.get(docs[0]['url'].split('/wiki')[0]+'/api/v1/Articles/Details',
                         params={'ids': ','.join(doc['id'].split('_')[1] for doc in docs)}).json().get('items', {})

    for doc in docs:
        details[doc['id']] = items.get(doc['id'].split('_')[1], {})
        details[doc['id']]['thumbnail'] = details[doc['id']].get('thumbnail', no_image_url)

    return render_template('video_index.html',
                           docs=docs,
                           details=details,
                           queried_doc=queried_doc,
                           qs=re.sub(r'id=\d+(&)?', '', request.query_string).replace('&&', '&'))


def main():
    global SOLR_URL, video_topic_data, app

    SOLR_URL = get_args().solr_url

    app.debug = True
    app.add_template_filter(get_topics_sorted_keys, 'topics_sorted_keys')
    app.add_template_filter(intersection_count, 'intersection_count')
    app.add_template_filter(append, 'append')
    app.add_template_filter(strip_file, 'strip_file')
    app.add_template_filter(get_topics_sorted, 'topics_sorted')
    app.run('0.0.0.0')

if __name__ == '__main__':
    video_topic_data = None
    main()
