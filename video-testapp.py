from flask import Flask, request, render_template, Response
from lib.filters import get_topics_sorted_keys, intersection_count, get_topics_sorted, strip_file, append
import requests
import re
import random
import json
import os

app = Flask(__name__)


def get_random_grouping():
    params = dict(rows=25, q='is_video:true', sort='views desc',
                  wt='json', fl='id,title_en,topic_*,url,wid,wikititle_en')
    params['start'] = request.args.get('start', int(random.randint(0, 50000)))
    docs = requests.get('%s/select/' % SOLR_URL, params=params).json().get('response', {}).get('docs', [])
    return docs


def as_euclidean(query):
    doc_response = requests.get('%s/select/' % SOLR_URL, params=dict(rows=1, q='id:%s' % query, wt='json')).json()
    doc = doc_response.get('response', {}).get('docs', [None])[0]
    if doc is None:
        return None, []  # same diff

    keys = get_topics_sorted_keys(doc)

    sort = 'dist(2, vector(%s), vector(%s))' % (", ".join(keys), ", ".join(['%.8f' % doc[key] for key in keys]))

    params = {'wt': 'json',
              'q': 'title_en:* AND is_video:true AND -(%s)' % " AND ".join(["%s:0" % key for key in keys]),
              'sort': sort + ' asc',
              'rows': 20,
              'fq': '-id:%s' % doc['id'],
              'fl': 'id,wikititle_en,title_en,topic_*,wam,wid,url,'+sort}

    if request.args.get('nosame'):
        params['q'] += ' AND -wid:'+str(doc['wid'])

    docs = requests.get('%s/select/' % SOLR_URL, params=params).json().get('response', {}).get('docs', [])
    map(lambda x: x.__setitem__('score', x[sort]), docs)

    return doc, docs


@app.route('/topic_data.js')
def topic_js():
    global video_topic_data
    return Response("var topic_data = %s;" % json.dumps(video_topic_data),
                    mimetype="application/javascript", content_type="application/javascript")


@app.route('/')
def index():
    query = request.args.get('id')
    queried_doc = None
    if query is not None:
        queried_doc, docs = as_euclidean(query)
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
    """
    Might be a good candidate for a config file.
    Another interesting idea w.r.t. automation: argparse + chef recipes
    """
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config.json'), 'r') as config:
        SOLR_URL = json.loads(config.read())['solr_url'] + 'main'

    with open('video-999topics-words.txt', 'r') as topics_file:
        video_topic_data = dict([('topic_%d_tf' % x[0], x[1][:-1]) for x in enumerate(topics_file)])

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
