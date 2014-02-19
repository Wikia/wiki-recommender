from flask import Flask, request, render_template, Response
from lib.filters import get_topics_sorted_keys, intersection_count, get_topics_sorted, strip_file, append
from lib.wikis import as_euclidean
import requests
import re
import random
import json
import os

app = Flask(__name__)

"""
Might be a good candidate for a config file.
Another interesting idea w.r.t. automation: argparse + chef recipes
"""
with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config.json'), 'r') as config:
    SOLR_URL = json.loads(config.read())['solr_url'] + 'xwiki'


def get_random_grouping():
    params = dict(rows=50, q='has_topics_b:true', sort='wam_i desc', wt='json',
                  fl='id,sitename_txt,topic_*,top_articles_mv_en,wam_i')
    docs = requests.get('%s/select/' % SOLR_URL, params=params).json().get('response', {}).get('docs', [])
    random.shuffle(docs)
    return docs


@app.route('/topic_data.js')
def topic_js():
    global wiki_topic_data
    return Response("var topic_data = %s;" % json.dumps(wiki_topic_data),
                    mimetype="application/javascript", content_type="application/javascript")


@app.route('/')
def index():
    query = request.args.get('id')
    queried_doc = None
    if query is not None:
        queried_doc, docs = as_euclidean(query)
    else:
        docs = get_random_grouping()

    no_image_url = ("http://slot1.images.wikia.nocookie.net/__cb62407/"
                    + "common/extensions/wikia/Search/images/wiki_image_placeholder.png")
    details = requests.get("http://www.wikia.com/api/v1/Wikis/Details/",
                           params={'ids': ",".join([doc['id'] for doc in docs])}).json().get('items', {})
    for doc in docs:
        if not details.get(doc['id'], {}).get('image', ''):
            details[doc['id']] = dict(details.get(doc['id'], {}).items() + [('image', no_image_url)])

    return render_template('index.html', docs=docs, queried_doc=queried_doc,
                           qs=re.sub(r'id=\d+(&)?', '', request.query_string).replace('&&', '&'), details=details)


def main():
    global app, wiki_topic_data
    app.debug = True
    app.debug = True
    app.add_template_filter(get_topics_sorted_keys, 'topics_sorted_keys')
    app.add_template_filter(intersection_count, 'intersection_count')
    app.add_template_filter(append, 'append')
    app.add_template_filter(strip_file, 'strip_file')
    app.add_template_filter(get_topics_sorted, 'topics_sorted')
    app.run('0.0.0.0', port=5002)


if __name__ == '__main__':
    wiki_topic_data = None
    main()



