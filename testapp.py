from flask import Flask, request, render_template
import requests
import sys
import re
import random

app = Flask(__name__)

"""
Might be a good candidate for a config file.
Another interesting idea w.r.t. automation: argparse + chef recipes
"""
SOLR_URL = sys.argv[1]


@app.template_filter('topics_sorted')
def get_topics_sorted(doc):
    return sorted([(key, doc[key]) for key in doc.keys() if re.match(r'topic_\d+_tf', key) is not None], reverse=True, key=lambda x:x[1])

@app.template_filter('intersection_count')
def intersection_count(tuples1, tuples2):
    return len([x for x in tuples1 if x[0] in [y[0] for y in tuples2]])

@app.template_filter('topics_sorted_keys')
def get_topics_sorted(doc):
    return sorted([key for key in doc.keys() if re.match(r'topic_\d+_tf', key) is not None], reverse=True, key=lambda x:x[1])

@app.template_filter('append')
def append(list, val):
    return list + [val]


def get_random_grouping():
    params = dict(rows=50, q='*:*', sort='wam_i desc', wt='json', fl='id,sitename_txt,topic_*,top_articles_mv_en')
    docs = requests.get('%s/select/' % SOLR_URL, params=params).json().get('response', {}).get('docs', [])
    random.shuffle(docs)
    return docs


def get_similar(query, wam_boost=None, topic_boost='1000', delta=0.15, naive=False, use_titles=False):
    doc_response = requests.get('%s/select/' % SOLR_URL, params=dict(rows=1, q='id:%s' % query, wt='json')).json()
    doc = doc_response.get('response', {}).get('docs', [None])[0]
    if doc is None:
        return []  # same diff

    """
    Let's boost the query by the value of the topics, with boost
    """
    bf = ['%s^%s' % (key, topic_boost) for key in doc.keys() if re.match(r'topic_\d+_tf', key) is not None]

    if wam_boost is not None:
        bf += ['wam_i^%s' % wam_boost]

    if float(delta) != 0:
        q = " OR ".join(['%s:[%.3f TO %.3f]' % (key, float(doc[key]) - float(delta), float(doc[key]) + float(delta)) for key in doc.keys() if re.match(r'topic_\d+_tf', key) is not None])
    else:
        q = '*:*'

    params = dict(rows=20, q=q, bf=bf, wt='json', defType='edismax', fl='id,sitename_txt,topic_*')

    if naive:
        params['q'] = " OR ".join(['%s:[%.3f TO %.3f]' % (x[0], float(x[1]) - float(delta), float(x[1]) + float(delta)) for x in get_topics_sorted(doc)[:4]])
        del params['bf']
        if wam_boost is not None:
            params['bf'] = ['wam_i^%s' % wam_boost]

    """ TODO -- needs solr query syntax, sooooo
    if use_titles:
        params['bq'] = " OR ".join(['("%s")' % article for article in doc.get('top_articles_mv_en', [])[:10]])
        params['qf'] = "top_articles_mv_en,top_categories_mv_en,sitename_txt,description_en,headline_txt"
    """

    params['boost'] = map(lambda x: re.sub(r'\^\d+', '', x), params['bf'])
    del params['bf']

    return (doc, requests.get('%s/select/' % SOLR_URL, params=params).json().get('response', {}).get('docs', []))

@app.route('/')
def index():
    query = request.args.get('id')
    queried_doc = None
    if query is not None:
        queried_doc, docs = get_similar(query, 
                                        wam_boost=request.args.get('wam_boost'), 
                                        topic_boost=request.args.get('topic_boost', 1000), 
                                        delta=request.args.get('delta', 0.15),
                                        naive=bool(request.args.get('naive', False)),
                                        use_titles=bool(request.args.get('use_titles', False)))
    else:
        docs = get_random_grouping()
    docs = [doc for doc in docs if len(doc.keys()) > 2]  # dropping topicless wikis
    return render_template('index.html', docs=docs, queried_doc=queried_doc, qs=re.sub(r'id=\d+(&)?', '', request.query_string).replace('&&', '&'))

app.debug = True
app.run()
