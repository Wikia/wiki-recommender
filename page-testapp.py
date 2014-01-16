from flask import Flask, request, render_template
import requests
import sys
import re
import random
import json
import time
import os

app = Flask(__name__)

"""
Might be a good candidate for a config file.
Another interesting idea w.r.t. automation: argparse + chef recipes
"""
SOLR_URL = json.loads(open(os.path.join(os.path.dirname(os.path.realpath(__file__)) , 'config.json')).read())['solr_url'].replace('xwiki', 'main')


@app.template_filter('topics_sorted')
def get_topics_sorted(doc):
    return sorted([(key, doc[key]) for key in doc.keys() if re.match(r'topic_\d+_tf', key) is not None and doc[key] > 0], reverse=True, key=lambda x:x[1])

@app.template_filter('intersection_count')
def intersection_count(tuples1, tuples2):
    return len([x for x in tuples1 if x[0] in [y[0] for y in tuples2]])

@app.template_filter('topics_sorted_keys')
def get_topics_sorted_keys(doc):
    return sorted([key for key in doc.keys() if re.match(r'topic_\d+_tf', key) is not None and doc[key] > 0], reverse=True, key=lambda x:x[1])

@app.template_filter('append')
def append(list, val):
    return list + [val]


def get_random_grouping():
    params = dict(rows=25, q='title_en:* AND -is_video:true', sort='views desc', wt='json', fl='id,title_en,topic_*,url,wid,wikititle_en')
    params['start'] = request.args.get('start', int(random.randint(0, 50000)))
    docs = requests.get('%s/select/' % SOLR_URL, params=params).json().get('response', {}).get('docs', [])
    return docs


def get_similar_old(query, wam_boost=None, topic_boost='1000', delta=0.15, naive=False, use_titles=False):
    doc_response = requests.get('%s/select/' % SOLR_URL, params=dict(rows=1, q='id:%s' % query, wt='json')).json()
    doc = doc_response.get('response', {}).get('docs', [None])[0]
    if doc is None:
        return None, []  # same diff

    """
    Let's boost the query by the value of the topics, with boost
    """
    bf = ['map(%s, 0, 100, 0, 0)^%s' % (key, topic_boost) for key in doc.keys() if re.match(r'topic_\d+_tf', key) is not None]

    if wam_boost is not None:
        bf += ['wam_i^%s' % wam_boost]

    if float(delta) != 0:
        q = " OR ".join(['%s:[%.3f TO %.3f]' % (key, float(doc[key]) - float(delta), float(doc[key]) + float(delta)) for key in doc.keys() if re.match(r'topic_\d+_tf', key) is not None])
    else:
        q = '*:*'

    params = dict(rows=20, q=q, bf=bf, wt='json', defType='edismax', fl='id,title_en,url,wid,topic_*')

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

    return (doc, requests.get('%s/select/' % SOLR_URL, params=params).json().get('response', {}).get('docs', []))

def mlt(query):
    doc_response = requests.get('%s/select/' % SOLR_URL, params=dict(rows=1, q='id:%s' % query, wt='json')).json()
    doc = doc_response.get('response', {}).get('docs', [None])[0]
    if doc is None:
        return None, []  # same diff

    params = {'wt':'json',
              'q':'-id:%s AND (%s)' % (doc['id'], " OR ".join(['(%s:*)' % key for key in get_topics_sorted_keys(doc)])),
              'bf': ['%s^10' for key in get_topics_sorted_keys(doc)] + ['wam'],
              'rows':20,
              'fl':'id,title_en,wam,wid,topic_*'}

    return (doc, requests.get('%s/select/' % SOLR_URL, params=params).json().get('response', {}).get('docs', []))


def as_euclidean(query):
    doc_response = requests.get('%s/select/' % SOLR_URL, params=dict(rows=1, q='id:%s' % query, wt='json')).json()
    doc = doc_response.get('response', {}).get('docs', [None])[0]
    if doc is None:
        return None, []  # same diff

    keys = get_topics_sorted_keys(doc)

    sort = 'dist(2, vector(%s), vector(%s))' % (", ".join(keys), ", ".join(['%.8f' % doc[key] for key in keys]))

    params = {'wt':'json',
              #'q':'-id:%s AND (%s)' % (doc['id'], " OR ".join(['(%s:*)' % key for key in keys])),
              'q':'title_en:* AND -is_video:true AND -(%s)' % " AND ".join(["%s:0" % key for key in keys]),
              'sort': sort + ' asc',
              'rows':20,
              'fq': '-id:%s' % doc['id'],
              'fl':'id,wikititle_en,title_en,topic_*,wam,wid,url,'+sort}

    if request.args.get('nosame'):
        params['q'] += ' AND -wid:'+str(doc['wid'])

    docs = requests.get('%s/select/' % SOLR_URL, params=params).json().get('response', {}).get('docs', [])
    map(lambda x: x.__setitem__('score', x[sort]), docs)

    return (doc, docs)


@app.route('/')
def index():
    query = request.args.get('id')
    queried_doc = None
    if query is not None:
        """
        queried_doc, docs = get_similar(query,
                                        wam_boost=request.args.get('wam_boost'),
                                        topic_boost=request.args.get('topic_boost', 1000),
                                        delta=request.args.get('delta', 0.15),
                                        naive=bool(request.args.get('naive', False)),
                                        use_titles=bool(request.args.get('use_titles', False)))
        """
        queried_doc, docs = as_euclidean(query)
    else:
        docs = get_random_grouping()

    details = {}
    NO_IMAGE_URL = "http://slot1.images.wikia.nocookie.net/__cb62407/common/extensions/wikia/Search/images/wiki_image_placeholder.png"
    for doc in docs:
        items = [{}]
        try:
            items = requests.get(doc['url'].split('/wiki')[0]+'/api/v1/Articles/Details', 
                                              params={'ids': doc['id'].split('_')[1]}).json().get('items', {123: {}}).values()
        except:
            pass
        details[doc['id']] = items[0] if len(items) > 0 else {}
        details[doc['id']]['thumbnail'] = details[doc['id']].get('thumbnail', NO_IMAGE_URL)

    return render_template('page_index.html', docs=docs, details=details, queried_doc=queried_doc, qs=re.sub(r'id=\d+(&)?', '', request.query_string).replace('&&', '&'))

if __name__ == '__main__':
    app.debug = True
    app.run('0.0.0.0')
