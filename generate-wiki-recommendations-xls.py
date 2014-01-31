import sys
import requests
import xlwt
import re


SOLR_URL = 'http://dev-search:8983/solr/xwiki'


def get_topics_sorted_keys(doc):
    return sorted([key for key in doc.keys() if re.match(r'topic_\d+_tf', key) is not None and doc[key] > 0],
                  reverse=True, key=lambda x: x[1])


def as_euclidean(doc_id):
    doc_response = requests.get('%s/select/' % SOLR_URL, params=dict(rows=1, q='id:%s' % doc_id, wt='json')).json()
    doc = doc_response.get('response', {}).get('docs', [None])[0]
    if doc is None:
        return None, []  # same diff

    keys = get_topics_sorted_keys(doc)

    if not keys:
        return {}, []

    sort = 'dist(2, vector(%s), vector(%s))' % (", ".join(keys), ", ".join(['%.8f' % doc[key] for key in keys]))

    params = {'wt': 'json',
              'q': '-id:%s AND (%s)' % (doc['id'], " OR ".join(['(%s:*)' % key for key in keys])),
              'sort': sort + ' asc',
              'rows': 20,
              'fl': 'id,sitename_txt,topic_*,wam_i,url,'+sort}

    docs = requests.get('%s/select/' % SOLR_URL, params=params).json().get('response', {}).get('docs', [])
    map(lambda x: x.__setitem__('score', x[sort]), docs)

    return doc, docs


my_workbook = xlwt.Workbook()
ids_worksheet = my_workbook.add_sheet("Wiki IDs")
ids_worksheet.write(0, 0, 'Wiki')
ids_worksheet.write(0, 1, 'Recommendations')

urls_worksheet = my_workbook.add_sheet("Wiki URLs")
urls_worksheet.write(0, 0, 'Wiki')
urls_worksheet.write(0, 1, 'Recommendations')

names_worksheet = my_workbook.add_sheet("Wiki Names")
names_worksheet.write(0, 0, 'Wiki')
names_worksheet.write(0, 1, 'Recommendations')

i = 1
for wiki_id in [line.strip() for line in open(sys.argv[1], 'r')]:
    print wiki_id
    try:
        wiki_doc, recommendations = as_euclidean(wiki_id)
    except ValueError:
        continue
    if 'url' not in wiki_doc or not recommendations:
        continue
    ids_worksheet.write(i, 0, wiki_doc['id'])
    map(lambda j: ids_worksheet.write(i, j, recommendations[j-1].get('id', '?')),
        range(1, len(recommendations)+1))

    urls_worksheet.write(i, 0, wiki_doc['url'])
    map(lambda j: urls_worksheet.write(i, j, recommendations[j-1].get('url', '?')),
        range(1, len(recommendations)+1))

    names_worksheet.write(i, 0, ''.join(wiki_doc['sitename_txt']))
    map(lambda j: names_worksheet.write(i, j, ''.join(recommendations[j-1].get('sitename_txt', ['?']))),
        range(1, len(recommendations)+1))
    i += 1

my_workbook.save("top-wiki-recommendations.xls")