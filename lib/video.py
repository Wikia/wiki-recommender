import requests
import json

VIDEO_WIKI_ID = 298117


def reset_video_results(search_server='http://search-s10:8983', data_server='http://dev-search:8983'):
    delete_params = {'delete': {'query': 'wid:%d' % VIDEO_WIKI_ID}}
    requests.post('%s/solr/main/update?commit=true' % data_server,
                  data=json.dumps(delete_params),
                  headers={'Content-type': 'application/json'})
    search_query_params = {'q': 'wid:%d' % VIDEO_WIKI_ID, 'offset': 0, 'rows': 5000, 'fl': '*', 'wt': 'json'}
    while True:
        print search_query_params['offset']
        response = requests.get('%s/solr/main/select' % search_server,
                                params=search_query_params).json().get('response')
        print response.get('numFound')
        docs = response.get('docs', [])
        print len(docs)
        if len(docs) == 0:
            break
        requests.post('%s/solr/main/update' % data_server,
                      data=json.dumps(docs),
                      headers={'Content-type': 'application/json'})
        search_query_params['offset'] += search_query_params['rows']
    return True

