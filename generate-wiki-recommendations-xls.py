import sys
import xlwt
from lib.wikis import as_euclidean, get_wikis_with_topics, csv_to_solr
from multiprocessing import Pool


SOLR_URL = 'http://dev-search:8983/solr/xwiki'


def get_recommendations(wiki):
    wiki_id = wiki['id']
    print wiki_id
    try:
        wiki_doc, recommendations = as_euclidean(wiki_id)
        wiki_doc['recommendation_ids'] = [x.get('id', '?') for x in recommendations]
        wiki_doc['recommendation_urls'] = [x.get('url', '?') for x in recommendations]
        wiki_doc['recommendation_sitenames'] = [' '.join(x.get('sitename_txt', ['?'])) for x in recommendations]
        if 'url' not in wiki_doc:
            return None
        return wiki_doc
    except ValueError as e:
        print e
        return None


def main():
    name = 'latest'
    if len(sys.argv) >= 2:
        print "Reinitializing Solr with desired topics CSV"
        name = sys.argv[1].split('.')[0]
        with open(sys.argv[1], 'r') as fl:
            print csv_to_solr(fl)

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

    pool = Pool(processes=4)
    recommendations_results = filter(lambda x: x, pool.map_async(get_recommendations, get_wikis_with_topics()).get())
    for counter, wiki_doc in enumerate(recommendations_results):
        i = counter + 1
        ids_worksheet.write(i, 0, wiki_doc['id'])
        map(lambda j: ids_worksheet.write(i, j, wiki_doc['recommendation_ids'][j-1]),
            range(1, len(wiki_doc['recommendations_ids'])+1))

        urls_worksheet.write(i, 0, wiki_doc['url'])
        map(lambda j: urls_worksheet.write(i, j, wiki_doc['recommendation_urls'][j-1]),
            range(1, len(wiki_doc['recommendations_urls'])+1))

        names_worksheet.write(i, 0, ''.join(wiki_doc['sitename_txt']))
        map(lambda j: names_worksheet.write(i, j, wiki_doc['recommendations_sitenames'][j-1]),
            range(1, len(wiki_doc['recommendations_sitenames'])+1))

    my_workbook.save(name+"-wiki-recommendations.xls")


if __name__ == '__main__':
    main()