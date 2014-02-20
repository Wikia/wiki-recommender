import sys
import xlwt
from lib.wikis import as_euclidean, get_wikis_with_topics, csv_to_solr


SOLR_URL = 'http://dev-search:8983/solr/xwiki'


def main():
    if len(sys.argv) >= 2:
        print "Reinitializing Solr with desired topics CSV"
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

    i = 1
    for wiki in get_wikis_with_topics():
        wiki_id = wiki['id']
        print wiki_id
        try:
            wiki_doc, recommendations = as_euclidean(wiki_id)
        except ValueError as e:
            print e
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

    my_workbook.save(sys.argv[1].split('.')[0]+"-wiki-recommendations.xls")


if __name__ == '__main__':
    main()