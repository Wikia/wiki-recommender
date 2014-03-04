import sys
import requests
import xlwt
import datetime
from multiprocessing import Pool
from lib.wikis import wiki_data_for_ids


def cw_search(term):
    return requests.get('http://www.wikia.com/api/v1/Search/CrossWiki',
                        params=dict(expand=1, lang='en', query=term, limit=20)).json().get('items', [])


def main():
    p = Pool(processes=8)
    with open(sys.argv[1], 'r') as fl:
        lines_split = [ln.strip().split(',') for ln in fl.readlines() if len(ln.strip().split(',')) >= 3]
        terms = [tp[2] for tp in lines_split]
        terms_and_recs = zip(lines_split, p.map_async(cw_search, terms).get())

        ids = [tp[0] for tp in lines_split]
        wiki_data = {}
        r = p.map_async(wiki_data_for_ids, [ids[i:i+20] for i in range(0, len(ids), 20)])
        map(wiki_data.update, r.get())
        map(wiki_data.update, [r for terms, recs in terms_and_recs for r in recs])

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

        for counter, grouping in enumerate(terms_and_recs):
            this_wiki, recommendations = grouping
            line = [this_wiki[0]] + [r['id'] for r in recommendations]
            row = counter + 1
            for col in range(0, len(recommendations)):
                ids_worksheet.write(row, col, line[col])
                urls_worksheet.write(row, col, wiki_data.get(line[col], {}).get('url', '?'))
                names_worksheet.write(row, col, "%s" % (wiki_data.get(line[col], {}).get('title', '?')))

        fname = 'topic-recommendations-%s.xls' % (datetime.strftime(datetime.now(), '%Y-%m-%d-%H-%M'))
        my_workbook.save(fname)
        print fname

if __name__ == '__main__':
    main()
