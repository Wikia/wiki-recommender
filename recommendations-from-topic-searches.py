import sys
import requests
import xlwt
from datetime import datetime
from multiprocessing import Pool
from lib.wikis import wiki_data_for_ids


def cw_search(term):
    return requests.get(u'http://www.wikia.com/api/v1/Search/CrossWiki',
                        params=dict(expand=1, lang='en', query=term, limit=20)).json().get(u'items', [])


def main():
    p = Pool(processes=16)
    with open(sys.argv[1], u'r') as fl:
        lines_split = [ln.strip().split(u',') for ln in fl.readlines() if len(ln.strip().split(u',')) >= 3]
        terms = [tp[2] for tp in lines_split]
        terms_and_recs = zip(lines_split, p.map_async(cw_search, terms).get())

        ids = [tp[0] for tp in lines_split]
        wiki_data = {}
        r = p.map_async(wiki_data_for_ids, [ids[i:i+20] for i in range(0, len(ids), 20)])
        map(wiki_data.update, r.get())
        map(wiki_data.update, [{r[u'id']: r} for terms, recs in terms_and_recs for r in recs if type(r) == dict])

        my_workbook = xlwt.Workbook()
        ids_worksheet = my_workbook.add_sheet(u"Wiki IDs")
        ids_worksheet.write(0, 0, u'Wiki')
        ids_worksheet.write(0, 1, u'Recommendations')

        urls_worksheet = my_workbook.add_sheet(u"Wiki URLs")
        urls_worksheet.write(0, 0, u'Wiki')
        urls_worksheet.write(0, 1, u'Recommendations')

        names_worksheet = my_workbook.add_sheet(u"Wiki Names")
        names_worksheet.write(0, 0, u'Wiki')
        names_worksheet.write(0, 1, u'Recommendations')

        topics_worksheet = my_workbook.add_sheet(u"Wiki Topics")
        topics_worksheet.write(0, 0, u'Wiki ID')
        topics_worksheet.write(0, 1, u'Wiki Name')
        topics_worksheet.write(0, 2, u'Topic')

        row = 0
        tr_sorted = sorted(filter(lambda y: wiki_data.get(y[0][0]), terms_and_recs),
                           key=lambda x: wiki_data.get(x[0][0], {}).get(u'wam_score', 0) if x else 0,
                           reverse=True)
        for this_wiki, recommendations in tr_sorted:
            try:
                line = [this_wiki[0]] + [r[u'id'] for r in recommendations if r[u'id'] != this_wiki[0]]
            except TypeError:
                continue
            if len(line) < 2:
                continue
            row += 1
            if len(this_wiki) >= 3:
                topics_worksheet.write(row, 0, this_wiki[0])
                topics_worksheet.write(row, 1, wiki_data.get(this_wiki[0], {}).get(u'headline', this_wiki[1]))
                topics_worksheet.write(row, 2, this_wiki[2])
            for col in range(0, len(line)):
                ids_worksheet.write(row, col, line[col])
                urls_worksheet.write(row, col, wiki_data.get(line[col], {}).get(u'url', '?').encode('utf-8'))
                names_worksheet.write(row, col, wiki_data.get(line[col], {}).get(u'title', '?').encode('utf-8'))

        fname = u'topic-recommendations-%s.xls' % (datetime.strftime(datetime.now(), u'%Y-%m-%d-%H-%M'))
        my_workbook.save(fname)
        print fname

if __name__ == '__main__':
    main()
