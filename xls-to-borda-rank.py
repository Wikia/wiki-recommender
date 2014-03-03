import sys
import requests
import xlrd
import xlwt
import os
from lib.wikis import wiki_data_for_ids
from datetime import datetime
from collections import defaultdict
from multiprocessing import Pool


def borda(list_of_lists):
    dct = defaultdict(list)
    for li in list_of_lists:
        for rank, val in list(enumerate(li)):
            if rank > 10:
                break
            dct[val].append(rank)

    lowest_rank = max([rank for val, ranks in dct.items() for rank in ranks]) + 1

    # 1000000 is an arbtrarily big number for instances that aren't included in borda
    normalized = [(val, sum(ranks + [lowest_rank] * (len(list_of_lists) * len(ranks)))) for val, ranks in dct.items()]

    return sorted(normalized, key=lambda x: x[1])


def dict_from_args():
    doc_to_recs = defaultdict(list)

    def do_fname(filename):
        book = xlrd.open_workbook(filename)
        sheet = book.sheets()[0]
        for i in range(1, sheet.nrows):
            row_vals = sheet.row_values(i)
            doc_to_recs[row_vals[0]].append(row_vals[1:])

    for arg in sys.argv[1:]:
        if os.path.isfile(arg):
            do_fname(arg)
        elif os.path.isdir(arg):
            map(do_fname, [arg+'/'+fl for fl in os.listdir(arg)])

    return doc_to_recs


def main():
    print "Combining recommendations"
    borda_recs = []
    borda_vals = dict()
    for doc_id, recs in dict_from_args().items():
        individual_borda_recs = borda(recs)
        borda_vals[doc_id] = dict(individual_borda_recs)
        borda_recs.append([doc_id] + [x[0] for x in individual_borda_recs])

    print "Getting wiki data"
    pool = Pool(processes=4)
    ids = list(set([col for row in borda_recs for col in row]))
    wiki_data = {}
    r = pool.map_async(wiki_data_for_ids, [ids[i:i+20] for i in range(0, len(ids), 20)])
    map(wiki_data.update, r.get())

    print "Writing Data"
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

    seeds_worksheet = my_workbook.add_sheet("Individual Recommendation Files")
    for counter, arg in enumerate(sys.argv[1:]):
        seeds_worksheet.write(counter, 0, arg)

    borda_recs = sorted(borda_recs, key=lambda y: wiki_data.get(y[0], {}).get('wam_score', 0), reverse=True)
    for counter, line in enumerate(borda_recs):
        row = counter + 1
        for col in range(0, len(line)):
            ids_worksheet.write(row, col, line[col])
            urls_worksheet.write(row, col, wiki_data.get(line[col], {}).get('url', '?'))
            names_worksheet.write(row, col, "%s (%s)" % (wiki_data.get(line[col], {}).get('title', '?'),
                                                         str(borda_vals[line[0]].get(line[col], '?'))))

    fname = 'combined-recommendations-%s.xls' % (datetime.strftime(datetime.now(), '%Y-%m-%d-%H-%M'))
    my_workbook.save(fname)
    print fname


if __name__ == '__main__':
    main()