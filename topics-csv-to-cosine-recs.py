import xlwt
import numpy as np
from lib.wikis import wiki_data_for_ids
from collections import defaultdict
from scipy.spatial.distance import cdist
from multiprocessing import Pool
from datetime import datetime
from argparse import ArgumentParser, FileType

# man i gotta figure this out
def tup_dist(tup):
    func, a, b = tup
    return func(a, b)


def get_args():
    ap = ArgumentParser()
    ap.add_argument('--infile', dest="infile", type=FileType('r'))
    ap.add_argument('--metric', dest="metric", default="cosine")
    ap.add_argument('--output-format', dest="format", default="csv")
    return ap.parse_args()


def get_recommendations(args, docid_to_topics):
    docid_distances = defaultdict(dict)

    keys = docid_to_topics.keys()
    values = np.array([d.values() for d in docid_to_topics.values()])

    func = {'cosine': cosine, 'mahalanobis': mahalanobis, 'euclidean': euclidean}.get(args.metric, cosine)

    print cdist(values)

    return docid_distances


def to_xls(args, recommendations):
    ids = recommendations.keys()
    print "Getting Wiki Data"
    wiki_data = {}
    r = Pool(processes=8).map_async(wiki_data_for_ids, [ids[i:i+20] for i in range(0, len(ids), 20)])
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

    docids = sorted(recommendations.keys(), key=lambda y: wiki_data.get(y, {}).get('wam_score', 0), reverse=True)
    for counter, docid in enumerate(docids):
        row = counter + 1
        line = [docid] + [z[0] for z in recommendations[docid][:25]]
        for col in range(0, len(line)):
            ids_worksheet.write(row, col, str(line[col]))
            urls_worksheet.write(row, col, wiki_data.get(line[col], {}).get('url', '?'))
            names_worksheet.write(row, col, wiki_data.get(line[col], {}).get('title', '?'))

    fname = '%s-recommendations-%s.xls' % (args.func, datetime.strftime(datetime.now(), '%Y-%m-%d-%H-%M'))
    my_workbook.save(fname)
    print fname


def to_csv(args, recommendations):
    fname = '%s-recommendations-%s.csv' % (args.func, datetime.strftime(datetime.now(), '%Y-%m-%d-%H-%M'))
    with open(fname, 'w') as fl:
        for doc in recommendations:
            fl.write("%s,%s" % (doc, ",".join(recommendations[doc])))
    print fname


def main():
    args = get_args()
    print "Scraping CSV"
    docid_to_topics = dict()

    for line in args.infile:
        cols = line.strip().split(',')
        docid = cols[0]
        docid_to_topics[docid] = [0] * 999  # initialize
        for col in cols[1:]:
            topic, val = col.split('-')
            docid_to_topics[docid][int(topic)] = float(val)

    recommendations = get_recommendations(args, docid_to_topics)

    if args.format == 'xls':
        to_xls(args, recommendations)
    else:
        to_csv(args, recommendations)


if __name__ == '__main__':
    main()
