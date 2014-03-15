import sys
import itertools
import xlwt
import os
import subprocess
from time import sleep
from lib.wikis import wiki_data_for_ids
from collections import defaultdict
from scipy.spatial.distance import cosine, mahalanobis, euclidean
from multiprocessing import Pool
from datetime import datetime
from argparse import ArgumentParser, FileType

TMP_DIR = '/mnt/doc_relations/'


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


def pairwise(tup):
    global TMP_DIR
    key, keys = tup
    with open(TMP_DIR+key, 'w') as fl:
        fl.write("\n".join(["%s,%s" % (pair[0], pair[1])
                            for pair in [sorted((key, k)) for k in keys] if pair[0] != pair[1]]))


def get_recommendations(args, docid_to_topics):
    global TMP_DIR
    docid_distances = defaultdict(list)

    keys = docid_to_topics.keys()

    pl = Pool(processes=8)

    print "Getting all pairwise relations"
    ln = len(keys) * len(keys)
    print "Product is", ln, "pairs"
    res = pl.map_async(pairwise, [(k, keys) for k in keys])
    while not res.ready():
        print subprocess.check_output('cat %s* | wc -l' % TMP_DIR, shell=True).strip(), "out of", ln
        sleep(15)

    print "Aggregating unique results"

    subprocess.call("cat %s/* | sort | uniq > %s/combined" % (TMP_DIR, TMP_DIR))

    relations = subprocess.check_output('wc -l %s/combined' % TMP_DIR, shell=True)

    print len(relations), "unique relations"

    print "Building param sets from relations"
    func = {'cosine': cosine, 'mahalanobis': mahalanobis, 'euclidean': euclidean}.get(args.metric, cosine)

    with open('%s/combined' % TMP_DIR, 'r') as fl:
        params = [(func, docid_to_topics[x.strip().split(',')[0]],
                   docid_to_topics[x.strip().split(',')[1]]) for x in fl]

    print "Computing relations"
    computed = pl.map(tup_dist, params)
    distances = zip(relations, computed)

    print "Storing distances"

    max_distance = max(distance for relation, distance in distances)
    for relation, distance in filter(lambda y: y[1] != max_distance, distances):
        a, b = relation.split("_")
        docid_distances[a].append((b, distance))
        docid_distances[b].append((a, distance))

    for doc in docid_distances:
        docid_distances[doc] = sorted(docid_distances[doc], key=lambda x: x[1])

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
    global TMP_DIR
    print "Initializing temp directory"
    if os.path.exists(TMP_DIR):
        map(os.remove, os.listdir(TMP_DIR))
    else:
        os.makedirs(TMP_DIR)

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
