import sys
import itertools
import xlwt
from lib.wikis import wiki_data_for_ids
from collections import defaultdict
from scipy.spatial.distance import cosine
from multiprocessing import Pool
from datetime import datetime


# man i gotta figure this out
def tup_cos(tup):
    a, b = tup
    return cosine(a, b)


def get_recommendations(docid_to_topics):
    docid_distances = defaultdict(list)

    keys = docid_to_topics.keys()

    print "Getting all pairwise relations"
    relations = list(set(["%s_%s" % (sorted(k)[0], sorted(k)[1])
                          for k in itertools.product(keys, keys) if k[0] != k[1]]))

    print "Building param sets from relations"
    params = [(docid_to_topics[x.split('_')[0]], docid_to_topics[x.split('_')[1]]) for x in relations]

    print "Computing relations"
    computed = Pool(processes=4).map(cosine, params)
    distances = zip(relations, computed)

    print "Storing distances"
    for relation, distance in distances:
        a, b = relation.split("_")
        docid_distances[a].append((b, distance))
        docid_distances[b].append((a, distance))

    for doc in distances:
        docid_distances[doc] = sorted(docid_distances[doc], key=lambda x: x[1])

    return docid_distances


def main():
    print "Scraping CSV"
    docid_to_topics = dict()
    with open(sys.argv[1]) as fl:
        for line in fl:
            cols = line.strip().split(',')
            docid = cols[0]
            docid_to_topics[docid] = [0] * 999  # initialize
            for col in cols[1:]:
                topic, val = col.split('-')
                docid_to_topics[docid][int(topic)] = float(val)

    recommendations = get_recommendations(docid_to_topics)
    ids = recommendations.keys()
    wiki_data = {}
    r = Pool(processes=4).map_async(wiki_data_for_ids, [ids[i:i+20] for i in range(0, len(ids), 20)])
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

    docids = sorted(recommendations.keys(), key=lambda y: wiki_data.get(y[0], {}).get('wam_score', 0), reverse=True)
    for counter, docid in enumerate(docids):
        row = counter + 1
        line = recommendations[docid]
        for col in range(0, len(line)):
            ids_worksheet.write(row, col, line[col])
            urls_worksheet.write(row, col, wiki_data.get(line[col], {}).get('url', '?'))
            names_worksheet.write(row, col, wiki_data.get(line[col], {}).get('title', '?'))

    fname = 'cosine-recommendations-%s.xls' % (datetime.strftime(datetime.now(), '%Y-%m-%d-%H-%M'))
    my_workbook.save(fname)
    print fname



if __name__ == '__main__':
    main()