from lib.querying import QueryIterator, as_euclidean
from argparse import ArgumentParser
from multiprocessing import Pool


def get_args():
    ap = ArgumentParser()
    ap.add_argument('--server', dest='server', default='dev-search')
    ap.add_argument('--num-processes', dest='num_processes', default=8, type=int)
    return ap.parse_args()


def main():
    args = get_args()
    qi = QueryIterator(query='is_video:true AND has_topics_b:true', server=args.server, core='main')
    p = Pool(processes=args.num_processes)
    with open('video_recommendations.csv', 'w') as fl:
        try:
            while True:
                [fl.write("%s,%s" % (doc['id'], ",".join([d['id'] for d in recs])))
                 for doc, recs in p.map_async(as_euclidean, [qi.next() for _ in range(0, 1000)]).get()]
        except StopIteration:
            pass


if __name__ == '__main__':
    main()