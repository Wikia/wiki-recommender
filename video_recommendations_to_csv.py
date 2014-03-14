from lib.querying import QueryIterator, as_euclidean
from argparse import ArgumentParser
from multiprocessing import Pool


def get_args():
    ap = ArgumentParser()
    ap.add_argument('--server', dest='server', default='dev-search')
    ap.add_argument('--num-processes', dest='num_processes', default=8, type=int)
    ap.add_argument('--batch-size', dest='batchsize', default=100, type=int)
    return ap.parse_args()


def my_as_euclidean(docid):
    return as_euclidean(docid, core='main', requested_fields='id')


def write_recommendations(fl, pool, docs):
    print pool.map(my_as_euclidean, [doc['id'] for doc in docs if doc and doc.get('id', '').startswith('298117')])
    [fl.write("%s,%s\n" % (doc['id'], ",".join([d['id'] for d in recs])))
    for doc, recs in pool.map(my_as_euclidean, [doc['id'] for doc in docs if doc and doc.get('id', '').startswith('298117')]) if doc and recs]


def main():
    args = get_args()
    qi = QueryIterator(query='has_topics_b:true', server=args.server, core='main')
    p = Pool(processes=args.num_processes)
    total = 0
    with open('video_recommendations.csv', 'w') as fl:
        docs = []
        try:
            while True:
                print total
                for i in range(0, args.batchsize):
                    docs.append(qi.next())
                    write_recommendations(fl, p, docs)
                    docs = []
                total += args.batchsize

                
                 
        except StopIteration:
            write_recommendations(fl, p, docs)


if __name__ == '__main__':
    main()
