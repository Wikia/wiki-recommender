import argparse
import sys
from lib import wikis, video, csv_to_solr


def get_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('--data-endpoint', dest="data_endpoint", default="http://dev-search:8983/solr/")
    ap.add_argument('--solr-endpoint', dest='solr_endpoint', default='http://search-s10:8983/solr/')
    ap.add_argument('--role', dest='role', default='wiki')
    ap.add_argument('--num-topics', dest='num_topics', default=999)
    ap.add_argument('--csv', dest='csvfile', type=argparse.Filetype('r'))
    ap.add_argument('--with-reset', dest='with_reset', action='store_true', default=False)
    return ap.parse_args()


def reset_callback_from_args(args):
    if not args.with_reset:
        return None
    if args.role is 'wiki':
        return wikis.reinitialize_topics
    if args.role is 'video':
        return video.reset_video_results
    # obvi pages my g


def endpoint_from_args(args):
    if args.role is 'wiki':
        core = 'xwiki'
    else:
        core = 'main'

    return args.data_endpoint+core


def main():
    args = get_args()
    csv_to_solr(args.csvfile, endpoint=endpoint_from_args(args),
                num_topics=args.num_topics, reset_callback=reset_callback_from_args(args))


if __name__ == '__main__':
    main()