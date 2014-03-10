import phpserialize
import argparse
import xlwt
from lib.wikis import wiki_data_for_ids
from multiprocessing import Pool
from collections import defaultdict


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output-format', dest='output_format', default='wf')
    parser.add_argument('--topics-file', dest='topics_file', type=argparse.FileType('r'))
    parser.add_argument('--features-file', dest='features_file', type=argparse.FileType('r'))
    return parser.parse_args()


def main():
    args = get_args()
    wid_to_topics = defaultdict(dict)
    topics_to_wids = defaultdict(list)
    for line in args.topics_file:
        ploded = line[:-1].split(',')
        wid = ploded[0]
        for grouping in ploded[1:]:
            topic, value = grouping.split('-')
            wid_to_topics[wid][topic] = value
            topics_to_wids[topic].append(wid)
    if args.output_format == 'adops':
        to_adops_xls(args, wid_to_topics)
    else:
        to_wf(args, wid_to_topics)


def to_wf(args, wid_to_topics):
    with open(args.topics_file.name.replace('.csv', '-wf-vars.csv'), 'w') as newfl:
        newlines = []
        for wid in wid_to_topics:
            top_topics = sorted(wid_to_topics[wid].keys(), key=lambda x: wid_to_topics[wid][x], reverse=True)[:5]
            newlines.append("%s,1344,%s" % (wid, phpserialize.dumps(top_topics)))
        newfl.write("\n".join(newlines))


def to_adops_xls(args, wid_to_topics):
    my_workbook = xlwt.Workbook()
    ids_worksheet = my_workbook.add_sheet("Wikis to Topics")
    ids_worksheet.write(0, 0, 'Wiki')
    ids_worksheet.write(0, 1, 'URL')
    ids_worksheet.write(0, 2, 'Topic')
    ids_worksheet.write(0, 3, 'Rank')

    ids = wid_to_topics.keys()
    r = Pool(processes=16).map_async(wiki_data_for_ids, [ids[i:i+20] for i in range(0, len(ids), 20)])
    wiki_data = {}
    map(wiki_data.update, r.get())

    row = 1
    for wid, topics in wid_to_topics.items():
        top_five = sorted(wid_to_topics[wid].keys(), key=lambda x: wid_to_topics[wid][x], reverse=True)[:5]
        for counter, topic in enumerate(top_five):
            ids_worksheet.write(row, 0, wid)
            ids_worksheet.write(row, 1, wiki_data.get(wid, {}).get('url', '?'))
            ids_worksheet.write(row, 2, int(topic)+1)
            ids_worksheet.write(row, 3, counter)
            row += 1

    urls_worksheet = my_workbook.add_sheet("Topic Data")
    urls_worksheet.write(0, 0, 'Topic')
    urls_worksheet.write(0, 1, 'Phrase')
    urls_worksheet.write(0, 2, 'Weight')
    urls_worksheet.write(0, 3, 'Rank')

    row = 1
    for topic, line in enumerate(args.features_file):
        words = line.decode('utf8').split(u' + ')
        for rank, word_data in enumerate(words):
            weight, word = word_data.split('*')
            urls_worksheet.write(row, 0, topic+1)
            urls_worksheet.write(row, 1, word)
            urls_worksheet.write(row, 2, weight)
            urls_worksheet.write(row, 3, rank+1)
            row += 1

    my_workbook.save(args.topics_file.name.replace('.csv', '-adops-report.xls'))
    print args.topics_file.name.replace('.csv', '-adops-report.xls')


if __name__ == '__main__':
    main()