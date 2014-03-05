import sys
import phpserialize
from collections import defaultdict


def main():
    with open(sys.argv[1], 'r') as fl:
        wid_to_topics = defaultdict(dict)
        topics_to_wids = defaultdict(list)
        for line in fl:
            ploded = line[:-1].split(',')
            wid = ploded[0]
            for grouping in ploded[1:]:
                topic, value = grouping.split('-')
                wid_to_topics[wid][topic] = value
                topics_to_wids[topic].append(wid)
    with open(sys.argv[1].replace('.csv', '-wf-vars.csv'), 'w') as newfl:
        newlines = []
        for wid in wid_to_topics:
            top_topics = sorted(wid_to_topics[wid].keys(), key=lambda x: wid_to_topics[wid][x], reverse=True)[:5]
            newlines.append("%s,1344,%s" % (wid, phpserialize.dumps(top_topics)))
        newfl.write("\n".join(newlines))



if __name__ == '__main__':
    main()