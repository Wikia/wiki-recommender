import re


def get_topics_sorted(doc):
    return sorted([(key, doc[key]) for key in doc.keys()
                   if re.match(r'topic_\d+_tf', key) is not None and doc[key] > 0],
                  reverse=True,
                  key=lambda x: x[1])


def get_topics_sorted_keys(doc):
    return sorted([key for key in doc.keys() if re.match(r'topic_\d+_tf', key) is not None and doc[key] > 0],
                  reverse=True,
                  key=lambda x: x[1])


def intersection_count(tuples1, tuples2):
    return len([x for x in tuples1 if x[0] in [y[0] for y in tuples2]])


def strip_file(doc_title):
    return doc_title.replace('File:', '')


def append(lst, val):
    return lst + [val]