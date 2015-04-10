__author__ = 'nikki'
import csv
import re
from collections import Counter
from json import loads

filename1 = 'miniTwitter.csv'
csv_delimiter = ','

debug = False
def open_with_python_csv_list(filename):
    def yield_fn():
        with open(filename, 'rb') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=csv_delimiter, quotechar='\"')
            next(csv_reader)
            for row in csv_reader:
                yield row[4]
    return yield_fn

def search_term(source_fn, phrase):
    hash_tags_global = Counter()
    mentions_global = Counter()
    count = 0

    for row in source_fn():
        text = loads(row, encoding="utf-8")["text"]
        count_object = find_whole_word(phrase)(text)
        hash_tags = [word.lower() for word in text.split() if word.startswith('#') and len(word) >1]
        mentions = [ re.sub('[!#@.,:$]', '', word).lower() for word in text.split() if word.startswith('@') and len(word) >1]

        if count_object is not None:
            count += 1
        if len(hash_tags)>0:
            counts = Counter(hash_tags)
            hash_tags_global.update(counts)
        if len(mentions)>0:
            count_mentions = Counter(mentions)
            mentions_global.update(count_mentions)

    return {
        "count": count,
        "mentions": dict(mentions_global),
        "hashtags": dict(hash_tags_global)
    }

def find_whole_word(w):
    return re.compile(r'\b({0})\b'.format(w), flags=re.IGNORECASE).search

def merge_search(*args):
    total_count = 0
    total_mentions = Counter()
    total_hashtags = Counter()
    for args in args:
        total_count += args["count"]
        total_mentions.update(args["mentions"])
        total_hashtags.update(args["hashtags"])

    print total_count
    print total_mentions.most_common(10)
    print total_hashtags.most_common(10)

def main():
    ##out = search_term(open_with_python_csv_list(filename1), "how")
    out1 = search_term(open_with_python_csv_list("twitter-1.csv"), "how")
    out2 = search_term(open_with_python_csv_list("twitter-2.csv"), "how")
    merge_search(out1, out2)
    ##print out1
    ##print out2

if __name__ == "__main__":
    main()






