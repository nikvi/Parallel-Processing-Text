__author__ = 'nikki'
import os
from mpi4py import MPI
import csv
import re
import numpy as np
import itertools
from collections import Counter
from json import loads
import pprint

csv_delimiter = ','
WORKTAG = 1
DIETAG  = 0

#single process search
def single_process_search(file_name,search_phrase):
     return search_term(open_with_python_csv_list(file_name), search_phrase)

def open_with_python_csv_list(filename):
    def yield_fn():
        with open(filename, 'rb') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=csv_delimiter, quotechar='\"')
            next(csv_reader)
            for row in csv_reader:
                yield row[4]
    return yield_fn

# similar function to get data from exported chubnks:
def parse_chunk(data_chunk):
    def yield_fn():
        for i,val in enumerate(data_chunk):
            if(val !=0):
                yield val[4]
    return yield_fn

#source function gets dta asrows
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
        if(args != {}):
            total_count += args["count"]
            total_mentions.update(args["mentions"])
            total_hashtags.update(args["hashtags"])
    return {
        "count": total_count,
        "mentions": dict(total_mentions),
        "hashtags": dict(total_hashtags)
    }

def read_in_chunks(file_object, chunk_size=1024):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data

def process_data(data_chunk,phrase):
   out1 = search_term(parse_chunk(data_chunk), phrase)
   return out1

def master_process(file_name):
     summary = {}
     comm = MPI.COMM_WORLD
     size = comm.size
     chunk_size = 10 #file_size/size
     reader = csv.reader(open(file_name, 'rb'), delimiter=csv_delimiter, quotechar='\"')
     next(reader)
     count = 1
     chunks_sent = 1
     for chunk in gen_chunks(reader,chunk_size):
         print 'sending chunk to', str(count), len(chunk)
         comm.send(chunk, dest=count, tag=WORKTAG)
         count += 1
         chunks_sent += 1
         if count >= size:
             count = 1
     
     print 'expects ', str(chunks_sent)
     for i in range(1, chunks_sent):
        data = comm.recv(obj=None, source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG)
        summary = merge_search(summary,data)

     for i in range(1, size):
        comm.send(obj=None, dest=i, tag=DIETAG)
     return summary

def slave_process(search_phrase):
    comm = MPI.COMM_WORLD
    status = MPI.Status()
    while True:
        data = comm.recv(obj=None, source=0, tag=MPI.ANY_TAG, status=status)
        if status.Get_tag() == DIETAG: break
        comm.send(obj=process_data(data,search_phrase), dest=0)

def write_data(data, time, outfile):
  with open(outfile, 'w') as out:
    out.write("Count:" + str(data["count"]) + os.linesep)
    out.write("Mentions" + os.linesep)
    pprint.pprint(Counter(data['mentions']).most_common(10), out, indent=4)
    out.write("Hashtags" + os.linesep)
    pprint.pprint(Counter(data['hashtags']).most_common(10), out, indent=4)
    out.write(os.linesep + str(time))

#print provided data
def print_data(out_put):
    count_t = out_put["count"]
    mentions_t = Counter(out_put["mentions"]).most_common(10)
    hashtags_t = Counter(out_put["hashtags"]).most_common(10)
    print count_t
    print mentions_t
    print hashtags_t

#how to chunk data and send
def search(file_name, search_phrase, outfile):
   print file_name, search_phrase, outfile
   comm = MPI.COMM_WORLD
   size= comm.size
   rank=comm.rank
   #file_name = "miniTwitter.csv"
   #search_phrase ="how"
   comm.Barrier()
   t_start = MPI.Wtime()
   if size == 1:
        print "Hello! I'm rank %d from %d running in total..." % (rank,size)
        output = single_process_search(file_name,search_phrase)
        write_data(output, MPI.Wtime() - t_start, outfile)
   else:
        if(rank==0):
            print "Hello! I'm rank %d from %d running in total..." % (rank,size)
            #file_size = os.stat(file_name).st_size
            output = master_process(file_name)
            write_data(output, MPI.Wtime() - t_start, outfile)
            #print MPI.Wtime() - t_start
        else:
            slave_process(search_phrase)

   comm.Barrier()
   #t_diff = MPI.Wtime()-t_start
   #print t_diff

#use numpy


def gen_chunks(reader, chunk_size=100):
        chunk = np.zeros((chunk_size,), dtype=object)
        for i, line in enumerate(reader):
            k = i%chunk_size
            if (k == 0 and i > 0):
                yield chunk
                chunk = np.zeros((chunk_size,), dtype=object)
            chunk[k] = line
        yield chunk


def sample_chunk():
    reader = csv.reader(open('miniTwitter.csv', 'rb'), delimiter=csv_delimiter, quotechar='\"')
    next(reader)
    summary = {}
    for chunk in gen_chunks(reader):
        data = process_data(chunk,"how")
        summary = merge_search(summary,data)
    print_data(summary)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Run search on tweets')
    parser.add_argument('filename', metavar='F', type=str, nargs=1, help='file containing tweets')
    parser.add_argument('searchterm', metavar='ST', type=str, nargs=1, help='term to search')
    parser.add_argument('outfile', metavar='OF', type=str, nargs=1, help='file to output results')
    args = parser.parse_args()
    #print args
    search(args.filename[0], args.searchterm[0], args.outfile[0])
    #main()
    #sample_chunk()
