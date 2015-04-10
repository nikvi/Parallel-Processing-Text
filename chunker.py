__author__ = 'nikki'
import os
from mpi4py import MPI

csv_delimiter = ','
chunk_size =0;

def read_in_chunks(file_object, chunk_size=1024):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data

def process_data(data_chunk):
    data = data_chunk.split(",")
    for row in data:

           # dt = json.loads(row)
           # print dt

#how do I work with the chunks:
def process_data_function(source_fn):
    for piece in source_fn():
        print piece
        
#how to chunk data and send
def main():
   comm = MPI.COMM_WORLD
   size=comm.get_size()
   rank=comm.get_rank()
   comm.Barrier()
   file_name = "miniTwitter.csv"
   if(rank==0):
        chunk_size = os.stat('file_name').st_size/size
        with open(file_name) as f:
            for piece in read_in_chunks(f):
                process_data(piece)

if __name__ == "__main__":
    main()
