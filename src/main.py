"""
Author:
Description: This main function is designed for analyzing the twitter data
"""

# Import mpi so we can run on more than one node and processor
from mpi4py import MPI
from collections import defaultdict, Counter
import sys, time, util, operator


def master_processor(comm, twitter_file_path):
    # Init parameters
    comm_rank, comm_size = comm.Get_rank(), comm.Get_size()
    author_tweet_counter = Counter()

    # Print num of processors
    util.print_num_process(comm_size)
    # Get start, end position
    start_position, end_position = util.get_start_end_position(twitter_file_path, comm_rank, comm_size)
    # Get line generator
    line_generator = util.get_line_genereator(twitter_file_path, start_position, end_position)
    # Loop throught lines
    for line in line_generator:
        if util.have_author_id(line):
            # If this line contains author id, we store them into the author_tweet_counter
            author_tweet_counter[int(util.get_author_id(line))] += 1
    # Reduce to root and merge the dictionary. Set commute to true: dict1 + dict2 = dict2 + dict1
    reduced_author_counter = comm.reduce(author_tweet_counter, root=0, op=operator.add)
    # Check if we got the value from other processors
    print(reduced_author_counter.items())

def slave_processor(comm, twitter_file_path):
    # Init parameters
    comm_rank, comm_size = comm.Get_rank(), comm.Get_size()
    author_tweet_counter = Counter()

    # Get start, end position
    start_position, end_position = util.get_start_end_position(twitter_file_path, comm_rank, comm_size)
    # Get line generator
    line_generator = util.get_line_genereator(twitter_file_path, start_position, end_position)
    # Loop throught lines
    for line in line_generator:
        if util.have_author_id(line):
            # If this line contains author id, we store them into the author_tweet_counter
            author_tweet_counter[int(util.get_author_id(line))] += 1
    # Reduce to root and merge the dictionary. Set commute to true: dict1 + dict2 = dict2 + dict1
    reduced_author_counter = comm.reduce(author_tweet_counter, root=0, op=operator.add)

def main(twitter_file_path):
    """
    """
    # Start time
    program_start_time = time.time()
    # Create communicator
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    if rank == 0:
        master_processor(comm, twitter_file_path)
        # Elapsed time
        util.print_elapsed_time(time.time(), program_start_time)
        # Terminates MPI execution env and exit the program
        MPI.Finalize()
    else:
        slave_processor(comm, twitter_file_path)

if __name__ == "__main__":
    # run program
    twitter_file_path = sys.argv[1]
    main(twitter_file_path)