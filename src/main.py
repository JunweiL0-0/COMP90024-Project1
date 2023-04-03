"""
Author:
Description: This main function is designed for analyzing the twitter data
"""

# Import mpi so we can run on more than one node and processor
from mpi4py import MPI
from collections import Counter
import sys, time, util, operator, json


def master_processor(comm, twitter_file_path, sal_file_path):
    # Init parameters
    comm_rank, comm_size = comm.Get_rank(), comm.Get_size()
    util.print_num_process(comm_size)
    author_counter = Counter()
    number_of_tweet_counter = Counter()
    master_tweet_list = util.get_all_tweet(twitter_file_path)
    print("Total length", len(master_tweet_list))

    # Distribute the work to all the workers, if we have extra worker
    if comm_size > 1:
        util.distribute_work_to_worker(comm, master_tweet_list)

    # Reduce the size of the list to save some memory and get ready for analyze
    master_tweet_list = master_tweet_list[:len(master_tweet_list)//comm_size]

    # Counte the number of tweet for each author id
    for tweet in master_tweet_list:
        author_counter[tweet.author_id] += 1
    # Merge the author counter from all the workers
    reduced_author_counter = comm.reduce(author_counter, op=operator.add, root=comm_rank)
    util.solve_first_question(reduced_author_counter)

    SAL_DATA = comm.bcast(util.get_sal_data_list(sal_file_path), root=comm_rank)

    for tweet in master_tweet_list:
        if tweet.place in SAL_DATA.keys():
            number_of_tweet_counter[SAL_DATA[tweet.place]] += 1
    print(number_of_tweet_counter)

    print("Master length", len(master_tweet_list))


def worker_processor(comm):
    # Init parameters
    comm_rank, comm_size = comm.Get_rank(), comm.Get_size()
    master_node = 0
    author_counter = Counter()
    number_of_tweet_counter = Counter()
    # Receiving the data, blocking
    worker_tweet_list = comm.recv(source=master_node)

    for tweet in worker_tweet_list:
        author_counter[tweet.author_id] += 1
    # Merge the result back to the master node
    comm.reduce(author_counter, op=operator.add, root=master_node)
    SAL_DATA = comm.bcast(None, root=master_node)

    for tweet in worker_tweet_list:
        # print(SAL_DATA.keys())
        if tweet.place in SAL_DATA.keys():
            number_of_tweet_counter[SAL_DATA[tweet.place]] += 1
    # print(number_of_tweet_counter.items())
         

    print("Worker length", len(worker_tweet_list))


def main(twitter_file_path, sal_file_path):
    """
    """
    # Start time
    program_start_time = time.time()
    # Create communicator
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    if rank == 0:
        master_processor(comm, twitter_file_path, sal_file_path)
        # Elapsed time
        util.print_elapsed_time(time.time(), program_start_time)
        # Terminates MPI execution env and exit the program
        MPI.Finalize()
    else:
        worker_processor(comm)

if __name__ == "__main__":
    # run program
    twitter_file_path = sys.argv[1]
    sal_file_path = sys.argv[2]
    main(twitter_file_path, sal_file_path)