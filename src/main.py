"""
Author:
Description: This main function is designed for analyzing the twitter data
"""

# Import mpi so we can run on more than one node and processor
from mpi4py import MPI
from collections import Counter, defaultdict
import sys, time, util, operator


def master_processor(comm, twitter_files_path, sal_file_path):
    # Init parameters
    comm_rank, comm_size = comm.Get_rank(), comm.Get_size()
    util.print_num_process(comm_size)
    question1_counter = Counter()
    question2_counter = Counter()
    question3_dict = defaultdict(Counter)
    unknown = set()
    # Get all tweet
    master_tweet_list = util.get_all_tweet(twitter_files_path[comm_rank])
    # Get the sal list
    master_sal_list = util.get_sal_data_list(sal_file_path)

    # Counte the number of tweet for each author id
    for tweet in master_tweet_list:
        author_id = tweet['author_id']
        place_full_name = tweet['place_full_name']
        # Question 1
        question1_counter[author_id] += 1
        # Question 2
        is_in_gcc, gcc_code = util.is_in_gcc(place_full_name, master_sal_list)
        if is_in_gcc:
            question2_counter[gcc_code] += 1
            question3_dict[author_id][gcc_code] += 1
        else:
            unknown.add(place_full_name)
        # Question 3

    # Merge the author counter from all the workers
    reduced_question1_counter = comm.reduce(question1_counter, op=operator.add, root=comm_rank)
    reduced_question2_counter = comm.reduce(question2_counter, op=operator.add, root=comm_rank)
    reduced_question3_dict = comm.reduce(question3_dict, op=MPI.Op.Create(util.add_default_dict, commute=True), root=comm_rank)
    # reduced_unknown = comm.reduce(unknown, op=operator.add, root=0)
    util.solve_first_question(reduced_question1_counter)
    util.solve_second_question(reduced_question2_counter)
    util.solve_third_question(reduced_question3_dict)

def worker_processor(comm, twitter_files_path, sal_file_path):
    # Init parameters
    comm_rank, comm_size = comm.Get_rank(), comm.Get_size()
    master_node = 0
    question1_counter = Counter()
    question2_counter = Counter()
    question3_dict = defaultdict(Counter)
    unknown = set()
    # Get all tweet
    worker_tweet_list = util.get_all_tweet(twitter_files_path[comm_rank])
    # Get the sal data
    worker_sal_list = util.get_sal_data_list(sal_file_path)
    
    # Count number of tweet for each author
    for tweet in worker_tweet_list:
        author_id = tweet['author_id']
        place_full_name = tweet['place_full_name']
        # Question 1
        question1_counter[author_id] += 1
        # Question 2
        is_in_gcc, gcc_code = util.is_in_gcc(place_full_name, worker_sal_list)
        if is_in_gcc:
            question2_counter[gcc_code] += 1
            question3_dict[author_id][gcc_code] += 1
        else:
            unknown.add(place_full_name)
    # Merge the result back to the master node to solve question 1
    comm.reduce(question1_counter, op=operator.add, root=master_node)
    comm.reduce(question2_counter, op=operator.add, root=master_node)
    comm.reduce(question3_dict, op=MPI.Op.Create(util.add_default_dict, commute=True), root=master_node)

def main(twitter_files_path, sal_file_path):
    """
    """
    # Start time
    program_start_time = time.time()
    # Create communicator
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    if rank == 0:
        master_processor(comm, twitter_files_path, sal_file_path)
        # Elapsed time
        util.print_elapsed_time(time.time(), program_start_time)
        # Terminates MPI execution env and exit the program
        MPI.Finalize()
    else:
        worker_processor(comm, twitter_files_path, sal_file_path)

if __name__ == "__main__":
    # run program
    twitter_files_path = sys.argv[1].split('|')
    sal_file_path = sys.argv[2]
    main(twitter_files_path, sal_file_path)