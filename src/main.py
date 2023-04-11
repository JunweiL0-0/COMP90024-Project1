"""
Author: Junwei Liang, Kai xu
"""

# Import libraries
from mpi4py import MPI
from collections import Counter, defaultdict
import sys, time, util, operator


def master_processor(comm, twitter_file_path, sal_file_path):
    """
    :param comm: the communicator
    :twitter_file_path: a string represent the path to the twitter joson file
    :sal_file_path: a string represent the path to the sal json file

    This function represent the master process.
    """
    # Init parameters
    comm_rank, comm_size = comm.Get_rank(), comm.Get_size()
    question1_counter, question2_counter = Counter(), Counter()
    question3_dict = defaultdict(Counter)
    unknown = set()

    # Print number of processors
    util.print_num_process(comm_size)
    # Get lines to read. (How many lines we want to read)
    # Boardcast it to all processors (Each processors will read the same amount of lines)
    LINES_TO_READ = comm.bcast(util.get_lines_to_read(twitter_file_path, comm_size), root=0)
    # Start reading the tweet
    master_tweet_list, end_position = util.get_all_tweet(twitter_file_path, 0, LINES_TO_READ)
    # Send the ending position to next node to resume reading if we got extra processors
    if comm_size > 1:
        comm.send(end_position, dest=comm_rank+1)
    # Get the sal list
    master_sal_list = util.get_sal_data_list(sal_file_path)
    # Start answing questions
    # Counte the number of tweet for each author id
    for tweet in master_tweet_list:
        # Extract the data we are interested in
        author_id = tweet["author_id"]
        place_full_name = tweet['place_full_name']
        # Question 1
        question1_counter[author_id] += 1
        # Question 2 & Question 3
        is_in_gcc, gcc_code = util.is_in_gcc(place_full_name, master_sal_list)
        if is_in_gcc:
            question2_counter[gcc_code] += 1
            question3_dict[author_id][gcc_code] += 1
        else:
            unknown.add(place_full_name)
    # Merge the author counter from all the workers
    reduced_question1_counter = comm.reduce(question1_counter, op=operator.add, root=comm_rank)
    reduced_question2_counter = comm.reduce(question2_counter, op=operator.add, root=comm_rank)
    reduced_question3_dict = comm.reduce(question3_dict, op=MPI.Op.Create(util.add_default_dict, commute=True), root=comm_rank)
    # reduced_unknown = comm.reduce(unknown, op=operator.add, root=0)
    util.solve_first_question(reduced_question1_counter)
    util.solve_second_question(reduced_question2_counter)
    util.solve_third_question(reduced_question3_dict)

def worker_processor(comm, twitter_file_path, sal_file_path):
    """
    :param comm: the communicator
    :param twitter_file_path: a string represent the path to the twitter json file
    :param sal_file_path: a string represent the paht to the sal json file

    This function represent the worker process
    """
    # Init parameters
    comm_rank, comm_size = comm.Get_rank(), comm.Get_size()
    master_node = 0
    question1_counter, question2_counter = Counter(), Counter()
    question3_dict = defaultdict(Counter)
    unknown = set()

    # Get lines to read
    LINES_TO_READ = comm.bcast(None, root=master_node)
    # Wait till you get the starting position
    start_position = comm.recv(source=comm_rank-1)
    # Get all tweet
    worker_tweet_list, end_position = util.get_all_tweet(twitter_file_path, start_position, LINES_TO_READ)
    # Send the position to next node if we are not the very last node
    if comm_rank != comm_size-1:
        comm.send(end_position, dest=comm_rank+1)
    # Get the sal data
    worker_sal_list = util.get_sal_data_list(sal_file_path)
    # Start answing questions
    # Count number of tweet for each author
    for tweet in worker_tweet_list:
        # Extract the data we are interested in
        author_id = tweet["author_id"]
        place_full_name = tweet['place_full_name']
        # Question 1
        question1_counter[author_id] += 1
        # Question 2 & Question 3
        is_in_gcc, gcc_code = util.is_in_gcc(place_full_name, worker_sal_list)
        if is_in_gcc:
            question2_counter[gcc_code] += 1
            question3_dict[author_id][gcc_code] += 1
        else:
            unknown.add(place_full_name)
    # Merge the result back to the master node to solve questions
    comm.reduce(question1_counter, op=operator.add, root=master_node)
    comm.reduce(question2_counter, op=operator.add, root=master_node)
    comm.reduce(question3_dict, op=MPI.Op.Create(util.add_default_dict, commute=True), root=master_node)

def main(twitter_file_path, sal_file_path):
    """
    :param twitter_file_path: a string represent the path to the twitter file
    :param sal_file_path: a string represent the path to the twitter file

    This is the main program
    """
    # Start time
    program_start_time = time.time()
    # Create communicator
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    if rank == 0:
        # Master process
        master_processor(comm, twitter_file_path, sal_file_path)
        # Elapsed time
        util.print_elapsed_time(time.time(), program_start_time)
        # Terminates MPI execution env and exit the program
        MPI.Finalize()
    else:
        # Worker process
        worker_processor(comm, twitter_file_path, sal_file_path)

if __name__ == "__main__":
    # Get file paths
    twitter_file_path = sys.argv[1]
    sal_file_path = sys.argv[2]
    # run program
    main(twitter_file_path, sal_file_path)