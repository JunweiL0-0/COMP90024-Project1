"""
Author:
Description: This main function is designed for analyzing the twitter data
"""

# Import mpi so we can run on more than one node and processor
from mpi4py import MPI
import sys, time, util


def master_processor(comm, twitter_file_path):
    comm_rank = comm.Get_rank()
    comm_size = comm.Get_size()
    # Print num of processors
    util.print_num_process(comm_size)
    # Get num of tweet
    NUM_OF_TWEET = comm.bcast(util.get_num_of_tweet(twitter_file_path), root=0)

def slave_processor(comm, twitter_file_path):
    comm_rank = comm.Get_rank()
    comm_size = comm.Get_size()
    # Get num of tweet
    NUM_OF_TWEET = comm.bcast(util.get_num_of_tweet(twitter_file_path), root=0)

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
        # Output running time on each process
        elapsed = time.time() - program_start_time
        print(f"Porgrams runs {elapsed:.10f}")
        # Terminates MPI execution env and exit the program
        MPI.Finalize()
    else:
        slave_processor(comm, twitter_file_path)



if __name__ == "__main__":
    # run program
    twitter_file_path = sys.argv[1]
    main(twitter_file_path)