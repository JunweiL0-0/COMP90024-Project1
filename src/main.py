"""
Author:
Description: This main function is designed for analyzing the twitter data
"""
import time


from mpi4py import MPI
import util as util


def main(twitter_file_path):
    """
    """
    # Start time
    program_start_time = time.time()
    # Create communicator
    comm = MPI.COMM_WORLD
    comm_rank = comm.Get_rank()
    comm_size = comm.Get_size()

    # Master process
    if comm_rank == 0:
        # Print num of processors
        util.print_num_process(comm_size)
    # Get num of tweet
    NUM_OF_TWEET = comm.bcast(util.get_num_of_tweet(twitter_file_path), root=0)
    # Output running time on each process
    elapsed = time.time() - program_start_time
    print(f"Porgrams runs {elapsed:.10f} seconds on process {comm_rank}.")

    # Master process output result
    if comm_rank == 0:
        # Terminates MPI execution env and exit the program
        MPI.Finalize()
        return 0

if __name__ == "__main__":
    # run program
    main("data/twitter-data-small.json")