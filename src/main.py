"""
Author:
Description: This main function is designed for analyzing the twitter data
"""
import time


from mpi4py import MPI
import util as util


def main():
    """
    """
    # Start time
    program_start_time = time.time()
    # Create communication
    comm = MPI.COMM_WORLD
    comm_rank = comm.Get_rank()
    comm_size = comm.Get_size()
    # Master processor
    if comm_rank == 0:
        # Print num of processors
        util.print_num_processor(comm_size)


    # Master processor output result
    if comm_rank == 0:
        # Calculate and output the total running time
        elapsed = time.time() - program_start_time
        print(f"Porgrams runs {elapsed:.10f} seconds.")
        # Terminates MPI execution env and exit the program
        MPI.Finalize()
        return 0

if __name__ == "__main__":
    # run program
    main()
