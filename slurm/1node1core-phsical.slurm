#!/bin/bash

#SBATCH --ntasks=1
#SBATCH --output=1node1core-physical.txt
#SBATCH --mail-user=junwliang1@student.unimelb.edu.au
#SBATCH --mail-type=ALL
# Wall time, hours:mins:seconds
#SBATCH --time=0:1:0

module purge
module load foss/2019b
module load python/3.7.4
module load mpi4py/3.0.2-timed-pingpong
# Install ijson
mkdir ~/virtualenv
virtualenv ~/virtualenv/python3.7.4
source ~/virtualenv/python3.7.4/bin/activate
pipe install ijson
deactivate

# Run program
srun -n 1 python3 "src/main.py"

# Job monitor command
my-job-stats -a -n -s