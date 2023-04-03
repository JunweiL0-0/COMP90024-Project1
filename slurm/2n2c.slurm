#!/bin/bash
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --mail-user=junwliang1@student.unimelb.edu.au
#SBATCH --mail-type=END
#SBATCH --partition=physical
#SBATCH --output=2n2c.txt
# Wall time, hours:mins:seconds
#SBATCH --time=0:1:0

# Remove old modules
module purge
# Load new modules
module load foss/2019b
module load mpi4py

# Run program
mpiexec -n 2 python3 "../src/main.py" "tinyTwitter.json"

# Job monitor command
my-job-stats -a -n -s
