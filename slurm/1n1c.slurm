#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --mail-user=junwliang1@student.unimelb.edu.au
#SBATCH --mail-type=END
#SBATCH --partition=physical
#SBATCH --output=1n1c.txt
# Wall time, hours:mins:seconds
#SBATCH --time=0:15:0

# Remove old modules even the sticky modules
module --force purge
# Load new modules
module load foss/2019b
module load mpi4py
# Run program
# Analyze with 1 processors
mpiexec -n 1 python3 "../src/main.py" "../data/bigTwitter.json" "../data/sal.json"
# Job monitor command
my-job-stats -a -n -s
