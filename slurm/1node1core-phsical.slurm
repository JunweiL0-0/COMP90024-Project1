#!/bin/bash

#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --partition-physical
#SBATCH --output=1node1core-physical.txt
#SBATCH --mail-user=junwliang1@student.unimelb.edu.au

# Run program
mpiexec python3 "src/main.py"