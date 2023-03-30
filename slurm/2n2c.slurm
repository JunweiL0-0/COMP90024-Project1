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
# Install extra required libraries locally
virtualenv ~/venvs/venv-python3.7.4
source ~/venvs/venv-python3.7.4/bin/activate
pip install -r ../requirements.txt

# Run program
mpiexec -n 2 python3 "../src/main.py" "tinyTwitter.json"

# Deactivate virtualenv
deactivate

# Job monitor command
my-job-stats -a -n -s