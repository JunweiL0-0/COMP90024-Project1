#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --mail-user=junwliang1@student.unimelb.edu.au
#SBATCH --mail-type=END
#SBATCH --partition=physical
#SBATCH --output=1n1c.txt
# Wall time, hours:mins:seconds
#SBATCH --time=0:1:0

# Remove old modules
module purge
# Load new modules
module load foss/2019b
module load mpi4py
# Load virtual enviroment and install software locally
virtualenv ~/venvs/venv-python3.7.4
source ~/venvs/venv-python3.7.4/bin/activate
pip3 install ijson
# Run program
# Split json file to chunks one chunck
python3 split_script.py 1
# Analyze 1 processors
mpiexec -n 1 python3 "../src/main.py" "chunck1.json" "sal.json"
# Delete chuncks 
rm chunck*.json
# Deactivate virtualenv
deactivate
# Job monitor command
my-job-stats -a -n -s
