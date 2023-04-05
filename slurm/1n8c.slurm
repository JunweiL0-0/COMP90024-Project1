#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --mail-user=junwliang1@student.unimelb.edu.au
#SBATCH --mail-type=END
#SBATCH --partition=physical
#SBATCH --output=1n8c.txt
# Wall time, hours:mins:seconds
#SBATCH --time=0:1:0

# Remove old modules even the sticky modules
module --force purge
# Load new modules
module load foss/2019b
module load mpi4py
# Load virtual enviroment and install software locally
virtualenv ~/venvs/venv-python3.7.4
source ~/venvs/venv-python3.7.4/bin/activate
pip3 install requirements.txt
# Run program
# Go to data folder
cd ../data/
# Split json file to chunks 8 processors
python3 split_script.py 8
# Analyze 8 processors
mpiexec -n 8 python3 "../src/main.py" "chunck1.json|chunck2.json|chunck3.json|chunck4.json|chunck5.json|chunck6.json|chunck7.json|chunck8.json" "sal.json"
# Delete all json files and links 
rm *.json
# Deactivate virtualenv
deactivate
# Job monitor command
my-job-stats -a -n -s
