#!/bin/bash -l

################# Part-1 Slurm directives ####################
## Working dir
#SBATCH -D {WORKDIR}
## Environment variables
#SBATCH --export=ALL
## Output and Error Files
#SBATCH -o job-%j.output
#SBATCH -e job-%j.error
## Job name
#SBATCH -J parallel-mpi-test
## Run time: "hours:minutes:seconds", "days-hours"
#SBATCH --time=00:05:00
## Memory limit (in megabytes). Total --mem or amount per cpu --mem-per-cpu
#SBATCH --mem=2g
## Processing slots
#SBATCH --nodes=2
#SBATCH --ntasks=2
## Specify partition
#SBATCH -p nodes

################# Part-2 Shell script ####################
#===============================
#  Activate Flight Environment
#-------------------------------
source "${flight_ROOT:-/opt/flight}"/etc/setup.sh

#==============================
#  Activate Package Ecosystem
#------------------------------
# e.g.:
# Load the OpenMPI module for access to `mpirun` command
flight env activate gridware
module load mpi/openmpi/4.1.5

if ! command -v mpirun &>/dev/null; then
    echo "No mpirun command found, ensure that a version of MPI is installed and available in PATH" >&2
    exit 1
fi

#===========================
#  Create results directory
#---------------------------
RESULTS_DIR="$(pwd)/${SLURM_JOB_NAME}-outputs/${SLURM_JOB_ID}"
echo "Your results will be stored in: $RESULTS_DIR"
mkdir -p "$RESULTS_DIR"

#===============================
#  Application launch commands
#-------------------------------
# Customize this section to suit your needs.

echo "Executing job commands, current working directory is $(pwd)"

# REPLACE THE FOLLOWING WITH YOUR APPLICATION COMMANDS

echo "This is an example job. It was allocated $SLURM_NTASKS slot(s) across $SLURM_JOB_NUM_NODES node(s). The master process ran on `hostname -s` (as `whoami`)." >> $RESULTS_DIR/test.output
mpirun -np $SLURM_NTASKS {ARGS} >> $RESULTS_DIR/test.output

echo "Output file has been generated, please check $RESULTS_DIR/test.output"