#!/bin/bash -l

################# Slurm directives ####################
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
#SBATCH --ntasks-per-node=1
## Specify partition
#SBATCH -p nodes

export MPJ_HOME={MPJ_HOME}
export PATH=$MPJ_HOME/bin:$PATH
export JAVA_HOME={JAVA_HOME}

mpjrun.sh -np $SLURM_NTASKS {ARGS}