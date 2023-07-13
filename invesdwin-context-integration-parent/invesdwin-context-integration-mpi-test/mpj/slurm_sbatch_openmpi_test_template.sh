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
#SBATCH --ntasks=2
## Specify partition
#SBATCH -p nodes

#source "${flight_ROOT:-/opt/flight}"/etc/setup.sh
#flight env activate gridware
#module load mpi/openmpi/4.1.5

#download openmpi 4.1.5, prepare openjdk 17 and JAVA_HOME
#./configure psm=false psm2=false torque=false sge=false pmi=/usr pmilib=/usr/lib64 extraopts=--with-libevent%external --enable-mpi-java --prefix=$HOME/tools/openmpi
#make
#make install

# create a .bash_profile similar to:
#JAVA_HOME=$HOME/tools/jdk/jdk-17.0.2
#export JAVA_HOME
#MAVEN_HOME=$HOME/tools/apache-maven-3.9.3
#export MAVEN_HOME
#MPI_HOME=$HOME/tools/openmpi
#export MPI_HOME
#OPAL_PREFIX=$MPI_HOME
#export OPAL_PREFIX
#PATH=$PATH:$HOME/.local/bin:$HOME/bin:$JAVA_HOME/bin:$MAVEN_HOME/bin:$MPI_HOME/bin
#export PATH
#LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$MPI_HOME/lib
#export LD_LIBRARY_PATH
#ulimit -Sn $(ulimit -Hn)

source ~/.bash_profile

mpirun -np $SLURM_NTASKS {ARGS}
