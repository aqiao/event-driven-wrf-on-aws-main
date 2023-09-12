#!/bin/bash -l
export I_MPI_OFI_LIBRARY_INTERNAL=0
export FI_PROVIDER=efa
export I_MPI_DEBUG=5
export I_MPI_FABRICS=ofi
export I_MPI_OFI_PROVIDER=efa
export I_MPI_PIN_DOMAIN=omp
export KMP_AFFINITY=compact
export OMP_STACKSIZE=12G
export SLURM_EXPORT_ENV=ALL
export WRF_VERSION=4.2.2
export MKL_NUM_THREADS=4
export OMP_NUM_THREADS=4


source /apps/scripts/env.sh 3 2

ulimit -s unlimited

HPC_MPI_DEBUG=1
# load MPI settings
source /apps/scripts/mpi_settings.sh


cd run
WRF_LOG=mpirun_${SARCH}_${HPC_COMPILER}_${HPC_MPI}_wrf-${WRF_VERSION}.log

echo "Running WRF on $(date)"
#ln -sfn ${HPC_PREFIX}/${HPC_COMPILER}/${HPC_MPI}/WRF-${WRF_VERSION}/main/wrf.exe .
START_DATE=$(date)
echo "zzz *** ${START_DATE} *** - JobStart - $(basename ${JOB_DIR}) - ${HPC_COMPILER} - ${HPC_MPI}" >> ${WRF_LOG} 2>&1
mpirun ${MPI_SHOW_BIND_OPTS} ./wrf.exe >> ${WRF_LOG} 2>&1

END_DATE=$(date)
echo "zzz *** ${END_DATE} *** - JobEnd - $(basename ${JOB_DIR}) - ${HPC_COMPILER} - ${HPC_MPI}" >> ${WRF_LOG} 2>&1

JOB_FINISH_TIME=$(($(date -d "${END_DATE}" +%s)-$(date -d "${START_DATE}" +%s)))
echo "zzz *** $(date) *** - Job ${JOB_DIR} took ${JOB_FINISH_TIME} seconds($(echo "scale=2;${JOB_FINISH_TIME}/3600" | bc) hours)." >> ${WRF_LOG} 2>&1


