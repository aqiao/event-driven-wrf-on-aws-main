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

echo $(pwd)
source /apps/scripts/env.sh 3 2

ulimit -s unlimited

NP=$(ls /sys/class/cpuid/ | wc -l)
day=$(date +%Y%m%d)



###################################################################################
# Print log function
###################################################################################
log ()
{
        timestamp=`date "+%Y.%m.%d-%H:%M:%S %Z"`
        echo "$timestamp $*"
}

###################################################################################
# Start Preprocessing
###################################################################################
cd preproc
echo $(pwd)
log "INFO - Starting geogrid.exe"
./geogrid.exe > geogrid.$day.log 2>&1
if [ $? -ne 0 ]
then
        log "CRIT - geogrid.exe Completed with errors."
        exit 1
fi
log "INFO - geogrid.exe Completed"

./link_grib.csh ../../downloads/ > link_grid.$day.log 2>&1
log "INFO - generate geog data"

rm -f FILE*
rm -f PFILE*


log "INFO - Starting ungrib.exe"
./ungrib.exe > ungrib.$day.log 2>&1
if [ $? -ne 0 ]
then
        log "CRIT - ungrib.exe Completed with errors."
        exit 1
fi

log "INFO - ungrib.exe Completed"

rm -f met_em*

log "INFO - Starting metgrid.exe"
#mpirun -np $NP ./metgrid.exe > metgrid.$day.log 2>&1
./metgrid.exe > metgrid.$day.log 2>&1
if [ $? -ne 0 ]
then
        log "CRIT - metgrid.exe Completed with errors."
        exit 1
fi
log "INFO - metgrid.exe Completed"


mv met_em* ../run/

###################################################################################
# Start WRF Processing
###################################################################################

cd ../run

rm -f rsl.*
rm -f wrfinput*
rm -f wrfbdy*
rm -f wrfout*


log "INFO - Starting real.exe"
#mpirun -np 4 ./real.exe  > real.$day.log 2>&1
./real.exe >real.$day.log 2>&1
if [ $? -ne 0 ]
then
        log "CRIT - real.exe Completed with errors."
        exit 1
fi
log "INFO - real.exe Completed"
cd ..
echo $(pwd)
#exit 0
