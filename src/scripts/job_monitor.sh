#!/bin/bash

root_dir=/home/ec2-user # dev environment
#root_dir=/fsx
monitor_home=$root_dir/monitor
job_monitor_log=$monitor_home/job_monitor.log
# record each domain run folder last modified date
# sample /fsx/domain_1/run 12345678
job_record=$monitor_home/job_record
bucket=$1
#forecast_days=$2
expected_wrf_file_num=554
# job prefix in slurm squeue
PRE_JOB_PREFIX="pre"
WRF_JOB_PREFIX="wrf"
POST_JOB_PREFIX="post"
FINISH_JOB_PREFIX="fini"
# job folder in /fsx/domain_x
PRE_JOB_FOLDER="preproc"
WRF_JOB_FOLDER="run"
POST_JOB_FOLDER="post"
FINISH_JOB_FOLDER="fini"
# max retry number
MAX_RETRY_NUM=1

log(){
  message=$1
  current_datetime=$(date +"%Y-%m-%d %H:%M:%S")
  echo $current_datetime $message >> $job_monitor_log
}

# use post job id as key
# use post job name, post job status, wrf job id, wrf job status, finish job id as value
build_post_job_map(){
  map=$1
  job_status="CF,R,PD,F,CD"
  job_fields="%.i %.j %.E %.R %.t"
  finished_job_id=-1
  squeue -h -t ${job_status} -o "${job_fields}" | while read -r job
  do
    # no need add double quote around job variable
#    echo "job ${job}"
    arr=(${job})

    job_id=${arr[0]}
    job_name=${arr[1]}
    job_dependency=${arr[2]}
    job_reason=${arr[3]}
    job_status=${arr[4]}
    post_job_name=$(echo ${job_name} | grep ${POST_JOB_PREFIX})
    finish_job_name=$(echo ${job_name} | grep ${FINISH_JOB_PREFIX})
#    echo ${post_job_name}
#    echo ${finish_job_name}
    if [ -n "${post_job_name}" ];then
      wrf_job_id=$(echo "${job_dependency}" | sed -r 's/^afterok:([0-9]+)\(([a-z]+)\)$/\1/')
      wrf_job_status=$(echo "${job_dependency}" | sed -r 's/^afterok:([0-9]+)\(([a-z]+)\)$/\2/')
      log "Extracted wrf_job_id ${wrf_job_id}, wrf_job_status ${wrf_job_status} from ${job}"
      map[${job_id}]="${job_name}, ${job_status}, ${wrf_job_id}, ${wrf_job_status} ${map[${job_id}]}"
    fi
    if [ -n "${finish_job_name}" ];then
      # finished job dependency field sample: afterok:4(unfulfilled),afterok:5(unfulfilled),afterok:6(unfulfilled)
      finished_job_id=${job_id}
    fi
  done
  # append finish job id at the end
  for post_job_id in ${!map[@]}
  do
    echo ${post_job_id}
    echo ${map[${post_job_id}]}
    map[${post_job_id}]="${map[${post_job_id}]} ${finished_job_id}"
  done
}

retry(){
  post_job_map=$1
  # iterate map key
  for post_job_id in ${!post_job_map[@]}
  do
    echo ${post_job_map[${post_job_id}]}
  done
}

scan_failed_wrf_job(){
  declare -A map;
  build_post_job_map ${map}
  retry ${map}
}
scan_failed_wrf_job