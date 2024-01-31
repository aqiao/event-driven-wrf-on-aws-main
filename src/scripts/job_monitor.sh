#!/bin/bash

# root_dir=/home/ec2-user/fsx # dev environment
root_dir=/fsx
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
# error code
INVALID_PARAM_CODE="invalid_param"
SBATCH_SUBMIT_FAILED_CODE="sbatch_submit_failed"
SCONTROL_UPDATE_FAILED_CODE="scontrol_update_failed"
SBATCH_RESUBMIT_FAILED_CODE="sbatch_resubmit_faild"
NO_FAILED_JOB_FOUND_CODE="no_failed_job_found"
NO_PENDING_JOB_FOUND_CODE="no_pending_job_found"
NO_RUNNING_JOB_FOUND_CODE="no_running_job_found"
RETRY_NUMBER_UPDATE_FAILED_CODE="retry_number_update_failed"
SUCCEEDED_CODE="succeeded"

if [ -z $bucket ];then
  echo "Please specify the s3 bucket as the first param"
  return 1
fi
#if [ -z $forecast_days ];then
#  forecast_days=12
#fi
#if [ -z "$expected_wrf_file_num" ];then
#  expected_wrf_file_num=2
#fi

# log runtime information to file job_monitor_log
# parameter: $1 represents message will be logged
# return: no return
log(){
  message=$1
  current_datetime=$(date +"%Y-%m-%d %H:%M:%S")
  echo $current_datetime $message >> $job_monitor_log
}

# clear legacy files if failed job
# since we check job output files to identify if job run successfully
# so once job failed, it's required to clear legacy files
# parameter: $1 legacy file path
# return: no return
clear_legacy_file(){
  domain_name=$1
  run_path="${root_dir}/${domain_name}/${WRF_JOB_FOLDER}"
  log "Cleaning legacy wrf out files in $run_path"
  rm -rf $run_path/wrfout_d* 2>/dev/null
  log "Cleaning legacy rsl files"
  rm -rf $run_path/rsl.out.* 2>/dev/null
  log "Cleaning done"
}

reach_max_retry(){
  job_id=$1
  job_name=$2
  if [ -z "job_id" ];then
    log "Job id is empty, check failed"
    return 1
  fi
  if [ -z "$job_name" ];then
    log "Job name is empty, check failed"
    return 1
  fi
  record_id="${job_name}_${job_id}"
  log "Running pre-check before retry job $record_id"
  retry_number=$(grep "$record_id" "$job_record" | awk '{print $4}')
  if [ -z "$retry_number" ];then
    retry_number=0
  fi
  if [ $retry_number -ge $MAX_RETRY_NUM ];then
    log "Job $job_name retry times ($retry_number) reached max retry number $MAX_RETRY_NUM for $job_name, check failed"
    return 1
  fi
  return 0
}

retry_pre_job(){
  job_name=$1
  log "Running specific pre option for job $job_name."
  log "Overriding namelist files with backup ones for job $job_name."
  domain_name=$(echo $job_name | awk -F '_' '{printf "%s_%s",$2,$3}')
  aws s3 cp "s3://$bucket/input/${domain_name}_backup/namelist.wps" "$root_dir/${domain_name}/${PRE_JOB_FOLDER}/" --quiet
}

retry_wrf_job(){
  job_name=$1
  log "Running specific wrf option for job $job_name."
  domain_name=$(echo $job_name | awk -F '_' '{printf "%s_%s",$2,$3}')
  log "Overriding namelist files with backup ones for domain $domain_name"
  aws s3 cp "s3://${bucket}/input/${domain_name}_backup/namelist.wps" "${root_dir}/${domain_name}/${PRE_JOB_FOLDER}/" --quiet
  aws s3 cp "s3://${bucket}/input/${domain_name}_backup/namelist.input" "${root_dir}/${domain_name}/${WRF_JOB_FOLDER}/" --quiet
  clear_legacy_file $domain_name
}

retry_post_job(){
  job_name=$1
  log "Running specific post option for job $job_name."
}

retry_fini_job(){
  job_name=$1
  log "Running specific fini option for job $job_name."
}

update_retry_num(){
  job_id=$1
  job_name=$2
  record_id="${job_name}_${job_id}"
  log "Updating retry number for job $record_id"
  domain_name=$(echo $job_name | awk -F '_' '{printf "%s_%s\n",$2,$3}')
  run_path="${root_dir}/${domain_name}/${WRF_JOB_FOLDER}"
  retry_number=$(grep "$record_id" "$job_record" | awk '{printf $4}')
  latest_wrf_file_num=$(ls $run_path | grep wrfout_d | wc -l)
  if [ $latest_wrf_file_num -eq 0 ];then
    latest_modified=0
  else
    latest_modified=$(ls -1 ${run_path}/wrfout_d* -t | head -n 1 | xargs stat -c %Y)
  fi
  if [ -z "$retry_number" ];then
    log "$retry_number is initialized to 0 for job ${record_id}"
    retry_number=$MAX_RETRY_NUM
  else
    log "Adding one to current retry $retry_number in domain $domain_name"
    ((retry_number++))
    log "latest retry number $retry_number"
  fi
  update_job_record ${record_id} ${latest_modified} ${latest_wrf_file_num} ${retry_number}
}

scan_failed_job(){
  log "Scanning failed job ..."
  output_fields="%.i %.j %.E %.R"
  job_status="F"
  squeue -h -t $job_status -o "$output_fields" | while read -r failed_job;do
    retry_failed_job "$failed_job"
  done
}

retry_failed_job(){
  job="$1"
  log "Found failed job $job"
  job_props=($job)
  job_id=${job_props[0]}
  job_name=${job_props[1]}
  job_reason=${job_props[3]}
  # if there is more pre-check such as reach_max_retry in the further
  # we can extract a specific method like running_wrf_retry_precheck
  reach_max_retry $job_id $job_name
  if [ $? -eq 1 ];then
    echo ""
    return 1
  fi
  retry_common $job_name $job_id
}

retry_common(){
  job_name=$1
  job_id=$2
  log "Retrying failed job $job_name"
  job_prefix=$(echo $job_name | awk -F '_' '{print $1}')
  func_name="retry_${job_prefix}_job"
  log "Running function $func_name to retry"
  $func_name $job_name
  new_job_id=$(resubmit $job_name $job_id)
  log "Got new job id after retry $new_job_id"
  if [ -n "$new_job_id" ];then
    update_job_dependency $job_id $new_job_id
    update_retry_num $job_id $job_name
    log "Job $job_name retry succeeded"
  fi
}

resubmit(){
  job_name=$1
  job_id=$2

  sbatch_file="${monitor_home}/${job_name}.sh"
  if [ ! -f $sbatch_file ];then
    log "Downloading sbatch script ${job_name}.sh for job $job_id"
    aws s3 cp "s3://$bucket/monitor/${job_name}.sh" $monitor_home --quiet
    aws s3 cp "s3://$bucket/monitor/${job_name}_script.sh" $monitor_home --quiet
  fi
  # in case download failed, let's check it again
  if [ -f $sbatch_file ];then
    result=$(sbatch $sbatch_file)
    log "sbatch running result $result after resubmit"
    if [ $? -eq 0 ];then
      new_job_id=$(echo "$result" | grep -oP [0-9]+)
      log "Job $job_id resubmit succeeded, new job id is $new_job_id"
      echo $new_job_id
    else
      log "Job $job_id resubmit failed, error message: $result"
      echo ""
    fi
  else
    log "$sbatch_file not found, please check if s3://$bucket/monitor/${job_name}.sh existed"
    echo ""
  fi
}

running_job_retry_precheck(){
  job_id=$1
  job_name="$2"
  if [ -z "$job_id" ];then
    log "Please specify job id as the first param for function running_job_retry_precheck"
    return 1
  fi
  if [ -z "$job_name" ];then
    log "Please specify job name as the second param for function running_job_retry_precheck"
    return 1
  fi
  record_id="${job_name}_${job_id}"
  log "Checking if job $record_id is wrf job"
  wrf_job_name=$(echo "$job_name" | grep "^$WRF_JOB_PREFIX")
  if [ -z "$wrf_job_name" ];then
    log "Job $job_name is not a wrf job"
    return 1
  fi
  domain_name=$(echo $job_name | awk -F '_' '{printf "%s_%s",$2,$3}')
  run_path="${root_dir}/${domain_name}/$WRF_JOB_FOLDER"
  latest_wrf_file_num=$(ls $run_path | grep wrfout_d | wc -l)
  if [ $latest_wrf_file_num -eq 0 ];then
    latest_modified=0
  else
    latest_modified=$(ls -1 ${run_path}/wrfout_d* -t | head -n 1 | xargs stat -c %Y)
  fi
  record=$(grep "$record_id" "$job_record")
  log "Checking if record $record is empty"
  if [ -z "$record" ];then
    last_modified=0
    last_wrf_file_num=0
    retry_number=0
  else
    job_props=($record)
    last_modified=${job_props[1]}
    last_wrf_file_num=${job_props[2]}
    retry_number=${job_props[3]}
  fi
  # since we have gotten last record per job, let's update job record with latest value
  update_job_record $record_id $latest_modified $latest_wrf_file_num $retry_number
  log "Detecting if wrf out files increased from job record $record for job  $job_name"

  log "Comparing last_modified: $last_modified, latest_modified: $latest_modified for job $job_name"
  if [ $last_modified -eq $latest_modified ];then
    if [ $latest_wrf_file_num -eq 0 ];then
      log "Current wrf out file number is 0, will check in the next loop for job $job_name"
      return 1
    fi
    if [ $latest_wrf_file_num -eq $expected_wrf_file_num ];then
      log "Current wrf out file number reaches expected $latest_wrf_file_num for job $job_name"
      return 1
    fi
    reach_max_retry $job_id $job_name
    if [ $? -eq 1 ];then
      # reached max retry number
      return 1
    fi
    return 0
  fi
  log "WRF out files are still being produced for job $job_name"
  return 1
}

update_job_record(){
  record_id=$1
  latest_modified=$2
  latest_wrf_file_num=$3
  retry_number=$4
  record=$(grep "${record_id}" "${job_record}")
  log "Handling record $record_id with latest_modified: $latest_modified, latest_wrf_file_num: $latest_wrf_file_num, retry_number: $retry_number"
  if [ -n "$record" ];then
    sed_command="s/^${record_id}.*/${record_id} ${latest_modified} ${latest_wrf_file_num} ${retry_number}/"
    log "Will run below command: sed -i $sed_command"
    sed -i "$sed_command" $job_record
    if [ $? -eq 0 ];then
     log "Job record update succeeded"
    else
     log "Job record update failed"
    fi
  else
    echo "$record_id $latest_modified $latest_wrf_file_num $retry_number" >> $job_record
    log "Job record creation succeeded"
  fi
}

scan_running_wrf_job(){
  log "Scanning long time running wrf job ..."
  output_fields="%.i %.j %.E %.R"
  job_status="R"
  squeue -h -t $job_status -o "$output_fields" | while read -r running_job; do
    retry_running_wrf_job "$running_job"
  done
}

retry_running_wrf_job(){
  job="$1"
  log "Found running job $job"
  job_props=($job)
  job_id=${job_props[0]}
  job_name=${job_props[1]}
  log "Doing pre-check for job $job_name ($job_id)"
  pre_check=$(running_job_retry_precheck $job_id $job_name)
  if [ $? -eq 0 ];then
    log "Cancelling wrf job $job_id"
    scancel $job_id
    if [ $? -eq 0 ];then
      retry_common $job_name $job_id
    fi
  fi
}

update_job_dependency(){
  old_job_id=$1
  new_job_id=$2
  log "new_job_id $new_job_id, old_job_id: $old_job_id"
  log "Checking if there is any job that depends on $old_job_id"
  output_fields="%.i %.j %.E %.R"
  job_status="PD"
  expected_pending_reason="(DependencyNeverSatisfied)"
  squeue -h -t $job_status -o "$output_fields" | while read -r pending_job;do
    log "Verifying pending job $pending_job"
    # DO not use double quote to enclose $pending_job variable, or it will not be split by space
    job_props=($pending_job)
    job_id=${job_props[0]}
    job_name=${job_props[1]}
    # afterok:45(failed)
    job_dependency=${job_props[2]}
    job_reason=${job_props[3]}
    log "Comparing job_reason: $job_reason, expected_pending_reason:$expected_pending_reason"
    failed_job_id=$(echo "$job_dependency" | grep -oP [0-9]+)
    if [ -z "$failed_job_id" ];then
      continue
    fi
    log "Comparing failed job id: $failed_job_id, depended job id: $old_job_id"
    if [ $failed_job_id -eq $old_job_id ];then
      log "Found depending job $job_id and the pending reason is $job_reason"
      log "Updating depending job dependency from $old_job_id to $new_job_id"
      result=$(scontrol update jobid=$job_id dependency=afterok:$new_job_id)
      if [ $? -eq 0 ];then
        log "Job dependency update succeeded"
      else
        log "Job dependency update failed due to $result"
      fi
    fi
  done
}

# main thread begin
if [ ! -f $job_record ];then
  log "Creating job record file $job_record"
  touch $job_record
fi

scan_failed_job
scan_running_wrf_job
# upload log and record file to s3 for further troubleshooting
#aws s3 cp $job_monitor_log "s3://$bucket/output/" --quiet
#aws s3 cp $job_record "s3://$bucket/monitor/" --quiet
# main thread end