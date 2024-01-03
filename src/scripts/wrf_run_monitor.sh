#!/bin/bash

# root_dir=/home/ec2-user/fsx # dev environment
root_dir=/fsx
wrf_run_monitor_log=$root_dir/wrf_run_monitor.log

log(){
  message=$1
  current_datetime=$(date +"%Y-%m-%d %H:%M:%S")
  echo $current_datetime $message >> $wrf_run_monitor_log
}
# record each domain run folder last modified date
# sample /fsx/domain_1/run 12345678
wrf_run_monitor=$root_dir/wrf_run_monitor

if [ ! -f $wrf_run_monitor ];then
  touch $wrf_run_monitor
fi


# remember to add 1d option, or it will list subfolder in domain_x
IFS=" "
expected_wrf_out_num=554
ls -1d $root_dir/domain_* | while read -r domain;
do
  # echo $domain
  run_path="$domain/run"
  log "Begin to check $run_path last modified date"
  # echo $run_path
  if [ -d $run_path ];then
    # Get the last modified timestamp

    latest_modified=$(stat -c %Y "$run_path")
    domain_line=$(grep $run_path $wrf_run_monitor)
    if [ -z "$domain_line" ];then
      echo "$run_path $latest_modified" >> $wrf_run_monitor
      log "Added last modified date $run_path $latest_modified"
    else
      log "Got last modified date $domain_line"
      # get last modified date from log file
      read -ra parts <<< "$domain_line"
      last_modified=${parts[1]}
      log "Checking last modified $last_modified and latest modified date $latest_modified"
      if [ $latest_modified -eq $last_modified ];then
        # no file updated,let's check wrfout file number
        wrf_out_num=$(ls $run_path | grep wrfout_d* | wc -l)
        log "Found $wrf_out_num WRF out files"
        if [ $wrf_out_num -eq $expected_wrf_out_num ];then
          # rsl_output=$(tail -1 )
          echo "all file generated"
          log "WRF job $run_path completed successfully"
        else
          echo "wrf run job failed"
          log "WRF job $run_path failed,will clean legacy files and cancel corresponding slurm job"
        fi
      else
        # wrf job is still running, let's check status in next loop
        log "wrf job is still running, let check in the next loop"
      fi
    fi
  else
    log "$run_path not found "
  fi
done
