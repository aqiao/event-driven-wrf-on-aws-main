# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from functools import lru_cache
import os
import time
import boto3
import botocore
import json
import requests

#region = os.getenv("AWS_REGION")
ip = "127.0.0.1"
bucket = os.getenv("BUCKET_NAME")
job_num= int(os.getenv("DOMAINS_NUM"))
ftime = "2023-01-01:12:00:00Z"
template = {
    "job": {
        "name":"",
        "nodes":1,
        "cpus_per_task": 4,
        "tasks_per_node": 24,
        "current_working_directory":"/fsx",
        "environment":{
            "PATH":"/bin:/usr/bin/:/usr/local/bin/",
            "LD_LIBRARY_PATH":"/lib/:/lib64/:/usr/local/lib"
        },
        "requeue": "false"
    },
    "script": ""
}

@lru_cache
def token():
    session = boto3.session.Session()
    sm = session.client('secretsmanager')
    secret = sm.get_secret_value(SecretId="JWTKey")
    return secret['SecretString']

@lru_cache
def headers():
    return {
            "X-SLURM-USER-NAME": "ec2-user",
            "X-SLURM-USER-TOKEN": token(),
            "content-type": "application/json",
            }

def submit(data):
    global ip
    url = f"http://{ip}:8080/slurm/v0.0.37/job/submit"
    resp = requests.post(url, data=json.dumps(data), headers=headers())
    print(resp)
    jid = resp.json()["job_id"]
    print(resp.json())
    print(resp.status_code)
    return jid

def status(jobid):
    global ip
    url = f"http://{ip}:8080/slurm/v0.0.37/job/{jobid}"
    resp = requests.get(url, headers=headers())
    print(resp)
    return resp.json()

def fini(ids):
    global bucket
    global ftime
    y=ftime[0:4]
    m=ftime[5:7]
    d=ftime[8:10]
    h=ftime[11:13]
    output = f"s3://{bucket}/outputs/{y}/{m}/{d}/{h}"    
    with open("jobs/fini.sh", "r") as f:
        script = f.read()
    script += f"\naws s3 cp forecast.done {output}/forecast.done"
    script += f"\naws s3 cp slurm-${{SLURM_JOB_ID}}.out {output}/logs/slurm-${{SLURM_JOB_ID}}.out\n"
    template["job"]["nodes"] = 1
    template["job"]["name"] = "fini"
    template["job"]["tasks_per_node"] = 1
    template["job"]["current_working_directory"] = "/fsx"
    template["job"]["dependency"] = f"afterok:{':'.join([str(x) for x in ids])}"
    template["script"] = script
    print(template)
    return submit(template)

    
def preproc(zone):
    global bucket
    global ftime
    y=ftime[0:4]
    m=ftime[5:7]
    d=ftime[8:10]
    h=ftime[11:13]
    output = f"s3://{bucket}/outputs/{y}/{m}/{d}/{h}/{zone}"
    with open("jobs/pre.sh", "r") as f:
        script = f.read()
    script += f"\naws s3 cp slurm-${{SLURM_JOB_ID}}.out {output}/logs/\n"
    script += f"\naws s3 cp preproc/geogrid.*.log {output}/logs/\n"
    script += f"\naws s3 cp preproc/ungrib.*.log {output}/logs/\n"
    script += f"\naws s3 cp preproc/metgrid.*.log {output}/logs/\n"
    script += f"\naws s3 cp run/real.*.log {output}/logs/\n"
    template["job"]["name"] = "pre_" + zone
    template["job"]["nodes"] = 1
    template["job"]["cpus_per_task"] = 1
    template["job"]["tasks_per_node"] = 12
    template["job"]["current_working_directory"] = f"/fsx/{zone}"
    template["script"] = script
    print(template)
    return submit(template)

def run_wrf(zone,pid):
    global bucket
    global ftime
    y=ftime[0:4]
    m=ftime[5:7]
    d=ftime[8:10]
    h=ftime[11:13]
    output = f"s3://{bucket}/outputs/{y}/{m}/{d}/{h}/{zone}"
    with open("jobs/run.sh", "r") as f:
        script = f.read()
    script += f"\naws s3 cp ../slurm-${{SLURM_JOB_ID}}.out {output}/logs/\n"
    script += f"aws s3 cp . {output}/wrfout/ --recursive --exclude \"*\" --include \"wrfout_*\"\n"
    script += f"mv wrfout_* ${y}-${m}-${d}"
    template["job"]["name"] = "wrf_" + zone
    template["job"]["nodes"] = 2 
    template["job"]["cpus_per_task"] = 4
    template["job"]["tasks_per_node"] = 24
    # please note current working directory is /fsx/{zone}
    # in run.sh script, it will change current working directory to run
    template["job"]["current_working_directory"] = f"/fsx/{zone}"
    template["job"]["dependency"] = f"afterok:{pid}"
    template["script"] = script
    print(template)
    return submit(template)
    

# 当前工作目录是 /fsx吗
def post(zone, jid):
    global bucket
    global ftime
    y=ftime[0:4]
    m=ftime[5:7]
    d=ftime[8:10]
    h=ftime[11:13]
    output = f"s3://{bucket}/outputs/{y}/{m}/{d}/{h}/{zone}"
    # with open("jobs/post.sh", "r") as f:
    #     script = f.read()
    # script += f"\naws s3 cp --no-progress ${{grib}} {output}/${{grib}}"
    # script += f"\naws s3 cp slurm-${{SLURM_JOB_ID}}.out {output}/logs/slurm-${{SLURM_JOB_ID}}.out\n"
    template["job"]["nodes"] = 1
    template["job"]["name"] = f"post_"+zone
    template["job"]["dependency"] = f"afterok:{jid}"
    # 设置当前工作路径
    template["job"]["current_working_directory"] = f"/fsx/post-scripts/"
    # download post scripts to efs path
    script = f"pip install netCDF4 geocat.comp wrf-python tqdm xarray pandas numpy\n"
    script += f"python3 /fsx/post-scripts/process_gfs.py /fsx/{zone} {output}"
    # script += f"\naws s3 cp /fsx/{zone}/post {output}/post/ --recursive \n"
    template["script"] = script
    print(template)
    return submit(template)


def main(event, context):
    global ip
    global job_num
    global ftime
    
    print(event)
    ip=event['headNode']['privateIpAddress']
    ftime=event['ftime']
    print(ip)
    pids=[]
    jids=[]
    lids=[]

    for i in range(1,job_num+1):
        n='domain_'+str(i)
        pids.append(preproc(n))
    for i in range(1,job_num+1):
        n='domain_'+str(i)
        jids.append(run_wrf(n,pids[i-1]))
    for i in range(1,job_num+1):
        n='domain_'+str(i)
        lids.append(post(n,jids[i-1]))
    fini(lids)
