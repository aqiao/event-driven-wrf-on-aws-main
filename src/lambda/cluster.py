# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import datetime as dt
import os
import re

import boto3
import botocore
import json
import requests
import yaml


region = os.getenv("REGION", "us-east-2")


def gateway(url, method, data):

    req_call = {
        "POST": requests.post,
        "GET": requests.get,
        "PUT": requests.put,
        "PATCH": requests.patch,
        "DELETE": requests.delete,
    }.get(method)

    print(f"url: {url}")
    session = botocore.session.Session()
    request = botocore.awsrequest.AWSRequest(method=method, url=url, data=data)
    botocore.auth.SigV4Auth(session.get_credentials(), "execute-api", region).add_auth(request)
    boto_request = request.prepare()
    boto_request.headers["content-type"] = "application/json"
    response = req_call(url, data=data, headers=boto_request.headers, timeout=300)
    #code = response.status_code
    #print(f"Response code: {code}")
    #return response.json()
    return response

def main(event, context):
    baseurl = os.getenv("PCLUSTER_API_URL")
    cluster_name = os.getenv("CLUSTER_NAME", "wx-pcluster")
    path = f"{baseurl}/v3/clusters"
    global region
    print(event)
    ftime=event['ftime']
    if (event['action']=='create'):
      if (event['type']=='od'):
        with open("hpc6a.yaml", "r") as cf:
          config_data = yaml.safe_load(cf)
        #ftime = '2023-01-05:12:00:00Z'
        #cluster_name=f"{cluster_name}-od"
        print(cluster_name)
        config_data["Region"] = region
        config_data["HeadNode"]["Networking"]["SubnetId"] = os.getenv("SUBNETID")
        config_data["HeadNode"]["Networking"]["AdditionalSecurityGroups"][0] = os.getenv("SG")
        config_data["Scheduling"]["SlurmQueues"][0]["Networking"]["SubnetIds"][0] = os.getenv("SUBNETID")
        config_data["Scheduling"]["SlurmQueues"][0]["Iam"]["S3Access"][0]["BucketName"] = os.getenv("BUCKET_NAME")
        config_data["HeadNode"]["CustomActions"]["OnNodeConfigured"]["Script"] = os.getenv("S3_URL_POST_INSTALL_HEADNODE")
        #config_data["HeadNode"]["CustomActions"]["OnNodeConfigured"]["Script"] = "https://raw.githubusercontent.com/nwcd-solutions/wrf-on-aws/master/pc_setup_scripts/post_install_amd.sh"
        config_data["HeadNode"]["CustomActions"]["OnNodeConfigured"]["Args"][0] = region
        config_data["HeadNode"]["CustomActions"]["OnNodeConfigured"]["Args"][1] = os.getenv("FORECAST_DAYS")
        #config_data["HeadNode"]["CustomActions"]["OnNodeConfigured"]["Args"][1] = os.getenv("SNS_TOPIC")
        config_data["HeadNode"]["CustomActions"]["OnNodeConfigured"]["Args"][2] = ftime
        config_data["HeadNode"]["CustomActions"]["OnNodeConfigured"]["Args"][3] = os.getenv("JWTKEY")
        config_data["HeadNode"]["CustomActions"]["OnNodeConfigured"]["Args"][4] = os.getenv("BUCKET_NAME")
        config_data["HeadNode"]["CustomActions"]["OnNodeConfigured"]["Args"][5] = os.getenv("NUM_DOMAINS")
        url=path
        method = "POST"
        data = json.dumps({"clusterConfiguration": yaml.dump(config_data, default_flow_style=False),
          "clusterName": cluster_name,
          "rollbackOnFailure": False
        })
        res=gateway(url, method, data)
        code = res.status_code
        print(code)
        if (code >=200 and code <400):
          out=res.json()['cluster']
          out['action']='status'
          out['ftime']=ftime
        else:
          print(res.json())
          out={"clusterStatus":"failed"}
          out['failed_message']=res.json()
          out['clusterName']=cluster_name
        return out
      elif (event['type']=='spot'):
        with open("hpc6a.yaml", "r") as cf:
          config_data = yaml.safe_load(cf)
        ftime = '2023-01-05:12:00:00Z'
        #cluster_name=f"{cluster_name}-spot"
        print(cluster_name)
        config_data["Region"] = region
        config_data["HeadNode"]["Networking"]["SubnetId"] = os.getenv("SUBNETID")
        config_data["HeadNode"]["Networking"]["AdditionalSecurityGroups"][0] = os.getenv("SG")
        config_data["Scheduling"]["SlurmQueues"][0]["Networking"]["SubnetIds"][0] = os.getenv("SUBNETID")
        config_data["Scheduling"]["SlurmQueues"][0]["Iam"]["S3Access"][0]["BucketName"] = os.getenv("BUCKET_NAME")
        config_data["HeadNode"]["CustomActions"]["OnNodeConfigured"]["Script"] = "https://raw.githubusercontent.com/nwcd-solutions/wrf-on-aws/master/pc_setup_scripts/post_install_amd.sh"
        config_data["HeadNode"]["CustomActions"]["OnNodeConfigured"]["Args"][0] = region
        config_data["HeadNode"]["CustomActions"]["OnNodeConfigured"]["Args"][1] = os.getenv("SNS_TOPIC")
        config_data["HeadNode"]["CustomActions"]["OnNodeConfigured"]["Args"][2] = ftime
        config_data["HeadNode"]["CustomActions"]["OnNodeConfigured"]["Args"][3] = os.getenv("JWTKEY")
        config_data["HeadNode"]["CustomActions"]["OnNodeConfigured"]["Args"][4] = os.getenv("BUCKET_NAME")
        config_data["HeadNode"]["CustomActions"]["OnNodeConfigured"]["Args"][5] = '2'
        url=path
        method = "POST"
        data = json.dumps({"clusterConfiguration": yaml.dump(config_data, default_flow_style=False),
          "clusterName": cluster_name,
          "rollbackOnFailure": False
        })
        res=gateway(url, method, data)
        code = res.status_code
        print(code)
        if (code >=200 and code <400):
          out=res.json()['cluster']
          out['action']='status'
          out['ftime']=ftime
        else:
          print(res.json())
          out={"clusterStatus":"failed"}
          out['failed_message']=res.json()
          out['clusterName']=cluster_name
        return out
    elif (event['action']=='status'):
      print('query status of the cluster')
      params = {"region": event['region']}
      data = json.dumps({"clusterName": event['clusterName']})
      #data = json.dumps({"clusterName": 'hpc-x86'})
      method = "GET"
      url = f"{path}/{event['clusterName']}?region={region}"
      #url = f"{path}/hpc-x86?region={region}"
      res=gateway(url, method, data)
      code = res.status_code
      print(code)
      if (code >=200 and code <400):
        out=res.json()
        print(out)
        if (out['clusterStatus']=='CREATE_IN_PROGRESS'):
            out['action']='status'
            out['ftime']=ftime
        else:
            out['action']='destroy'
            out['ftime']=ftime
      else:
        out={"CheckclusterStatus":"failed"}
      return out
    elif (event['action']=='destroy'):
      print("Destroy the cluster")    
      c = boto3.client("iam")
      roles = c.list_roles()
      for role in roles["Roles"]:
        n = role["RoleName"]
        #if n.startswith(f"{cluster_name}-Role") and not n.startswith(f"{cluster_name}-RoleHeadNode"):
        if n.startswith("wx-pcluster-Role") and not n.startswith("wx-pcluster-RoleHeadNode"):   
            policies = c.list_attached_role_policies(RoleName=n)
            for policy in policies["AttachedPolicies"]:
                c.detach_role_policy(RoleName=n, PolicyArn=policy["PolicyArn"])
      cluster_name=event['clusterName']
      region=event['region']
      params = {"region": region}
      data = json.dumps({"clusterName": cluster_name})
      method = "DELETE"
      url = f"{path}/{cluster_name}?region={region}"
      res=gateway(url, method, data)
      code = res.status_code
      print(code)
      if (code >=200 and code <400):
        out=res.json()
        out['action']='destroystatus'
        out['ftime']=ftime
      else:
        print(res.json())
        out={"CheckclusterStatus":"deleted failed"}
      print(out)
      return out
    elif (event['action']=='destroystatus'):
      print('query status of the cluster deleting')
      client = boto3.client('cloudformation')
      cf_arn=event['cluster']['cloudformationStackArn']
      print(cf_arn)
      res = client.describe_stacks( StackName=cf_arn )
      out={}
      out['action']='destroystatus'
      out['ftime']=ftime
      out['cluster']={
        "cloudformationStackArn":cf_arn,
        "clusterStatus":res['Stacks'][0]['StackStatus']
      }
      return out
