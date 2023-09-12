import json
import boto3
import os
import re

def main(event, context):
    
    msg = json.loads(event["Records"][0]["Sns"]["Message"])
    key = msg["Records"][0]["s3"]["object"]["key"]
    p = re.compile(r"""
                 gfs.                      # GFS prefix
                 (?P<y>\d{4})              # Year
                 (?P<m>\d{2})              # Month
                 (?P<d>\d{2})              # Day
                 /(?P<h>\d{2})             # Hour
                 /atmos                    # Atmospheric components
                 /gfs.t(?P=h)z.pgrb2.0p50.f096$   # Filename
                 """,
                 re.VERBOSE)
    m = p.match(key)
    if not m:
        return
    print(event)
    print(msg)
    print(key)
    ftime = f"{m.group('y')}-{m.group('m')}-{m.group('d')}T{m.group('h')}:00:00Z"

    sfn = boto3.client('stepfunctions')
    sfn.start_execution(
        stateMachineArn=os.getenv("SM_ARN"),
        input = "{\"action\" : \"create\",\"type\" : \"od\",\"ftime\":\"" + ftime + "\"}"
    )

def destroy(event, context):
    print(event)
    clustername=os.getenv('CLUSTER_NAME')
    region=os.getenv('REGION')
    sfn = boto3.client('stepfunctions')
    sfn.start_execution(
        stateMachineArn=os.getenv("SM_ARN"),
        input = "{\"action\" : \"destroy\",\"clusterName\":\""+clustername+ "\",\"region\":\""+region+"\",\"ftime\":\" \"}"
    )
