import json
import boto3
import os
from datetime import date, datetime

topic_arn = os.environ.get('Topic_Arn')

def lambda_handler(event, context):

    print("email lambda event: " + json.dumps(event, default=json_serial))
    print("sns topic: " + topic_arn)

    sns_client = boto3.client('sns')
    full_content = event["result"]["Payload"]
    adjusted_schedules  = event["result"]["Payload"]["Ajustments"]

    sns_client.publish(
        TopicArn=topic_arn,
        Subject="Adjusted Maintenance Schedules",
        Message=json.dumps(adjusted_schedules, default=json_serial))    

    sns_client.publish(
        TopicArn=topic_arn,
        Message=json.dumps(full_content, default=json_serial))    

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))