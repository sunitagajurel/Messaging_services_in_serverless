import boto3
from Inspector import Inspector
import time

conn = None

def connect_sqs():
    global conn
    conn = boto3.resource('sqs')

def send_to_SQS(msg):
    if not conn: 
        connect_sqs()
    try:
        queue = conn.get_queue_by_name(QueueName ="withoutkombu")
        queue.send_message(MessageBody = msg)
    except Exception as e:
        raise(e)

def handler(event,context):
    inspector = Inspector()
    inspector.inspectContainer()
    sleep = event['sleep'] if "sleep" in event else 0
    msg = event['msg']
    # for warm containers
    if not sleep:
        send_to_SQS(msg)
    # setting up sleep value to  warm multiple containers 
    else:
        time.sleep(sleep)
    return inspector.finish()