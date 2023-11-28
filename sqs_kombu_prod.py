from kombu import Connection
from Inspector import Inspector
import time

conn = None

def connect_sqs():
        global conn
        conn = Connection("sqs://")
       
def send_to_SQS(msg):
    if not conn: 
        connect_sqs()

    try:
        queue_name = "kombu"
        message= msg
        producer = conn.Producer(serializer='json')
        producer.publish(message,routing_key=queue_name)

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