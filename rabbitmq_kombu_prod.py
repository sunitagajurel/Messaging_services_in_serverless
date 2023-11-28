from kombu import Connection
from Inspector import Inspector
import time

conn = None

def connect_rabbitmq(host):
    global conn
    conn = Connection(host)
    
def send_to_rabbitmq(msg,host):
    if not conn: 
        connect_rabbitmq(host)
    try:
        queue_name = "test"
        message = msg 
        producer = conn.Producer(serializer='json')
        producer.publish(msg,routing_key=queue_name)
    except Exception as e:
        raise(e)

def handler(event,context):
    inspector = Inspector()
    inspector.inspectContainer()
    msg = event['msg']
    host= event['host']
     # for warm containers
    if not sleep:
        send_to_rabbitmq(msg,host)
     # setting up sleep value to  warm multiple containers 
    else:
        time.sleep(sleep)
    return inspector.finish()