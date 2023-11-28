import pika
from Inspector import Inspector
import time

conn = None
channel = None

def connect_rabbitmq(host):
    parameters = pika.URLParameters(host)
    conn =  pika.BlockingConnection(parameters)
    channel = conn.channel()
   
def send_to_rabbitmq(msg,host):
        connect_rabbitmq(host)
    try:
        channel.basic_publish(exchange='', routing_key='hello',body=msg)
        channel.close()
    except Exception as e:
        raise(e)

def handler(event,context):
    inspector = Inspector()
    inspector.inspectContainer()
    msg = event['msg']
    host = event['host']
      # for warm containers
    if not sleep:
        send_to_rabbitmq(msg,host)
     # setting up sleep value to  warm multiple containers 
    else:
        time.sleep(sleep)
    return inspector.finish()