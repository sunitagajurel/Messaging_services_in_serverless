from Inspector import Inspector
from confluent_kafka import Producer
import time

conn = None

def connect_kafka(host):
    global conn
    conf = {
            'bootstrap.servers':host,
            'security.protocol': 'SASL_SSL',
            'sasl.username': "alice",
            'sasl.password': "alice-secret",
            'sasl.mechanism': 'SCRAM-SHA-512'
        } 
    conn = Producer(**conf)

def send_to_kafka(msg,host):
    if not conn: 
        connect_kafka(host)
    try:
        conn.produce("test",msg)
        conn.flush()
    except Exception as e:
        raise(e)
   
def handler(event,context):
    inspector = Inspector()
    inspector.inspectContainer()
    msg = event['msg']
    sleep = event['sleep'] if "sleep" in event else 0
     # for warm containers
    if not sleep:
        send_to_kafka(msg,event['host'])
     # setting up sleep value to  warm multiple containers 
    else:
        time.sleep(sleep)
    return inspector.finish()