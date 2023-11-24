import threading
import subprocess
import sys
import json
import codecs
import time
import boto3
import ast
import csv
import os
from collections import defaultdict


#other global variables 
interface = sys.argv[1]
runs = int(sys.argv[2])
threads = int(sys.argv[3])
run_per_thread =  runs//threads
file_path = './data/posts_1.csv'

file = None
lambda_responses = []
threadList = []

functions = {
    'sqs':"SQS_producer",
    "sqs_kombu":"sqs_producer_kombu",
    "rabbitmq": "rabbitmq-producer",
    "kafka":"kafka_producer",
    "rabbitmq_kombu":"rabbitmq_kombu_producer",
    "kafka_kombu":"kafka_kombu_producer" 
}

urls = {
    'sqs':None,
    "rabbitmq":"amqps://admin:AppleBanana123@b-2ae054a6-70b3-45f8-b22f-6cefe0dbc4f1.mq.us-east-2.amazonaws.com:5671",
    "kafka":"b-2.kafkacluster.l2c4kx.c4.kafka.us-east-2.amazonaws.com:9096",
    "sqs_kombu":None,
    "rabbitmq_kombu":"amqps://admin:AppleBanana123@b-2ae054a6-70b3-45f8-b22f-6cefe0dbc4f1.mq.us-east-2.amazonaws.com:5671",
    "kafka_kombu":"confluentkafka://alice:alice-secret@b-2.kafkacluster.l2c4kx.c4.kafka.us-east-2.amazonaws.com:9096"
}

url = urls[interface]
function = functions[interface]
client = boto3.client('lambda')

# invoking lambda function 
def invoke_lambda(payload):
    for i in range(run_per_thread):
        file.seek(0,1)
        payload = {"msg":file.readline(),
                    "host":url,
                    # "sleep":10,
                    "batch_size":1
                    }
        try: 
            response = client.invoke(
                FunctionName=function,
                InvocationType='RequestResponse',
                Payload= json.dumps(payload)
            )
        # storing the response in a file 
            ans = response['Payload'].read().decode('utf-8')
            jsonDict = ast.literal_eval(ans)
            if "errorMessage" in jsonDict:
                print(f'{jsonDict["errorType"]} {jsonDict["stackTrace"]}')
                sys.exit()
            lambda_responses.append(jsonDict)
        
        except Exception as e: 
            raise e

def download_data_from_s3(bucket_name,file_name):
    try:
        # Download the CSV file from S3 to the /tmp directory
        s3 = boto3.client('s3')
        s3.download_file(bucket_name, file_name, file_path)
        print("file downloaded")
    except Exception as e:
        print(f"Error downloading file from S3: {str(e)}")
        sys.exit()

if not os.path.exists(file_path):
    download_data_from_s3("2023capstonedata","posts_1.csv")
    

file = codecs.open(file_path,'r',encoding ='utf-8')
start_time = time.time()
# uisng threads 
for i in range(threads):     
    thread = threading.Thread(target=invoke_lambda,args=(i,))
    thread.start()
    threadList.append(thread)
       
for i in range(len(threadList)):
    threadList[i].join()

end_time = time.time()

if lambda_responses:
    output_file = f'./json_outputs/{interface}_{runs}_{threads}.json'
    with open(output_file, 'w') as file:
        json.dump(lambda_responses,file)

    with open(output_file, 'r') as file:
        lambda_responses = json.load(file)

    # Extract keys for CSV header from the first JSON response
    csv_header = lambda_responses[0].keys()
    uuids = defaultdict(int)
    # Specify the CSV file name
    csv_file_name = f'./csv_outputs/{interface}_{runs}_{threads}.csv'

    # Write data to CSV file
    with open(csv_file_name, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_header)
        
        # Write header
        writer.writeheader()
        
        # Write rows
        for response in lambda_responses:
            uuids[response['uuid']] +=1
            writer.writerow(response)
        

    with open(csv_file_name, 'a', newline='') as csvfile:
        csvfile.write(f'total_no of uuids:{len(uuids)}\n')
        csvfile.write(f'uuids: {uuids.keys()}\n')
        csvfile.write(f'total_run_time: {round(end_time-start_time,2)}\n')

print(time.time()-start_time)
