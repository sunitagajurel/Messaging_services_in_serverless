# Serverless_producer_consumer_model
This project downloads the csv file from s3 bucket  into lambda function which pushe each line in messaging services like SQS, RabbitMQ and Kafka 


1. clone the repo
2.  cd to the diectory of messaging service you want to work with 
4.  change your directory to env/lib/site-packages
5.  zip the site-packages ( zip -r <file.zip> .) 
6.  upload the zip file  to aws lambda   (aws lambda update-function-code --function-name <function_name>  --zip-file fileb://<zip_file_name> 
7.  run test_parallel.py inside test folder 

