import json
import boto3

sqsClient = boto3.client("sqs")

def lambda_handler(event, context):
    responce = sqsClient.send_message(
        QueueUrl= "https://sqs.us-west-2.amazonaws.com/612449668280/myDemoSQS",
        MessageBody= json.dumps(event["body"]))
    return {
        'statusCode': 200,
        'body': json.dumps(event["body"])
    }
    