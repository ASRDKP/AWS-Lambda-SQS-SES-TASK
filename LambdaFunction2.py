import boto3
from botocore.exceptions import ClientError
import pandas as pd
import os

def send_email(event, context):

    print("Event :",event)
    records = event['Records']
    
    list1 = []
    
    for record in records:
        body = record['body']
        print("Body :",body)
        list1.append(body)
        
    print("List :", list1)
    
    SENDER = os.environ['source']
    RECIPIENT = os.environ['destination']

    AWS_REGION = os.environ['region']

    SUBJECT = "This is test email from aws Lambda2 for testing purpose..!!"
    
    BODY_TEXT = ("Hey There...\r\n"
            "This email is sent from Amazon SES using the Lambda2"
            "The data send from the sqs is :- "
            )

    CHARSET = "UTF-8" 

    # Create a new SES resource and specify a region.
    client = boto3.client('ses',region_name=os.environ['region'])

    try:
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    # 'satyam.s@consultadd.com'
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Text': {
        
                        'Data': BODY_TEXT + " " + str(list1)
                    },
                },
                'Subject': {

                    'Data': SUBJECT
                },
            },
            Source=SENDER
            # Source = 'prajapat7401rahul@gmail.com'
        )
        
        print("Email is sent successfully")
        
    except ClientError as e:
        print('Error Message :',e.response['Error']['Message'])
    else:
        print("Email sent!!!"),



def lambda_handler(event, context):
    # TODO implement
    send_email(event, context)


