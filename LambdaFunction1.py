import json
import boto3
import pandas as pd
import csv
import urllib

s3Client = boto3.client("s3")
sqsClient = boto3.client("sqs")


def lambda_handler(event, context):
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        print("Bucket name :", bucket)
        
        key1 = event['Records'][0]['s3']['object']['key']
        key = urllib.parse.unquote_plus(key1, encoding='utf-8')
        
        print("The file name :", key1)
        
        responce1 = s3Client.get_object(Bucket=bucket, Key=key)
        print("The Responce1 :", responce1)
        
        content = responce1['Body'].read().decode('utf-8').splitlines()
        print("Content :", content)

        if key1[-3:] == "csv":
            print("Inside the lambda_handler if block")
            # convert_csv_to_json(content, newfilename)
             
            responce = sqsClient.send_message(
            QueueUrl= "https://sqs.us-west-2.amazonaws.com/612449668280/myDemoSQS",
            MessageBody= json.dumps(content)
            )
           
        
    except Exception as e:
        print("Some error occured in the lambda function 1. Error_Message :", e)
        df = {
            "Error_Message" : e,
            "Status_Code" : 400
        }
        return df
    
    
    
# def convert_csv_to_json(content, filename):
#     try:
#         filename = filename + '.json'
#         print("FileName in convert_csv_to_json :", filename)
        
#         print("Content in convert_csv_to_json :", content)
#         print("Type of Content :", type(content))
        
       
#         df = pd.DataFrame(content)
        
#         print("DF :", df)
#         print("Type of DF ", type(df))
        
        
#         responce = sqsClient.send_message(
#             QueueUrl= "https://sqs.us-west-2.amazonaws.com/612449668280/myDemoSQS",
#             MessageBody= json.dumps(content)
#             )
        
#         print("Responce is working")
        
#     except Exception as e:
#         df = {
#             "Error_Message" : e,
#             "Status_Code" : 400
#         }
#         return df
        












































########################################################################
#Day 2

# import json
# import boto3
# import pandas as pd
# import csv
# import urllib

# s3Client = boto3.client("s3")
# sqsClient = boto3.client("sqs")


# def lambda_handler(event, context):
#     try:
#         bucket = event['Records'][0]['s3']['bucket']['name']
#         print("Bucket name :", bucket)
        
#         key1 = event['Records'][0]['s3']['object']['key']
#         key = urllib.parse.unquote_plus(key1, encoding='utf-8')
        
#         print("The file name :", key1)
        
#         responce1 = s3Client.get_object(Bucket=bucket, Key=key)
#         print("The Responce1 :", responce1)
        
#         content = responce1['Body'].read().decode('utf-8').splitlines()
#         print("Content :", content)
        
#         newfilename = key1[:-4:]
#         print("File name without extension :", newfilename)
        
#         if key1[-3:] == "csv":
#             print("Inside the lambda_handler if block")
#             convert_csv_to_json(content, newfilename)
           
#             # filename = newfilename + '.json'
#             # print("FileName in convert_csv_to_json :", filename)
            
#             # print("Content in convert_csv_to_json :", content)
#             # print("Type of Content :", type(content))
            
           
#             # df = pd.DataFrame(content)
            
#             # print("DF :", df)
#             # print("Type of DF ", type(df))
           
        
#     except Exception as e:
#         print("Some error occured in the lambda function 1. Error_Message :", e)
#         df = {
#             "Error_Message" : e,
#             "Status_Code" : 400
#         }
#         return df
    
    
    
# def convert_csv_to_json(content, filename):
#     try:
#         filename = filename + '.json'
#         print("FileName in convert_csv_to_json :", filename)
        
#         print("Content in convert_csv_to_json :", content)
#         print("Type of Content :", type(content))
        
       
#         df = pd.DataFrame(content)
        
#         print("DF :", df)
#         print("Type of DF ", type(df))
        
#         # file = df.to_json('/tmp/' + filename)
        
#         # print("Type of file",type(file))
#         # print("File is successfully uploaded.")
        
#         print("%^%^%%^%%")
        
        
        
#         responce = sqsClient.send_message(
#             QueueUrl= "https://sqs.us-west-2.amazonaws.com/612449668280/myDemoSQS",
#             MessageBody= json.dumps(content)
#             )
        
#         # print("Responce :",responce["MessageBody"])   
#         print("Responce is working")
        
#     except Exception as e:
#         df = {
#             "Error_Message" : e,
#             "Status_Code" : 400
#         }
#         return df
        
        
        
        
        
        
        
        
        
        
        
        
        
     
        
        
        
        

# ################################################################
# # Day-1
# import json
# import boto3
# import pandas as pd
# import csv
# import urllib

# s3Client = boto3.client("s3")

# def lambda_handler(event, context):
#     try:
#         bucket = event['Records'][0]['s3']['bucket']['name']
#         print("Bucket name :", bucket)
        
#         key1 = event['Records'][0]['s3']['object']['key']
#         key = urllib.parse.unquote_plus(key1, encoding='utf-8')
        
#         print("The file name :", key1)
        
#         responce = s3Client.get_object(Bucket=bucket, Key=key)
#         print("The Responce :", responce)
        
#         content = responce['Body'].read().decode('utf-8').splitlines()
#         print("Content :", content)
        
#         newfilename = key1[:-4:]
#         print("File name without extension :", newfilename)
        
#         if key1[-3:] == "csv":
#             print("Inside the lambda_handler if block")
#             convert_csv_to_json(content, newfilename)
           
#             # filename = newfilename + '.json'
#             # print("FileName in convert_csv_to_json :", filename)
            
#             # print("Content in convert_csv_to_json :", content)
#             # print("Type of Content :", type(content))
            
           
#             # df = pd.DataFrame(content)
            
#             # print("DF :", df)
#             # print("Type of DF ", type(df))
           
        
#     except Exception as e:
#         print("Some error occured in the lambda function 1. Error_Message :", e)
#         df = {
#             "Error_Message" : e,
#             "Status_Code" : 400
#         }
#         return df
    
    
    
# def convert_csv_to_json(content, filename):
#     try:
#         filename = filename + '.json'
#         print("FileName in convert_csv_to_json :", filename)
        
#         print("Content in convert_csv_to_json :", content)
#         print("Type of Content :", type(content))
        
       
#         df = pd.DataFrame(content)
        
#         print("DF :", df)
#         print("Type of DF ", type(df))
        
#         # file = df.to_json('/tmp/' + filename)
        
#         # print("Type of file",type(file))
#         # print("File is successfully uploaded.")
        
#         # responce = sqsClient.send_message(
#         #     QueueUrl= "https://sqs.us-west-2.amazonaws.com/612449668280/myDemoSQS",
#         #     MessageBody= json.dumps(df)
        
#     except Exception as e:
#         df = {
#             "Error_Message" : e,
#             "Status_Code" : 400
#         }
#         return df
        


