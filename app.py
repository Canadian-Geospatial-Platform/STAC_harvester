import requests
import os 
import json
import logging
import boto3
from botocore.exceptions import ClientError

BUCKET_NAME = "stac-harvest-json-dev"
#BUCKET_NAME = os.environ['BUCKET_NAME']
BUCKET_LOCATION = "ca-central-1"

def lambda_handler(event, context):
    message = ""

    '''
    Determine what STAC endpoint(s) we are harvesting.
    It is possible to harvest multiple STAC endpoints.
    E.g.,
    Note: The harvest_items function need to be revised to harvest the GeoMat STAC  
    {
       "stac_url": [
                       "https://datacube.services.geo.ca/api/collections/",
                       "https://api.weather.gc.ca/stac/msc-datamart?f=json"
                   ]
    }
    '''
    try:
        STAC_JSON_COLLECTION_URL = event['stac_url']
    except:
        STAC_JSON_COLLECTION_URL = False
    
    if STAC_JSON_COLLECTION_URL == False:
        #default -- harvest the CCMEO Datacube
        STAC_JSON_COLLECTION_URL = "https://datacube.services.geo.ca/api/collections/"
        
    stac_endpoint_list = STAC_JSON_COLLECTION_URL
    
    for stac in stac_endpoint_list:
        print(stac)
        message += harvest_items(stac_json_collection_url=stac, bucket=BUCKET_NAME, bucket_location=BUCKET_LOCATION)
    
    # Response for API gateway 
    response = {
        "statusCode": "200",
        "headers": {"Content-type": "application/json"},
        "body": json.dumps(
            {
                "message": message,
            }
        )
    }
    return response 
    
    
# Check if a bucket exist 
def bucket_exists(bucket_name):
    try:
        s3 = boto3.client("s3")
        response = s3.head_bucket(Bucket=bucket_name)
        print("Bucket exists.", bucket_name)
        exists = True 
    except ClientError as error: 
        error_code = int(error.response['Error']['Code'])
        if error_code == 403:
            print("Private Bucket. Forbidden Access! ", bucket_name)
        elif error_code == 404: 
            print("Bucket Does Not Exist!", bucket_name)
        exists = False 
    return exists 

def create_bucket(bucket_name, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    # Check if bucket exists 
    if bucket_exists(bucket_name):
        s3 =boto3.client("s3")
        response = s3.head_bucket(Bucket=bucket_name)
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            """ Bucket already exists and we have sufficent permissions """
            print("Bucket already exists and we have sufficent permissions")
            return True           
        
    else:     
        # Create bucket
        try:
            if region is None:
                s3_client = boto3.client('s3')
                s3_client.create_bucket(Bucket=bucket_name)
            else:
                s3_client = boto3.client('s3', region_name=region)
                location = {'LocationConstraint': region}
                s3_client.create_bucket(Bucket=bucket_name,CreateBucketConfiguration=location)
        except ClientError as e:
            logging.error(e)
            return False
        return True 

# Upload a json file to S3 
def upload_json_s3(file_name, bucket, json_data, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :param json_data: json data to be uploded 
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)
    # boto3.client vs boto3.resources:https://www.learnaws.org/2021/02/24/boto3-resource-client/ 
    s3 = boto3.resource('s3')
    s3object = s3.Object(bucket, file_name)
    try: 
        response = s3object.put(Body=(bytes(json.dumps(json_data, indent=4, ensure_ascii=False).encode('utf-8'))))
    except ClientError as e:
        logging.error(e)
        return False 
    return True 

# Harvest item level json if giving the collection url  
def harvest_items(stac_json_collection_url, bucket, bucket_location):
    """ Harvests STAC JSON file into s3_bucket_name
    
    :param collection_id: list of STAC collection ids 
    :param stac_json_collection_url: starting base path to the geo.ca datacube STAC collection api
    :param item_id: list of STAC item ids
    :param bucket: bucket to upload to
    :param bucket_location: bucket regions 
    :return: accumulated error messages
    """
    error_msg = None 
    if create_bucket(bucket, bucket_location):
        count = 0 
        # Request collection and get collection id 
        try:
            response = requests.get(stac_json_collection_url)
        except:
            error_msg = "Error trying to access " + stac_json_collection_url
            return error_msg
            
        if response.status_code != 200:
            error_msg = "Source STAC API did not return a HTTP 200 OK"
            return error_msg
        else:
            str_data = json.loads(response.text)
            collections = str_data["collections"]
            collection_id = [l["id"] for l in collections]
            if len(collection_id)!=0:
                print(f"{len(collections)} collection ids are identified. Collection names are {collection_id}")
                for collection in collection_id:
                    try: 
                        response = requests.get(stac_json_collection_url + collection+"/items")
                        str_data = json.loads(response.text) 
                        #print(str_data)
                        features = str_data["features"] 
                        item_id = [l["id"] for l in features]                    
                        if len(item_id)!=0:
                            for item in item_id:
                                print("Request " + item + " from Collection ", collection)
                                api_url=stac_json_collection_url + collection + "/items/" + item
                                response = requests.get(api_url)
                                str_data = json.loads(response.text)
                                #print(str_data)
                                file_name = collection + "_" + item + ".json"
                                if upload_json_s3(file_name, bucket, str_data):
                                    count += 1
                                    print(f"Upload file {file_name} to the bucket {bucket}")
                    except ClientError as e:
                        logging.error(e)
                        error_msg += e
            else: 
                print("Could not find any collections in the provided api")
                error_msg="Could not find any collections in api: " + stac_json_collection_url 
    else: 
        error_msg="Could not create S3 bucket: " + bucket 
    return error_msg