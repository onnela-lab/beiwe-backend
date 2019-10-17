import boto3
from config.settings import BEIWE_SERVER_AWS_SECRET_ACCESS_KEY, BEIWE_SERVER_AWS_ACCESS_KEY_ID

regions = [
    'us-east-2',
    'us-east-1',
    'us-west-1',
    'us-west-2',
    'ap-northeast-1',
    'ap-northeast-2',
    #'ap-northeast-3', # Only available in conjunction with the Asia Pacific (Tokyo) Region
    'ap-south-1',
    'ap-southeast-1',
    'ap-southeast-2',
    'ca-central-1',
    'cn-north-1',
    'cn-northwest-1',
    'eu-central-1',
    'eu-west-1',
    'eu-west-2',
    'eu-west-3',
    'sa-east-1',
]

for region in regions:
    conn = boto3.client('s3',
                        aws_access_key_id=BEIWE_SERVER_AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=BEIWE_SERVER_AWS_SECRET_ACCESS_KEY,
                        region_name=region)

    try:
        conn.list_buckets()
        print("S3 exists in region:",region)
    except Exception as e:
        print(e)
        print(region,"had an error")
