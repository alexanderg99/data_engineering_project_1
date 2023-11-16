import boto3

# Create an S3 client
s3 = boto3.client('s3')


# Specify the local file path and S3 bucket and object name
local_file_path = 'steam_new.json'
bucket_name = 'gaming-project-bucket'
object_name = 'gaming/gaming_data.json'

# Upload the file
#s3.upload_file(local_file_path, bucket_name, object_name)
s3.upload_file(local_file_path, bucket_name, object_name, ExtraArgs={'ACL':'public-read'})
