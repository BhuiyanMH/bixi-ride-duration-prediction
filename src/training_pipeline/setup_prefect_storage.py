from prefect.filesystems import S3
import os

AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_S3_DIRECTORY = os.environ.get("AWS_S3_DIRECTORY")

# if AWS_ACCESS_KEY is None or AWS_SECRET_ACCESS_KEY is None or AWS_S3_DIRECTORY is None:
if None in (AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY, AWS_S3_DIRECTORY):
    raise ValueError("AWS configuration is not set in the environment variables")

block = S3(
    bucket_path=AWS_S3_DIRECTORY,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

try:
    block.save("bixi-s3-block")
except ValueError as ve:
    print(ve)
