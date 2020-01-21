import boto3

from kleenlogger import kleenlogger


class S3Service:
    def __init__(self):
        self.service = None
        self.bucket = None

    def init_service(self, aws_key, aws_secret, bucket):
        kleenlogger.logger.info('Initializing S3 service')
        self.service = boto3.client('s3', aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
        self.bucket = bucket
        kleenlogger.logger.info('S3 service initialization complete')

    def upload_to_s3(self, name, path, body):
        s3path = path + name
        kleenlogger.logger.info('Uploading file {} at path {} in the bucket {}'.format(name, s3path, self.bucket))
        self.service.put_object(Body=body, Bucket=self.bucket, Key=s3path)


s3 = S3Service()
