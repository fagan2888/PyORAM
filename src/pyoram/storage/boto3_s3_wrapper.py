import os

try:
    import boto3
    import botocore
    boto3_available = True
except:
    boto3_available = False

class Boto3S3Wrapper(object):
    """
    A wrapper class for the boto3 S3 service.
    """

    def __init__(self,
                 bucket_name,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 region_name=None):
        if not boto3_available:
            raise ImportError("boto3 module is required to "
                              "use BlockStorageS3 device")

        self._s3 = boto3.session.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region).resource('s3')
        self._bucket = self._s3.Bucket(bucket_name)

    def exists(self, name):
        try:
            self._bucket.Object(name).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                raise e
        else:
            return True

    def download(self, key):
        return self._s3.meta.client.get_object(
            Bucket=self._bucket.name,
            Key=self._basename % key)['Body'].read()

    def upload(self, (key, block)):
        self._bucket.put_object(Key=key, Body=block)

    def clear(self):
        for obj in self._bucket.objects.filter(
                Prefix=storage_name+"/"):
            self._s3.Object(bucket.name, obj.key).delete()

class MockBoto3S3Wrapper(object):
    """
    A mock class for Boto3S3Wrapper that uses the local
    filesystem and treats the bucket name as a directory.
    """

    def __init__(self,
                 bucket_name,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 region_name=None):

        self._bucket_name = os.path.abspath(
            os.path.normpath(bucket_name))

    def exists(self, name):
        return os.path.exists(
            os.path.join(self._bucket_name, name))

    def download(self, key):
        with open(os.path.join(self._bucket_name, key), 'rb') as f:
            return f.read()

    def upload(self, (key, block)):
        with open(os.path.join(self._bucket_name, key), 'wb') as f:
            f.write(block)

    def clear(self, name):
        shutil.rmtree(
            os.path.join(self._bucket_name, name),
            ignore_errors=True)
        os.makedirs(
            os.path.join(self._bucket_name, name))
