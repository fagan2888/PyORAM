__all__ = ("Boto3S3Wrapper",
           "MockBoto3S3Wrapper")
import os
import shutil

try:
    import boto3
    import botocore
    boto3_available = True
except:                                                # pragma: no cover
    boto3_available = False                            # pragma: no cover

from six.moves import map

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
            raise ImportError(                         # pragma: no cover
                "boto3 module is required to "         # pragma: no cover
                "use BlockStorageS3 device")           # pragma: no cover

        self._s3 = boto3.session.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name).resource('s3')
        self._bucket = self._s3.Bucket(bucket_name)

    def exists(self, key):
        try:
            self._bucket.Object(key).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                pass
            else:
                raise e
        else:
            return True
        # It's not a file. Check if it's a "directory".
        for obj in self._bucket.objects.filter(
                Prefix=key+"/"):
            return True
        return False

    def download(self, key):
        try:
            return self._s3.meta.client.get_object(
                Bucket=self._bucket.name,
                Key=key)['Body'].read()
        except botocore.exceptions.ClientError:
            raise IOError("Can not download key: %s"
                          % (key))

    def upload(self, key_block):
        key, block = key_block
        self._bucket.put_object(Key=key, Body=block)

    def clear(self, key, threadpool=None):
        _del = lambda obj: \
            self._s3.Object(self._bucket.name, obj.key).delete()
        objs = self._bucket.objects.filter(Prefix=key+"/")
        if threadpool is not None:
            deliter = threadpool.imap(_del, objs)
        else:
            deliter = map(_del, objs)
        for _ in deliter:
            pass

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

    # called within upload to create directory
    # heirarchy on the fly
    def _makedirs_if_needed(self, key):
        if not os.path.exists(
                os.path.dirname(
                    os.path.join(self._bucket_name, key))):
            os.makedirs(
                os.path.dirname(
                    os.path.join(self._bucket_name, key)))
        assert not os.path.isdir(
            os.path.join(self._bucket_name, key))

    def exists(self, key):
        return os.path.exists(
            os.path.join(self._bucket_name, key))

    def download(self, key):
        with open(os.path.join(self._bucket_name, key), 'rb') as f:
            return f.read()

    def upload(self, key_block):
        key, block = key_block
        self._makedirs_if_needed(key)
        with open(os.path.join(self._bucket_name, key), 'wb') as f:
            f.write(block)

    def clear(self, key, threadpool=None):
        if os.path.exists(
                os.path.join(self._bucket_name, key)):
            if os.path.isdir(
                    os.path.join(self._bucket_name, key)):
                shutil.rmtree(
                    os.path.join(self._bucket_name, key),
                    ignore_errors=True)
            else:
                os.remove(
                    os.path.join(self._bucket_name, key))
