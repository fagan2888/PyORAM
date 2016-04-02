__all__ = ('BlockStorageInterface',
           'BlockStorageFile'
           'BlockStorageMMapFile',
           'BlockStorageS3')

import os
import mmap
import struct
from multiprocessing.pool import ThreadPool

import boto3
import botocore

from six.moves import xrange

class BlockStorageInterface(object):

    def __enter__(self):
        return self
    def __exit__(self, *args):
        self.close()

    #
    # Abstract Interface
    #

    @classmethod
    def setup(cls, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    def block_count(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    def block_size(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    def filename(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    def close(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    def read_blocks(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    def read_block(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    def write_blocks(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    def write_block(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover

class BlockStorageFile(BlockStorageInterface):

    _index_storage_string = "!QQ"
    _index_offset = struct.calcsize(_index_storage_string)

    def __init__(self, filename):
        if not os.path.exists(filename):
            raise IOError("Storage location does not exist: %s"
                          % (filename))
        self._f = None
        self._filename = filename
        self._f = open(self.filename, "r+b")
        self._f.seek(0)
        self._block_size, self._block_count = \
            struct.unpack(
                self._index_storage_string,
                self._f.read(self._index_offset))

    #
    # Define BlockStorageInterface Methods
    #

    @classmethod
    def setup(cls,
              filename,
              block_size,
              block_count,
              initialize=None,
              ignore_existing=False):
        if (not ignore_existing) and \
           os.path.exists(filename):
            raise ValueError(
                "Storage location already exists: %s"
                % (filename))
        if (block_size <= 0) or (block_size != int(block_size)):
            raise ValueError(
                "Block size must be a positive integer: %s"
                % (block_size))
        if (block_count <= 0) or (block_count != int(block_count)):
            raise ValueError(
                "Block count must be a positive integer: %s"
                % (block_count))
        if initialize is None:
            zeros = bytes(bytearray(block_size))
            initialize = lambda i: zeros
        with open(filename, "wb") as f:
            # create_index
            f.write(struct.pack(cls._index_storage_string,
                                block_size,
                                block_count))
            for i in xrange(block_count):
                block = initialize(i)
                assert len(block) == block_size
                f.write(block)
            f.flush()

    @property
    def block_count(self):
        return self._block_count

    @property
    def block_size(self):
        return self._block_size

    @property
    def filename(self):
        return self._filename

    def close(self):
        if self._f is not None:
            self._f.close()
            self._f = None

    def read_blocks(self, indices):
        blocks = []
        for i in indices:
            self._f.seek(self._index_offset + i * self.block_size)
            blocks.append(self._f.read(self.block_size))
        return blocks

    def read_block(self, i):
        self._f.seek(self._index_offset + i * self.block_size)
        return self._f.read(self.block_size)

    def write_blocks(self, indices, blocks):
        for i, block in zip(indices, blocks):
            self._f.seek(self._index_offset + i * self.block_size)
            assert len(block) == self.block_size
            self._f.write(block)

    def write_block(self, i, block):
        self._f.seek(self._index_offset + i * self.block_size)
        assert len(block) == self.block_size
        self._f.write(block)

class BlockStorageMMapFile(BlockStorageFile):

    def __init__(self, *args, **kwds):
        super(BlockStorageMMapFile, self).__init__(*args, **kwds)
        self._mm = None
        self._mm = mmap.mmap(self._f.fileno(), 0)

    #
    # Define BlockStorageInterface Methods
    # (override what is defined on BlockStorageFile)

    def close(self):
        if self._mm is not None:
            self._mm.close()
            self._mm = None
        super(BlockStorageMMapFile, self).close()

    def read_blocks(self, indices):
        blocks = []
        for i in indices:
            self._mm.seek(self._index_offset + i * self.block_size)
            blocks.append(self._mm.read(self.block_size))
        return blocks

    def read_block(self, i):
        return self._mm[self._index_offset + i*self.block_size : \
                        self._index_offset + (i+1)*self.block_size]

    def write_blocks(self, indices, blocks):
        for i, block in zip(indices, blocks):
            self._mm[self._index_offset + i*self.block_size : \
                     self._index_offset + (i+1)*self.block_size] = block

    def write_block(self, i, block):
        self._mm[self._index_offset + i*self.block_size : \
                 self._index_offset + (i+1)*self.block_size] = block

class BlockStorageS3(BlockStorageInterface):

    _index_name = "/PyORAMBlockStorageS3_index.txt"
    _index_storage_string = "!QQ"
    _index_offset = struct.calcsize(_index_storage_string)

    def __init__(self,
                 filename,
                 bucket_name,
                 access_key_id=None,
                 secret_access_key=None,
                 region=None,
                 threadpool_size=4):

        self._filename = filename
        self._bucket_name = bucket_name
        self._access_key_id = access_key_id
        self._secret_access_key = secret_access_key
        self._region = region
        self._threadpool_size = threadpool_size
        self._s3 = boto3.session.Session(
            aws_access_key_id=self._access_key_id,
            aws_secret_access_key=self._secret_access_key,
            region_name=self._region).resource('s3')
        self._bucket = self._s3.Bucket(self._bucket_name)
        self._basename = self.filename+"/b%d"
        self._pool = ThreadPool(processes=self._threadpool_size)
        self._upload = lambda key_block: \
                       self._bucket.put_object(
                           Key=self._basename % key_block[0],
                           Body=key_block[1])
        self._download = lambda key: \
                         self._s3.meta.client.get_object(
                             Bucket=self._bucket.name,
                             Key=self._basename % key)['Body'].read()
        self._async_write = None
        self._block_size, self._block_count = \
            struct.unpack(
                self._index_storage_string,
                self._s3.meta.client.get_object(
                    Bucket=self._bucket.name,
                    Key=self._filename+self._index_name)['Body'].read())

    def _check_async(self):
        if self._async_write is not None:
            self._async_write.wait()
            self._async_write = None

    #
    # Define BlockStorageInterface Methods
    #

    @classmethod
    def setup(cls,
              filename,
              block_size,
              block_count,
              bucket_name,
              access_key_id=None,
              secret_access_key=None,
              region=None,
              initialize=None,
              ignore_existing=False):
        if (block_size <= 0) or (block_size != int(block_size)):
            raise ValueError(
                "Block size must be a positive integer: %s"
                % (block_size))
        if (block_count <= 0) or (block_count != int(block_count)):
            raise ValueError(
                "Block count must be a positive integer: %s"
                % (block_count))

        s3 = boto3.session.Session(
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region).resource('s3')
        bucket = s3.Bucket(bucket_name)
        bucket.creation_date
        try:
            bucket.Object(filename+cls._index_name).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                exists = False
            else:
                raise e
        else:
            exists = True
        if (not ignore_existing) and exists:
            raise ValueError(
                "Storage location already exists in bucket %s: %s"
                % (bucket_name, filename))

        bucket.put_object(Key=filename+cls._index_name,
                          Body=struct.pack(cls._index_storage_string,
                                           block_size,
                                           block_count))
        if initialize is None:
            zeros = bytes(bytearray(block_size))
            initialize = lambda i: zeros
        basename = filename+"/b%d"
        for i in xrange(block_count):
            block = initialize(i)
            assert len(block) == block_size
            bucket.put_object(Key=basename % i,
                              Body=block)

    @property
    def block_count(self):
        return self._block_count

    @property
    def block_size(self):
        return self._block_size

    @property
    def filename(self):
        return self._filename

    def close(self):
        self._check_async()

    def read_blocks(self, indices):
        self._check_async()
        return self._pool.map(self._download, indices)

    def read_block(self, i):
        self._check_async()
        return self._download(i)

    def write_blocks(self, indices, blocks):
        self._check_async()
        self._async_write = \
            self._pool.map_async(self._upload,
                                 zip(indices, blocks))

    def write_block(self, i, block):
        self._check_async()
        self._upload((i, block))
