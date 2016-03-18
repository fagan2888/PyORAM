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
    def blockcount(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    def blocksize(self, *args, **kwds):
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
        self._blocksize, self._blockcount = \
            struct.unpack(
                self._index_storage_string,
                self._f.read(self._index_offset))

    #
    # Define BlockStorageInterface Methods
    #

    @classmethod
    def setup(cls,
              filename,
              blocksize,
              blockcount,
              ignore_existing=False):
        if (not ignore_existing) and \
           os.path.exists(filename):
            raise ValueError(
                "Storage location already exists: %s"
                % (filename))
        if (blocksize <= 0) or (blocksize != int(blocksize)):
            raise ValueError(
                "Blocksize must be a positive integer: %s"
                % (blocksize))
        if (blockcount <= 0) or (blockcount != int(blockcount)):
            raise ValueError(
                "Blockcount must be a positive integer: %s"
                % (blockcount))
        zeros = bytes(bytearray(blocksize))
        with open(filename, "wb") as f:
            # create_index
            f.write(struct.pack(cls._index_storage_string,
                                blocksize,
                                blockcount))
            for i in xrange(blockcount):
                f.write(zeros)
            f.flush()

    @property
    def blockcount(self): return self._blockcount
    @property
    def blocksize(self): return self._blocksize
    @property
    def filename(self): return self._filename

    def close(self):
        if self._f is not None:
            self._f.close()
            self._f = None

    def read_blocks(self, indices):
        blocks = []
        for i in indices:
            self._f.seek(self._index_offset + i * self.blocksize)
            blocks.append(self._f.read(self.blocksize))
        return blocks
    def read_block(self, i):
        self._f.seek(self._index_offset + i * self.blocksize)
        return self._f.read(self.blocksize)

    def write_blocks(self, indices, blocks):
        for i, block in zip(indices, blocks):
            self._f.seek(self._index_offset + i * self.blocksize)
            assert len(block) == self.blocksize
            self._f.write(block)
    def write_block(self, i, block):
        self._f.seek(self._index_offset + i * self.blocksize)
        assert len(block) == self.blocksize
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
            self._mm.seek(self._index_offset + i * self.blocksize)
            blocks.append(self._mm.read(self.blocksize))
        return blocks
    def read_block(self, i):
        return self._mm[self._index_offset + i*self.blocksize : \
                        self._index_offset + (i+1)*self.blocksize]

    def write_blocks(self, indices, blocks):
        for i, block in zip(indices, blocks):
            self._mm[self._index_offset + i*self.blocksize : \
                     self._index_offset + (i+1)*self.blocksize] = block
    def write_block(self, i, block):
        self._mm[self._index_offset + i*self.blocksize : \
                 self._index_offset + (i+1)*self.blocksize] = block

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
        self._blocksize, self._blockcount = \
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
              blocksize,
              blockcount,
              bucket_name,
              access_key_id=None,
              secret_access_key=None,
              region=None,
              ignore_existing=False):
        if (blocksize <= 0) or (blocksize != int(blocksize)):
            raise ValueError(
                "Blocksize must be a positive integer: %s"
                % (blocksize))
        if (blockcount <= 0) or (blockcount != int(blockcount)):
            raise ValueError(
                "Blockcount must be a positive integer: %s"
                % (blockcount))

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
                                           blocksize,
                                           blockcount))
        zeros = bytes(bytearray(blocksize))
        basename = filename+"/b%d"
        for i in xrange(blockcount):
            bucket.put_object(Key=basename % i,
                              Body=zeros)

    @property
    def blockcount(self):
        return self._blockcount
    @property
    def blocksize(self):
        return self._blocksize
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
