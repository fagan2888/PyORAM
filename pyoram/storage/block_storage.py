__all__ = ('BlockStorageFile'
           'BlockStorageMMapFile',
           'BlockStorageS3')

import os
import mmap
import struct
from multiprocessing.pool import ThreadPool

import boto3
import botocore

import six
from six.moves import xrange

def BlockStorageTypeFactory(storage_type_name):
    if storage_type_name == 'file':
        return BlockStorageFile
    elif storage_type_name == 'mmap':
        return BlockStorageMMapFile
    elif storage_type_name == 's3':
        return BlockStorageS3
    else:
        raise ValueError(
            "BlockStorageTypeFactory: Unsupported storage "
            "type: %s" % (storage_type_name))

class BlockStorageInterface(object):

    def __enter__(self):
        return self
    def __exit__(self, *args):
        self.close()

    #
    # Abstract Interface
    #

    @classmethod
    def compute_storage_size(cls, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    @classmethod
    def setup(cls, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    @property
    def header_data(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    @property
    def block_count(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    @property
    def block_size(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    @property
    def storage_name(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    def update_header_data(self, *args, **kwds):
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

    _index_storage_string = "!LLL"
    _index_offset = struct.calcsize(_index_storage_string)

    def __init__(self, storage_name):
        if not os.path.exists(storage_name):
            raise IOError("Storage location does not exist: %s"
                          % (storage_name))
        self._f = None
        self._storage_name = storage_name
        self._f = open(self.storage_name, "r+b")
        self._f.seek(0)
        self._block_size, self._block_count, user_header_size = \
            struct.unpack(
                self._index_storage_string,
                self._f.read(self._index_offset))
        self._user_header_data = bytes()
        if user_header_size > 0:
            self._user_header_data = \
                self._f.read(user_header_size)
        self._header_offset = self._index_offset + \
                              len(self._user_header_data)

    #
    # Define BlockStorageInterface Methods
    #
    @classmethod
    def compute_storage_size(cls,
                             block_size,
                             block_count,
                             header_data=None,
                             ignore_header=False):
        assert (block_size > 0) and (block_size == int(block_size))
        assert (block_count > 0) and (block_count == int(block_count))
        if header_data is None:
            header_data = bytes()
        if ignore_header:
            return block_size * block_count
        else:
            return cls._index_offset + \
                   len(header_data) + \
                   block_size * block_count

    @classmethod
    def setup(cls,
              storage_name,
              block_size,
              block_count,
              initialize=None,
              header_data=None,
              ignore_existing=False):
        if (not ignore_existing) and \
           os.path.exists(storage_name):
            raise ValueError(
                "Storage location already exists: %s"
                % (storage_name))
        if (block_size <= 0) or (block_size != int(block_size)):
            raise ValueError(
                "Block size (bytes) must be a positive integer: %s"
                % (block_size))
        if (block_count <= 0) or (block_count != int(block_count)):
            raise ValueError(
                "Block count must be a positive integer: %s"
                % (block_count))
        if (header_data is not None) and \
           (type(header_data) is not bytes):
            raise TypeError(
                "'header_data' must be of type bytes. "
                "Invalid type: %s" % (type(header_data)))

        if initialize is None:
            zeros = bytes(bytearray(block_size))
            initialize = lambda i: zeros
        try:
            with open(storage_name, "wb") as f:
                # create_index
                if header_data is None:
                    f.write(struct.pack(cls._index_storage_string,
                                        block_size,
                                        block_count,
                                        0))
                else:
                    f.write(struct.pack(cls._index_storage_string,
                                        block_size,
                                        block_count,
                                        len(header_data)))
                    f.write(header_data)
                for i in xrange(block_count):
                    block = initialize(i)
                    assert len(block) == block_size, \
                        ("%s != %s" % (len(block), block_size))
                    f.write(block)
                f.flush()
        except:
            os.remove(storage_name)
            raise
        return BlockStorageFile(storage_name)

    @property
    def header_data(self):
        return self._user_header_data

    @property
    def block_count(self):
        return self._block_count

    @property
    def block_size(self):
        return self._block_size

    @property
    def storage_name(self):
        return self._storage_name

    def update_header_data(self, new_header_data):
        if len(new_header_data) != len(self.header_data):
            raise ValueError(
                "The size of header data can not change.\n"
                "Original bytes: %s\n"
                "New bytes: %s" % (len(self.header_data),
                                   len(new_header_data)))
        self._user_header_data = new_header_data
        self._f.seek(self._index_offset)
        self._f.write(self._user_header_data)

    def close(self):
        if self._f is not None:
            try:
                self._f.close()
            except OSError:                            # pragma: no cover
                pass                                   # pragma: no cover
            self._f = None

    def read_blocks(self, indices):
        blocks = []
        for i in indices:
            assert 0 <= i < self.block_count
            self._f.seek(self._header_offset + i * self.block_size)
            blocks.append(self._f.read(self.block_size))
        return blocks

    def read_block(self, i):
        assert 0 <= i < self.block_count
        self._f.seek(self._header_offset + i * self.block_size)
        return self._f.read(self.block_size)

    def write_blocks(self, indices, blocks):
        for i, block in zip(indices, blocks):
            assert 0 <= i < self.block_count
            self._f.seek(self._header_offset + i * self.block_size)
            assert len(block) == self.block_size, \
                ("%s != %s" % (len(block), self.block_size))
            self._f.write(block)

    def write_block(self, i, block):
        assert 0 <= i < self.block_count
        self._f.seek(self._header_offset + i * self.block_size)
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

    @classmethod
    def setup(cls,
              storage_name,
              block_size,
              block_count,
              **kwds):
        f = BlockStorageFile.setup(storage_name,
                                   block_size,
                                   block_count,
                                   **kwds)
        f.close()
        return BlockStorageMMapFile(storage_name)

    def update_header_data(self, new_header_data):
        if len(new_header_data) != len(self.header_data):
            raise ValueError(
                "The size of header data can not change.\n"
                "Original bytes: %s\n"
                "New bytes: %s" % (len(self.header_data),
                                   len(new_header_data)))
        self._user_header_data = new_header_data
        self._mm.seek(self._index_offset)
        self._mm.write(self._user_header_data)

    def close(self):
        if self._mm is not None:
            try:
                self._mm.close()
            except OSError:                            # pragma: no cover
                pass                                   # pragma: no cover
            self._mm = None
        super(BlockStorageMMapFile, self).close()

    def read_blocks(self, indices):
        blocks = []
        for i in indices:
            assert 0 <= i < self.block_count
            self._mm.seek(self._header_offset + i * self.block_size)
            blocks.append(self._mm.read(self.block_size))
        return blocks

    def read_block(self, i):
        assert 0 <= i < self.block_count
        return self._mm[self._header_offset + i*self.block_size : \
                        self._header_offset + (i+1)*self.block_size]

    def write_blocks(self, indices, blocks):
        for i, block in zip(indices, blocks):
            assert 0 <= i < self.block_count
            self._mm[self._header_offset + i*self.block_size : \
                     self._header_offset + (i+1)*self.block_size] = block

    def write_block(self, i, block):
        assert 0 <= i < self.block_count
        self._mm[self._header_offset + i*self.block_size : \
                 self._header_offset + (i+1)*self.block_size] = block

class BlockStorageS3(BlockStorageInterface):

    _index_name = "/PyORAMBlockStorageS3_index.txt"
    _header_struct_string = "!LLL"
    _header_offset = struct.calcsize(_header_struct_string)

    def __init__(self,
                 storage_name,
                 bucket_name=None,
                 access_key_id=None,
                 secret_access_key=None,
                 region=None,
                 threadpool_size=4):
        if bucket_name is None:
            raise ValueError("'bucket_name' is required")
        self._storage_name = storage_name
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
        self._basename = self.storage_name+"/b%d"
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
        index_data = self._s3.meta.client.get_object(
            Bucket=self._bucket.name,
            Key=self._storage_name+self._index_name)['Body']
        self._block_size, self._block_count, user_header_size = \
            struct.unpack(
                self._header_struct_string,
                index_data.read(self._header_offset))
        self._user_header_data = bytes()
        if user_header_size > 0:
            self._user_header_data = \
                index_data.read(user_header_size)

    def _check_async(self):
        if self._async_write is not None:
            self._async_write.wait()
            self._async_write = None

    #
    # Define BlockStorageInterface Methods
    #

    @classmethod
    def compute_storage_size(cls,
                             block_size,
                             block_count,
                             header_data=None,
                             ignore_header=False):
        assert (block_size > 0) and (block_size == int(block_size))
        assert (block_count > 0) and (block_count == int(block_count))
        if header_data is None:
            header_data = bytes()
        if ignore_header:
            return block_size * block_count
        else:
            return self._header_offset + \
                    len(header_data) + \
                    block_size * block_count

    @classmethod
    def setup(cls,
              storage_name,
              block_size,
              block_count,
              bucket_name=None,
              access_key_id=None,
              secret_access_key=None,
              region=None,
              header_data=None,
              initialize=None,
              threadpool_size=4,
              ignore_existing=False):
        if bucket_name is None:
            raise ValueError("'bucket_name' is required")
        if (block_size <= 0) or (block_size != int(block_size)):
            raise ValueError(
                "Block size (bytes) must be a positive integer: %s"
                % (block_size))
        if (block_count <= 0) or (block_count != int(block_count)):
            raise ValueError(
                "Block count must be a positive integer: %s"
                % (block_count))
        if (header_data is not None) and \
           (type(header_data) is not bytes):
            raise TypeError(
                "'header_data' must be of type bytes. "
                "Invalid type: %s" % (type(header_data)))

        s3 = boto3.session.Session(
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region).resource('s3')
        bucket = s3.Bucket(bucket_name)
        bucket.creation_date
        try:
            bucket.Object(storage_name+cls._index_name).load()
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
                % (bucket_name, storage_name))

        if header_data is None:
            bucket.put_object(Key=storage_name+cls._index_name,
                              Body=struct.pack(cls._header_struct_string,
                                               block_size,
                                               block_count,
                                               0))
        else:
            bucket.put_object(Key=storage_name+cls._index_name,
                              Body=struct.pack(cls._header_struct_string,
                                               block_size,
                                               block_count,
                                               len(header_data)) + \
                                               header_data)

        if initialize is None:
            zeros = bytes(bytearray(block_size))
            initialize = lambda i: zeros
        basename = storage_name+"/b%d"
        for i in xrange(block_count):
            block = initialize(i)
            assert len(block) == block_size
            bucket.put_object(Key=basename % i,
                              Body=block)
        return BlockStorageS3(storage_name,
                              bucket_name=bucket_name,
                              access_key_id=access_key_id,
                              secret_access_key=secret_access_key,
                              region=region,
                              threadpool_size=threadpool_size)

    @property
    def header_data(self):
        return self._user_header_data

    @property
    def block_count(self):
        return self._block_count

    @property
    def block_size(self):
        return self._block_size

    @property
    def storage_name(self):
        return self._storage_name

    def update_header_data(self, new_header_data):
        if len(new_header_data) != len(self.header_data):
            raise ValueError(
                "The size of header data can not change.\n"
                "Original bytes: %s\n"
                "New bytes: %s" % (len(self.header_data),
                                   len(new_header_data)))
        self._user_header_data = new_header_data

        index_data = bytearray(self._s3.meta.client.get_object(
            Bucket=self._bucket.name,
            Key=self._storage_name+self._index_name)['Body'].read())
        lenbefore = len(index_data)
        index_data[self._header_offset:] = new_header_data
        assert lenbefore == len(index_data)
        self._bucket.put_object(
            Key=self._storage_name+self._index_name,
            Body=bytes(index_data))

    def close(self):
        self._check_async()

    def read_blocks(self, indices):
        # be sure not to exhaust this if it is an iterator
        # or generator
        indices = list(indices)
        assert all(0 <= i <= self.block_count for i in indices)
        self._check_async()
        return self._pool.map(self._download, indices)

    def read_block(self, i):
        assert 0 <= i < self.block_count
        self._check_async()
        return self._download(i)

    def write_blocks(self, indices, blocks):
        # be sure not to exhaust this if it is an iterator
        # or generator
        indices = list(indices)
        assert all(0 <= i <= self.block_count for i in indices)
        self._check_async()
        self._async_write = \
            self._pool.map_async(self._upload,
                                 zip(indices, blocks))

    def write_block(self, i, block):
        assert 0 <= i < self.block_count
        self._check_async()
        self._upload((i, block))
