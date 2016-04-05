__all__ = ('EncryptedHeapStorage',)

import struct

from pyoram.storage.virtualheap import (VirtualHeap,
                                        SizedVirtualHeap)
from pyoram.storage.encrypted_block_storage import \
    EncryptedBlockStorage

class HeapStorageInterface(object):

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
    @property
    def user_header_data(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    @property
    def bucket_count(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    @property
    def bucket_size(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    @property
    def blocks_per_bucket(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    @property
    def storage_name(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    @property
    def virtual_heap(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    def close(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    def read_path(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    def write_path(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover

class EncryptedHeapStorage(HeapStorageInterface):

    _index_storage_string = "!QQQ"
    _index_offset = struct.calcsize(_index_storage_string)

    def __init__(self, *args, **kwds):
        self._storage = EncryptedBlockStorage(*args, **kwds)
        base, height, blocks_per_bucket = \
            struct.unpack(
                self._index_storage_string,
                self._storage.\
                user_header_data[:self._index_offset])
        self._vheap = SizedVirtualHeap(
            base,
            height,
            blocks_per_bucket=blocks_per_bucket)

    #
    # Add some new methods
    #

    @property
    def encryption_key(self):
        return self._storage.encryption_key

    @property
    def ciphertext_bucket_size(self):
        return self._storage.ciphertext_block_size

    #
    # Define HeapStorageInterface Methods
    #

    @classmethod
    def setup(cls,
              height=None,
              blocks_per_bucket=None,
              *args,
              **kwds):
        if (height is None):
            raise ValueError("'height' is required")
        if (blocks_per_bucket is None):
            raise ValueError("'blocks_per_bucket' is required")
        block_size = kwds.get('block_size')
        if (block_size is None):
            raise ValueError("'block_size' is required")
        if "block_count" in kwds:
            raise ValueError("'block_count' is not a valid keyword")
        if height < 0:
            raise ValueError(
                "height must be 0 or greater. Invalid value: %s"
                % (height))
        if blocks_per_bucket < 1:
            raise ValueError(
                "blocks_per_bucket must be 1 or greater. Invalid value: %s"
                % (blocks_per_bucket))
        base = kwds.pop('base', 2)
        if base < 2:
            raise ValueError(
                "base must be 2 or greater. Invalid value: %s"
                % (base))

        vheap = SizedVirtualHeap(
            base,
            height,
            blocks_per_bucket=blocks_per_bucket)

        kwds['block_count'] = vheap.bucket_count()
        kwds['block_size'] = vheap.blocks_per_bucket * block_size
        user_header_data = kwds.get('user_header_data', bytes())
        if type(user_header_data) is not bytes:
            raise TypeError(
                "'user_header_data' must be of type bytes. "
                "Invalid type: %s" % (type(user_header_data)))
        kwds['user_header_data'] = \
            struct.pack(cls._index_storage_string,
                        base,
                        height,
                        blocks_per_bucket) + user_header_data
        return EncryptedBlockStorage.setup(*args, **kwds)

    @property
    def user_header_data(self):
        return self._storage.user_header_data[self._index_offset:]

    @property
    def bucket_count(self):
        return self._storage.block_count

    @property
    def bucket_size(self):
        return self._storage.block_size

    @property
    def blocks_per_bucket(self):
        return self._vheap.blocks_per_bucket

    @property
    def storage_name(self):
        return self._storage.storage_name

    @property
    def virtual_heap(self):
        return self._vheap

    def close(self, *args, **kwds):
        self._storage.close()

    def read_path(self, b):
        return self._storage.read_blocks(
            self._vheap.Node(b).bucket_path_from_root())

    def write_path(self, b, buckets):
        self._storage.write_blocks(
            self._vheap.Node(b).bucket_path_from_root(),
            buckets)
