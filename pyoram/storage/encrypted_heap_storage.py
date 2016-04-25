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
    def update_user_header_data(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    def close(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    def read_path(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    def write_path(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover

class EncryptedHeapStorage(HeapStorageInterface):

    _header_storage_string = "!LLL"
    _header_offset = struct.calcsize(_header_storage_string)

    def __init__(self, storage, **kwds):
        if isinstance(storage, EncryptedBlockStorage):
            self._storage = storage
            if len(kwds):
                raise ValueError(
                    "Keywords not used when initializing "
                    "with a storage device: %s"
                    % (str(kwds)))
        else:
            self._storage = EncryptedBlockStorage(storage, **kwds)
        heap_base, heap_height, blocks_per_bucket = \
            struct.unpack(
                self._header_storage_string,
                self._storage.\
                user_header_data[:self._header_offset])
        self._vheap = SizedVirtualHeap(
            heap_base,
            heap_height,
            blocks_per_bucket=blocks_per_bucket)

    #
    # Add some new methods
    #

    @property
    def key(self):
        return self._storage.key

    @property
    def ciphertext_bucket_size(self):
        return self._storage.ciphertext_block_size

    #
    # Define HeapStorageInterface Methods
    #

    @classmethod
    def setup(cls,
              storage_name,
              block_size,
              heap_height,
              **kwds):
        if 'block_count' in kwds:
            raise ValueError("'block_count' keyword is not accepted")
        if heap_height < 0:
            raise ValueError(
                "heap height must be 0 or greater. Invalid value: %s"
                % (heap_height))
        blocks_per_bucket = kwds.pop('blocks_per_bucket', 1)
        if blocks_per_bucket < 1:
            raise ValueError(
                "blocks_per_bucket must be 1 or greater. "
                "Invalid value: %s" % (blocks_per_bucket))
        heap_base = kwds.pop('heap_base', 2)
        if heap_base < 2:
            raise ValueError(
                "heap base must be 2 or greater. Invalid value: %s"
                % (heap_base))

        vheap = SizedVirtualHeap(
            heap_base,
            heap_height,
            blocks_per_bucket=blocks_per_bucket)

        user_header_data = kwds.pop('user_header_data', bytes())
        if type(user_header_data) is not bytes:
            raise TypeError(
                "'user_header_data' must be of type bytes. "
                "Invalid type: %s" % (type(user_header_data)))
        kwds['user_header_data'] = \
            struct.pack(cls._header_storage_string,
                        heap_base,
                        heap_height,
                        blocks_per_bucket) + \
            user_header_data
        return EncryptedHeapStorage(
            EncryptedBlockStorage.setup(
                storage_name,
                vheap.blocks_per_bucket * block_size,
                vheap.bucket_count(),
                **kwds))

    @property
    def user_header_data(self):
        return self._storage.user_header_data[self._header_offset:]

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

    def update_user_header_data(self, new_user_header_data):
        self._storage.update_user_header_data(
            self._storage.user_header_data[:self._header_offset] + \
            new_user_header_data)

    def close(self):
        self._storage.close()

    def read_path(self, b):
        return self._storage.read_blocks(
            self._vheap.Node(b).bucket_path_from_root())

    def write_path(self, b, buckets):
        self._storage.write_blocks(
            self._vheap.Node(b).bucket_path_from_root(),
            buckets)
