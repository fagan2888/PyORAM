__all__ = ('BlockStorageFile',)

import os
import struct
import logging

from pyoram.storage.block_storage import \
    (BlockStorageInterface,
     BlockStorageTypeFactory)

import six
from six.moves import xrange

log = logging.getLogger("pyoram")

class BlockStorageFile(BlockStorageInterface):

    _index_struct_string = "!LLL?"
    _index_offset = struct.calcsize(_index_struct_string)

    def __init__(self,
                 storage_name,
                 ignore_lock=False):
        self._ignore_lock = ignore_lock
        self._f = None
        if not os.path.exists(storage_name):
            raise IOError("Storage location does not exist: %s"
                          % (storage_name))
        self._storage_name = storage_name
        self._f = open(self.storage_name, "r+b")
        self._f.seek(0)
        self._block_size, self._block_count, user_header_size, locked = \
            struct.unpack(
                BlockStorageFile._index_struct_string,
                self._f.read(BlockStorageFile._index_offset))

        if locked and (not self._ignore_lock):
            self._f.close()
            self._f = None
            raise IOError(
                "Can not open block storage device because it is "
                "locked by another process. To ignore this check, "
                "initialize this class with the keyword 'ignore_lock' "
                "set to True.")
        self._user_header_data = bytes()
        if user_header_size > 0:
            self._user_header_data = \
                self._f.read(user_header_size)
        self._header_offset = BlockStorageFile._index_offset + \
                              len(self._user_header_data)
        if not self._ignore_lock:
            # turn on the locked flag
            self._f.seek(0)
            self._f.write(struct.pack(BlockStorageFile._index_struct_string,
                                      self.block_size,
                                      self.block_count,
                                      len(self._user_header_data),
                                      True))
            self._f.flush()

    #
    # Define BlockStorageInterface Methods
    #

    def clone_device(self):
        return BlockStorageFile(self.storage_name, ignore_lock=True)

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
            return BlockStorageFile._index_offset + \
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
                    f.write(struct.pack(BlockStorageFile._index_struct_string,
                                        block_size,
                                        block_count,
                                        0,
                                        False))
                else:
                    f.write(struct.pack(BlockStorageFile._index_struct_string,
                                        block_size,
                                        block_count,
                                        len(header_data),
                                        False))
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
        self._f.seek(BlockStorageFile._index_offset)
        self._f.write(self._user_header_data)

    def close(self):
        if self._f is not None:
            if not self._ignore_lock:
                # turn off the locked flag
                self._f.seek(0)
                self._f.write(
                    struct.pack(BlockStorageFile._index_struct_string,
                                self.block_size,
                                self.block_count,
                                len(self._user_header_data),
                                False))
                self._f.flush()
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
        self._f.flush()

    def write_block(self, i, block):
        assert 0 <= i < self.block_count
        self._f.seek(self._header_offset + i * self.block_size)
        assert len(block) == self.block_size
        self._f.write(block)
        self._f.flush()

BlockStorageTypeFactory.register_device("file", BlockStorageFile)
