__all__ = ('BlockStorageMMap',)

import logging
import mmap

from pyoram.storage.block_storage import \
    BlockStorageTypeFactory
from pyoram.storage.block_storage_file import \
    BlockStorageFile

log = logging.getLogger("pyoram")

class BlockStorageMMap(BlockStorageFile):

    def __init__(self, *args, **kwds):
        super(BlockStorageMMap, self).__init__(*args, **kwds)
        self._mm = None
        self._mm = mmap.mmap(self._f.fileno(), 0)

    #
    # Define BlockStorageInterface Methods
    # (override what is defined on BlockStorageFile)

    def clone_device(self):
        return BlockStorageMMap(self.storage_name,
                                ignore_lock=True)

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
        return BlockStorageMMap(storage_name)

    def update_header_data(self, new_header_data):
        if len(new_header_data) != len(self.header_data):
            raise ValueError(
                "The size of header data can not change.\n"
                "Original bytes: %s\n"
                "New bytes: %s" % (len(self.header_data),
                                   len(new_header_data)))
        self._user_header_data = new_header_data
        self._mm.seek(BlockStorageMMap._index_offset)
        self._mm.write(self._user_header_data)

    def close(self):
        if self._mm is not None:
            try:
                self._mm.close()
            except OSError:                            # pragma: no cover
                pass                                   # pragma: no cover
            self._mm = None
        super(BlockStorageMMap, self).close()

    def read_blocks(self, indices):
        blocks = []
        for i in indices:
            assert 0 <= i < self.block_count
            self._bytes_received += self.block_size
            self._mm.seek(self._header_offset + i * self.block_size)
            blocks.append(self._mm.read(self.block_size))
        return blocks

    def yield_blocks(self, indices):
        for i in indices:
            assert 0 <= i < self.block_count
            self._bytes_received += self.block_size
            self._mm.seek(self._header_offset + i * self.block_size)
            yield self._mm.read(self.block_size)

    def read_block(self, i):
        assert 0 <= i < self.block_count
        self._bytes_received += self.block_size
        return self._mm[self._header_offset + i*self.block_size : \
                        self._header_offset + (i+1)*self.block_size]

    def write_blocks(self, indices, blocks):
        for i, block in zip(indices, blocks):
            assert 0 <= i < self.block_count
            self._bytes_sent += self.block_size
            self._mm[self._header_offset + i*self.block_size : \
                     self._header_offset + (i+1)*self.block_size] = block

    def write_block(self, i, block):
        assert 0 <= i < self.block_count
        self._bytes_sent += self.block_size
        self._mm[self._header_offset + i*self.block_size : \
                 self._header_offset + (i+1)*self.block_size] = block

    @property
    def bytes_sent(self):
        return self._bytes_sent

    @property
    def bytes_received(self):
        return self._bytes_received

BlockStorageTypeFactory.register_device("mmap", BlockStorageMMap)
