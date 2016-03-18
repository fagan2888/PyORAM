from pyoram.storage.block_storage import (BlockStorageInterface,
                                          BlockStorageFile,
                                          BlockStorageMMapFile,
                                          BlockStorageS3)
from pyoram.crypto.aesctr import AESCTRMode

class EncryptedBlockStorage(BlockStorageInterface):
    def __init__(self, *args, **kwds):
        storage_type = kwds.pop('storage_type')
        self._encryption_key = kwds.pop('key')
        blocksize += AESCTRMode._ivsize
        kwds['blocksize'] = blocksize
        if storage_type == 'file':
            self._storage = BlockStorageFile(*args, **kwds)
        elif storage_type == 'mmap':
            self._storage = BlockStorageMMapFile(*args, **kwds)
        elif storage_type == 's3':
            self._storage = BlockStorageS3(*args, **kwds)

    #
    # Add some new methods
    #
    @property
    def iv_blocksize(self):
        return AESCTR_Enc._ivsize

    @property
    def ciphertext_blocksize(self):
        return self._storage.blocksize + self._iv_blocksize
    @property
    def encryption_key(self):
        return self._encryption_key

    #
    # Define BlockStorageInterface Methods
    #

    @property
    def blockcount(self): return self._storage.block_count
    @property
    def blocksize(self): return self._storage.block_size
    @property
    def filename(self): return self._storage.filename

    def close(self): self._storage.close()

    def read_block(self, i):
        return AESCTR_Dec(self._encryption_key,
                          self._storage.read_block(i))
    def read_blocks(self, indices):
        blocks = []
        for i, b in zip(indices, self._storage.read_blocks(indices)):
            blocks.append(AESCTR_Dec(self._encryption_key, b))
        return blocks

    def write_block(self, i, block):
        self._storage.write_block(i, AESCTR_Enc(self._encryption_key, block))
    def write_blocks(self, indices, blocks):
        enc_blocks = []
        for i, b in zip(indices, blocks):
            enc_blocks.append(AESCTR_Enc(self._encryption_key, b))
        self._storage.write_blocks(indices, enc_blocks)
