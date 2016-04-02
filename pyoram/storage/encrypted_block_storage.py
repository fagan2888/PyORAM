from pyoram.storage.block_storage import (BlockStorageInterface,
                                          BlockStorageFile,
                                          BlockStorageMMapFile,
                                          BlockStorageS3)
from pyoram.crypto.aesctr import AESCTR

class EncryptedBlockStorage(BlockStorageInterface):

    @staticmethod
    def BlockStorageTypeFactory(storage_type):
        if storage_type == 'file':
            return BlockStorageFile
        elif storage_type == 'mmap':
            return BlockStorageMMapFile
        elif storage_type == 's3':
            return BlockStorageS3
        else:
            raise ValueError("%s: Unsupported storage type: %s"
                             % (self.__class__.__name__, storage_type))

    def __init__(self,
                 key,
                 *args,
                 **kwds):

        self._encryption_key = key
        storage_type = kwds.pop('storage_type', 'file')
        self._storage = \
            self.BlockStorageTypeFactory(storage_type)(*args, **kwds)

    #
    # Add some new methods
    #

    @property
    def ciphertext_block_size(self):
        return self._storage.block_size

    @property
    def encryption_key(self):
        return self._encryption_key

    #
    # Define BlockStorageInterface Methods
    #

    @classmethod
    def setup(cls,
              key,
              filename,
              block_size,
              block_count,
              *args,
              **kwds):
        if (block_size <= 0) or (block_size != int(block_size)):
            raise ValueError(
                "Block size must be a positive integer: %s"
                % (block_size))

        storage_type = EncryptedBlockStorage.\
                       BlockStorageTypeFactory(
                           kwds.pop('storage_type', 'file'))
        initialize = kwds.pop('initialize', None)
        encrypted_block_size = block_size + AESCTR.block_size

        if initialize is None:
            zeros = bytes(bytearray(block_size))
            initialize = lambda i: zeros
        def encrypted_initialize(i):
            return AESCTR.Enc(key, initialize(i))
        kwds['initialize'] = encrypted_initialize

        storage_type.setup(filename,
                           encrypted_block_size,
                           block_count,
                           *args,
                           **kwds)

    @property
    def block_count(self):
        return self._storage.block_count

    @property
    def block_size(self):
        return self._storage.block_size - AESCTR.block_size

    @property
    def filename(self):
        return self._storage.filename

    def close(self):
        self._storage.close()

    def read_block(self, i):
        return AESCTR.Dec(self._encryption_key,
                          self._storage.read_block(i))

    def read_blocks(self, indices):
        return [AESCTR.Dec(self._encryption_key, b)
                for b in self._storage.read_blocks(indices)]

    def write_block(self, i, block):
        self._storage.write_block(
            i,
            AESCTR.Enc(self._encryption_key, block))

    def write_blocks(self, indices, blocks):
        enc_blocks = []
        for i, b in zip(indices, blocks):
            enc_blocks.append(
                AESCTR.Enc(self._encryption_key, b))
        self._storage.write_blocks(indices, enc_blocks)
