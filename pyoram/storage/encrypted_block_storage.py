from pyoram.storage.block_storage import (BlockStorageInterface,
                                          BlockStorageTypeFactory)

from pyoram.crypto.aesctr import AESCTR

class EncryptedBlockStorage(BlockStorageInterface):

    def __init__(self,
                 *args,
                 **kwds):
        self._encryption_key = kwds.pop('encryption_key')
        storage_type = kwds.pop('storage_type', 'file')
        self._storage = \
            BlockStorageTypeFactory(storage_type)\
            (*args, **kwds)

    #
    # Add some new methods
    #

    @property
    def encryption_key(self):
        return self._encryption_key

    @property
    def ciphertext_block_size(self):
        return self._storage.block_size

    #
    # Define BlockStorageInterface Methods
    #

    @classmethod
    def setup(cls,
              *args,
              **kwds):

        key_size = kwds.pop("key_size", None)
        if key_size is None:
            raise ValueError("'key_size' is required")
        encryption_key = AESCTR.KeyGen(key_size)

        block_size = kwds.get("block_size")
        if (block_size is None):
            raise ValueError("'block_size' is required")
        if (block_size <= 0) or (block_size != int(block_size)):
            raise ValueError(
                "Block size (bytes) must be a positive integer: %s"
                % (block_size))

        storage_type = BlockStorageTypeFactory(
            kwds.pop('storage_type', 'file'))
        initialize = kwds.pop('initialize', None)
        encrypted_block_size = block_size + AESCTR.block_size

        if initialize is None:
            zeros = bytes(bytearray(block_size))
            initialize = lambda i: zeros
        def encrypted_initialize(i):
            return AESCTR.Enc(encryption_key, initialize(i))
        kwds['initialize'] = encrypted_initialize

        kwds['block_size'] = encrypted_block_size

        user_header_data = kwds.get('user_header_data', bytes())
        if type(user_header_data) is not bytes:
            raise TypeError(
                "'user_header_data' must be of type bytes. "
                "Invalid type: %s" % (type(user_header_data)))
        kwds['user_header_data'] = \
                AESCTR.Enc(encryption_key, user_header_data)
        storage_type.setup(*args, **kwds)

        return encryption_key

    @property
    def user_header_data(self):
        return AESCTR.Dec(self._encryption_key,
                          self._storage.user_header_data)

    @property
    def block_count(self):
        return self._storage.block_count

    @property
    def block_size(self):
        return self._storage.block_size - AESCTR.block_size

    @property
    def storage_name(self):
        return self._storage.storage_name

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
