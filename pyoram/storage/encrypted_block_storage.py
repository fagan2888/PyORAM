from pyoram.storage.block_storage import (BlockStorageInterface,
                                          BlockStorageTypeFactory)

from pyoram.crypto.aesctr import AESCTR

class EncryptedBlockStorage(BlockStorageInterface):

    def __init__(self, storage, **kwds):
        self._key = kwds.pop('key', None)
        if self._key is None:
            raise ValueError(
                "An encryption key is required using "
                "the 'key' keyword.")
        if isinstance(storage, BlockStorageInterface):
            self._storage = storage
            if len(kwds):
                raise ValueError(
                    "Keywords not used when initializing "
                    "with a storage device: %s"
                    % (str(kwds)))
        else:
            storage_type = kwds.pop('storage_type', 'file')
            self._storage = \
                BlockStorageTypeFactory(storage_type)(storage, **kwds)

    #
    # Add some new methods
    #

    @property
    def key(self):
        return self._key

    @property
    def ciphertext_block_size(self):
        return self._storage.block_size

    #
    # Define BlockStorageInterface Methods
    #

    @classmethod
    def setup(cls,
              storage_name,
              block_size,
              block_count,
              **kwds):

        key = AESCTR.KeyGen(
            kwds.pop('key_size', AESCTR.key_sizes[-1]))
        if (block_size <= 0) or (block_size != int(block_size)):
            raise ValueError(
                "Block size (bytes) must be a positive integer: %s"
                % (block_size))

        storage_type_name = kwds.pop('storage_type', 'file')
        storage_type = BlockStorageTypeFactory(
            storage_type_name)
        initialize = kwds.pop('initialize', None)
        encrypted_block_size = block_size + AESCTR.block_size

        if initialize is None:
            zeros = bytes(bytearray(block_size))
            initialize = lambda i: zeros
        def encrypted_initialize(i):
            return AESCTR.Enc(key, initialize(i))
        kwds['initialize'] = encrypted_initialize

        user_header_data = kwds.get('user_header_data', bytes())
        if type(user_header_data) is not bytes:
            raise TypeError(
                "'user_header_data' must be of type bytes. "
                "Invalid type: %s" % (type(user_header_data)))
        kwds['user_header_data'] = \
                AESCTR.Enc(key, user_header_data)
        return EncryptedBlockStorage(
            storage_type.setup(storage_name,
                               encrypted_block_size,
                               block_count,
                               **kwds),
            key=key)

    @property
    def user_header_data(self):
        return AESCTR.Dec(self._key,
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

    def update_user_header_data(self, new_user_header_data):
        self._storage.update_user_header_data(
            AESCTR.Enc(self._key, new_user_header_data))

    def close(self):
        self._storage.close()

    def read_block(self, i):
        return AESCTR.Dec(self._key,
                          self._storage.read_block(i))

    def read_blocks(self, indices):
        return [AESCTR.Dec(self._key, b)
                for b in self._storage.read_blocks(indices)]

    def write_block(self, i, block):
        self._storage.write_block(
            i,
            AESCTR.Enc(self._key, block))

    def write_blocks(self, indices, blocks):
        enc_blocks = []
        for i, b in zip(indices, blocks):
            enc_blocks.append(
                AESCTR.Enc(self._key, b))
        self._storage.write_blocks(indices, enc_blocks)
