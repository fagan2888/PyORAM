import hashlib
import struct
import array

from pyoram.tree.tree_oram import \
    (TreeORAMStorage,
     TreeORAMStorageManagerExplicitAddressing,
     TreeORAMStorageManagerPointerAddressing)
from pyoram.storage.encrypted_block_storage import \
    EncryptedBlockStorageInterface
from pyoram.storage.encrypted_heap_storage import \
    EncryptedHeapStorage
from pyoram.storage.virtualheap import \
    (SizedVirtualHeap,
     calculate_necessary_heap_height)

import six
from six.moves import xrange

class PathORAM(EncryptedBlockStorageInterface):

    _header_struct_string = "!"+("x"*2*hashlib.sha1().digest_size)+"L?"
    _header_offset = struct.calcsize(_header_struct_string)

    def __init__(self,
                 storage,
                 stash,
                 position_map=None,
                 **kwds):

        if isinstance(storage, EncryptedHeapStorage):
            storage_heap = storage
            if len(kwds):
                raise ValueError(
                    "Keywords not used when initializing "
                    "with a storage device: %s"
                    % (str(kwds)))
        else:
            storage_heap = EncryptedHeapStorage(storage, **kwds)

        self._block_count, self._pointer_addressing = struct.unpack(
            self._header_struct_string,
            storage_heap.header_data\
            [:self._header_offset])
        stashdigest = storage_heap.\
                      header_data[:hashlib.sha1().digest_size]
        positiondigest = storage_heap.\
            header_data[hashlib.sha1().digest_size:\
                        (2*hashlib.sha1().digest_size)]

        if stashdigest != PathORAM.stash_digest(stash):
            raise ValueError(
                "Stash digest does not match that saved with "
                "storage heap %s" % (storage_heap.storage_name))

        if not self._pointer_addressing:
            tree_oram_type = TreeORAMStorageManagerExplicitAddressing
            if position_map is None:
                raise ValueError(
                    "Keyword 'position_map' is required for "
                    "PathORAM using explicit addressing")
            if positiondigest != PathORAM.position_map_digest(position_map):
                raise ValueError(
                    "Stash digest does not match that saved with "
                    "storage heap %s" % (storage_heap.storage_name))
            args = (stash, position_map)
        else:
            tree_oram_type = TreeORAMStorageManagerPointerAddressing
            if position_map is not None:
                raise ValueError(
                    "Keyword 'position_map' is not used for "
                    "PathORAM using pointer addressing")
            args = (stash,)
        self._oram = tree_oram_type(storage_heap, *args)
        assert self._block_count <= self._oram.storage_heap.bucket_count

    def _init_oram_block(self, id_, block):
        oram_block = bytearray(self.block_size)
        if self._pointer_addressing:
            assert len(oram_block) == len(block)
            oram_block[:] = block[:]
        else:
            oram_block[self._oram.block_info_storage_size:] = block[:]
        self._oram.tag_block_with_id(oram_block, id_)
        return oram_block

    def _extract_virtual_block(self, block):
        if self._pointer_addressing:
            return block
        else:
            return block[self._oram.block_info_storage_size:]

    #
    # Add some methods specific to Path ORAM
    #

    @classmethod
    def stash_digest(cls, stash, hasher=None):
        if hasher is None:
            hasher = hashlib.sha1()
        id_to_bytes = lambda id_: \
            struct.pack(TreeORAMStorage.block_id_storage_string, id_)
        if len(stash) == 0:
            hasher.update(b'0')
        else:
            for id_ in stash:
                hasher.update(id_to_bytes(id_))
                hasher.update(stash[id_])
        return hasher.digest()

    @classmethod
    def position_map_digest(cls, position_map, hasher=None):
        if hasher is None:
            hasher = hashlib.sha1()
        id_to_bytes = lambda id_: \
            struct.pack(TreeORAMStorage.block_id_storage_string, id_)
        if position_map is None:
            hasher.update(b'0')
        else:
            assert len(position_map) > 0
            for addr in position_map:
                hasher.update(id_to_bytes(addr))
        return hasher.digest()

    @property
    def position_map(self):
        return self._oram.position_map

    @property
    def stash(self):
        return self._oram.stash

    def access(self, id_, write_block=None):
        assert 0 <= id_ <= self.block_count
        b = self.position_map[id_]
        self.position_map[id_] = \
            self._oram.storage_heap.virtual_heap.random_leaf_bucket()
        self._oram.load_path(b)
        block = self._oram.extract_block_from_path(id_)
        if block is None:
            block = self.stash[id_]
        if write_block is not None:
            block = self._init_oram_block(id_, write_block)
        self.stash[id_] = block
        self._oram.push_down_path()
        self._oram.fill_path_from_stash()
        self._oram.evict_path()
        if write_block is None:
            return self._extract_virtual_block(block)

    #
    # Define EncryptedBlockStorageInterface Methods
    #

    @property
    def key(self):
        return self._oram.storage_heap.key

    #
    # Define BlockStorageInterface Methods
    #

    @classmethod
    def compute_storage_size(cls,
                             block_size,
                             block_count,
                             bucket_capacity=4,
                             addressing_scheme='explicit',
                             heap_base=2,
                             ignore_header=False,
                             **kwds):
        assert (block_size > 0) and (block_size == int(block_size))
        assert (block_count > 0) and (block_count == int(block_count))
        assert bucket_capacity >= 1
        assert addressing_scheme in ('explicit', 'pointer')
        assert heap_base >= 2
        assert 'heap_height' not in kwds
        heap_height = calculate_necessary_heap_height(heap_base,
                                                      block_count)
        if addressing_scheme == 'explicit':
            block_size += TreeORAMStorageManagerExplicitAddressing.\
                          block_info_storage_size
        if ignore_header:
            return EncryptedHeapStorage.compute_storage_size(
                block_size,
                heap_height,
                blocks_per_bucket=bucket_capacity,
                heap_base=heap_base,
                ignore_header=True,
                **kwds)
        else:
            return cls._header_offset + \
                   EncryptedHeapStorage.compute_storage_size(
                       block_size,
                       heap_height,
                       blocks_per_bucket=bucket_capacity,
                       heap_base=heap_base,
                       ignore_header=False,
                       **kwds)

    @classmethod
    def setup(cls,
              storage_name,
              block_size,
              block_count,
              bucket_capacity=4,
              addressing_scheme='explicit',
              heap_base=2,
              **kwds):
        if 'heap_height' in kwds:
            raise ValueError("'heap_height' keyword is not accepted")
        if addressing_scheme not in ('explicit', 'pointer'):
            raise ValueError(
                "Keyword 'addressing_scheme' must be "
                "'explicit' or 'pointer', not '%s'"
                % (addressing_scheme))
        if (bucket_capacity <= 0) or (bucket_capacity != int(bucket_capacity)):
            raise ValueError(
                "Bucket capacity must be a positive integer: %s"
                % (bucket_capacity))
        if (block_size <= 0) or (block_size != int(block_size)):
            raise ValueError(
                "Block size (bytes) must be a positive integer: %s"
                % (block_size))
        if (block_count <= 0) or (block_count != int(block_count)):
            raise ValueError(
                "Block count must be a positive integer: %s"
                % (block_count))
        if heap_base < 2:
            raise ValueError(
                "heap base must be 2 or greater. Invalid value: %s"
                % (heap_base))

        heap_height = calculate_necessary_heap_height(heap_base,
                                                      block_count)
        stash = {}
        position_map = None
        if addressing_scheme == 'explicit':
            tree_oram_type = TreeORAMStorageManagerExplicitAddressing
            vheap = SizedVirtualHeap(
                heap_base,
                heap_height,
                blocks_per_bucket=bucket_capacity)
            position_map = array.array("L", [vheap.random_leaf_bucket()
                                             for i in xrange(block_count)])
            oram_block_size = block_size + \
                              tree_oram_type.block_info_storage_size
        else:
            assert addressing_scheme == 'pointer'
            tree_oram_type = TreeORAMStorageManagerPointerAddressing
            position_map = None
            if block_size < tree_oram_type.block_info_storage_size:
                raise ValueError("Block size must be no less than %s bytes "
                                 "when running PathORAM with pointer-"
                                 "based addressing"
                                 % (tree_oram_type.block_info_storage_size))
            oram_block_size = block_size

        user_header_data = kwds.pop('header_data', bytes())
        if type(user_header_data) is not bytes:
            raise TypeError(
                "'header_data' must be of type bytes. "
                "Invalid type: %s" % (type(user_header_data)))

        initialize = kwds.pop('initialize', None)

        header_data = struct.pack(
            cls._header_struct_string,
            block_count,
            tree_oram_type is TreeORAMStorageManagerPointerAddressing)
        kwds['header_data'] = bytes(header_data) + user_header_data
        empty_bucket = bytearray(oram_block_size * bucket_capacity)
        empty_bucket_view = memoryview(empty_bucket)
        for i in xrange(bucket_capacity):
            tree_oram_type.tag_block_as_empty(
                empty_bucket_view[(i*oram_block_size):((i+1)*oram_block_size)])
        empty_bucket = bytes(empty_bucket)

        kwds['initialize'] = lambda i: empty_bucket
        f = None
        try:
            f = EncryptedHeapStorage.setup(storage_name,
                                           oram_block_size,
                                           heap_height,
                                           heap_base=heap_base,
                                           blocks_per_bucket=bucket_capacity,
                                           **kwds)

            oram = tree_oram_type(f, stash, position_map)

            if initialize is None:
                zeros = bytes(bytearray(block_size))
                initialize = lambda i: zeros
            initial_oram_block = bytearray(oram_block_size)
            for i in xrange(block_count):
                oram.tag_block_with_id(initial_oram_block, i)
                initial_oram_block[oram.block_info_storage_size:] = \
                    initialize(i)[:]

                b = oram.position_map[i]
                oram.position_map[i] = \
                    oram.storage_heap.virtual_heap.random_leaf_bucket()
                oram.load_path(b)
                oram.push_down_path()
                # place a copy in the stash
                oram.stash[i] = bytearray(initial_oram_block)
                oram.fill_path_from_stash()
                oram.evict_path()

            header_data = bytearray(header_data)
            stash_digest = cls.stash_digest(oram.stash)
            position_map_digest = cls.position_map_digest(oram.position_map)
            header_data[:len(stash_digest)] = stash_digest[:]
            header_data[len(stash_digest):\
                        (len(stash_digest)+len(position_map_digest))] = \
                position_map_digest[:]
            f.update_header_data(bytes(header_data) + user_header_data)
        except:
            if f is not None:
                f.close()
            raise
        assert f is not None
        return PathORAM(f, stash, position_map=position_map)

    @property
    def header_data(self):
        return self._oram.storage_heap.\
            header_data[self._header_offset:]

    @property
    def block_count(self):
        return self._block_count

    @property
    def block_size(self):
        if self._pointer_addressing:
            return self._oram.block_size
        else:
            return self._oram.block_size - self._oram.block_info_storage_size

    @property
    def storage_name(self):
        return self._oram.storage_heap.storage_name

    def update_header_data(self, new_header_data):
        self._oram.storage_heap.update_header_data(
            self._oram.storage_heap.header_data[:self._header_offset] + \
            new_header_data)

    def close(self):
        stashdigest = PathORAM.stash_digest(self._oram.stash)
        positiondigest = PathORAM.position_map_digest(self._oram.position_map)
        new_header_data = \
            bytearray(self._oram.storage_heap.header_data[:self._header_offset])
        new_header_data[:hashlib.sha1().digest_size] = stashdigest
        new_header_data[hashlib.sha1().digest_size:\
                        (2*hashlib.sha1().digest_size)] = positiondigest
        self._oram.storage_heap.update_header_data(
            bytes(new_header_data) + self.header_data)
        self._oram.storage_heap.close()

    def read_blocks(self, indices):
        blocks = []
        for i in indices:
            blocks.append(self.access(i))
        return blocks

    def read_block(self, i):
        return self.access(i)

    def write_blocks(self, indices, blocks):
        for i, block in zip(indices, blocks):
            self.access(i, write_block=block)

    def write_block(self, i, block):
        self.access(i, write_block=block)
