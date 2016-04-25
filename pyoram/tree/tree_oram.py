import struct
import copy

from pyoram.storage.virtualheap import \
    SizedVirtualHeap

from six.moves import xrange

class TreeORAMStorage(object):

    id_storage_string = "!L"
    empty_block_id = 0

    def __init__(self,
                 storage_heap,
                 stash):
        self.storage_heap = storage_heap
        self.stash = stash

        vheap = self.storage_heap.virtual_heap
        self.bucket_size = self.storage_heap.bucket_size
        self.block_size = self.bucket_size // vheap.blocks_per_bucket
        assert self.block_size * vheap.blocks_per_bucket == \
            self.bucket_size

        self.path_stop_bucket = None
        self.path_bucket_count = 0
        self.path_byte_dataview = \
            bytearray(self.bucket_size * vheap.levels)
        dataview = memoryview(self.path_byte_dataview)
        self.path_bucket_dataview = \
            [dataview[(i*self.bucket_size):((i+1)*self.bucket_size)]
             for i in xrange(vheap.levels)]

        self.path_block_dataview = []
        for i in xrange(vheap.levels):
            bucketview = self.path_bucket_dataview[i]
            for j in xrange(vheap.blocks_per_bucket):
                self.path_block_dataview.append(
                    bucketview[(j*self.block_size):((j+1)*self.block_size)])

        max_blocks_on_path = vheap.levels * vheap.blocks_per_bucket
        assert len(self.path_block_dataview) == max_blocks_on_path
        self.path_block_ids = [-1] * max_blocks_on_path
        self.path_block_eviction_levels = [None] * max_blocks_on_path
        self.path_block_reordering = [None] * max_blocks_on_path
        self.path_blocks_inserted = []

    def load_path(self, b):
        vheap = self.storage_heap.virtual_heap
        Z = vheap.blocks_per_bucket
        lcl = vheap.clib.calculate_last_common_level
        k = vheap.k

        assert 0 <= b < vheap.bucket_count()
        self.path_stop_bucket = b
        new_buckets = self.storage_heap.read_path(self.path_stop_bucket)
        self.path_bucket_count = len(new_buckets)
        pos = 0
        for i, bucket in enumerate(new_buckets):
            self.path_bucket_dataview[i][:] = bucket
            for j in xrange(Z):
                block_id, block_addr = \
                    self.get_block_info(self.path_block_dataview[pos])
                self.path_block_ids[pos] = block_id
                if block_id != self.empty_block_id:
                    self.path_block_eviction_levels[pos] = \
                        lcl(k, self.path_stop_bucket, block_addr)
                else:
                    self.path_block_eviction_levels[pos] = None
                self.path_block_reordering[pos] = None
                pos += 1

        max_blocks_on_path = vheap.levels * Z
        while pos != max_blocks_on_path:
            self.path_block_ids[pos] = None
            self.path_block_eviction_levels[pos] = None
            self.path_block_reordering[pos] = None
            pos += 1

        self.path_blocks_inserted = []

    def push_down_path(self):
        vheap = self.storage_heap.virtual_heap
        Z = vheap.blocks_per_bucket

        bucket_count = self.path_bucket_count
        block_ids = self.path_block_ids
        block_eviction_levels = self.path_block_eviction_levels
        block_reordering = self.path_block_reordering
        def _do_swap(write_pos, read_pos):
            block_ids[write_pos], block_eviction_levels[write_pos] = \
                block_ids[read_pos], block_eviction_levels[read_pos]
            block_ids[read_pos], block_eviction_levels[read_pos] = \
                self.empty_block_id, None
            block_reordering[write_pos] = read_pos
            block_reordering[read_pos] = -1

        def _next_write_pos(current):
            while (block_eviction_levels[current] is not None):
                current -= 1
                if current < 0:
                    return None, None
            assert block_ids[current] == \
                self.empty_block_id
            return current, current // Z
        write_pos, write_level = _next_write_pos(
            (bucket_count * Z) - 1)

        def _next_read_pos(current):
            current -= 1
            if current < 0:
                return None
            while (block_eviction_levels[current] is None):
                current -= 1
                if current < 0:
                    return None
            assert block_ids[current] != \
                self.empty_block_id
            return current

        while write_pos is not None:
            read_pos = _next_read_pos(write_pos)
            if read_pos is None:
                break
            while ((read_pos // Z) == write_level) or \
                  (write_level > block_eviction_levels[read_pos]):
                read_pos = _next_read_pos(read_pos)
                if read_pos is None:
                    break
            if read_pos is None:
                break
            _do_swap(write_pos, read_pos)
            write_pos, write_level = _next_write_pos(write_pos)

    def fill_path_from_stash(self):
        vheap = self.storage_heap.virtual_heap
        lcl = vheap.clib.calculate_last_common_level
        k = vheap.k
        Z = vheap.blocks_per_bucket

        bucket_count = self.path_bucket_count
        stop_bucket = self.path_stop_bucket
        block_ids = self.path_block_ids
        block_eviction_levels = self.path_block_eviction_levels
        blocks_inserted = self.path_blocks_inserted

        stash_eviction_levels = {}
        largest_write_position = (bucket_count * Z) - 1
        for write_pos in xrange(largest_write_position,-1,-1):
            write_level = write_pos // Z
            if block_ids[write_pos] == self.empty_block_id:
                del_id = None
                for id_ in self.stash:
                    if id_ not in stash_eviction_levels:
                        block_id, block_addr = \
                            self.get_block_info(self.stash[id_])
                        eviction_level = stash_eviction_levels[id_] = \
                            lcl(k, stop_bucket, block_addr)
                    else:
                        eviction_level = stash_eviction_levels[id_]
                    if write_level <= eviction_level:
                        block_ids[write_pos] = id_
                        block_eviction_levels[write_pos] = \
                            eviction_level
                        blocks_inserted.append(
                            (write_pos, self.stash[id_]))
                        del_id = id_
                        break
                if del_id is not None:
                    del self.stash[del_id]

    def evict_path(self):
        vheap = self.storage_heap.virtual_heap
        Z = vheap.blocks_per_bucket

        stop_bucket = self.path_stop_bucket
        bucket_dataview = self.path_bucket_dataview
        block_dataview = self.path_block_dataview
        block_reordering = self.path_block_reordering
        blocks_inserted = self.path_blocks_inserted

        for i, read_pos in enumerate(
                reversed(block_reordering)):
            if (read_pos is not None) and \
               (read_pos != -1):
                write_pos = len(block_reordering) - 1 - i
                block_dataview[write_pos][:] = block_dataview[read_pos]

        for write_pos, read_pos in enumerate(block_reordering):
            if read_pos == -1:
                self.tag_block_as_empty(block_dataview[write_pos])

        for write_pos, block in blocks_inserted:
            block_dataview[write_pos] = block

        self.storage_heap.write_path(
            stop_bucket,
            (b_.tobytes() for b_ in bucket_dataview))

    def extract_block_from_path(self, id_):
        vheap = self.storage_heap.virtual_heap
        Z = vheap.blocks_per_bucket

        block_ids = self._current_path.block_ids
        bucket_data = self._current_path.bucket_data
        try:
            pos = block_ids.index(id_)
            # make a copy
            block = bytes(self.path_block_dataview[pos])
            self._set_path_position_to_empty(pos)
            return block
        except ValueError:
            return None

    def _set_path_position_to_empty(self, pos):
        self.path_block_ids[pos] = self.empty_block_id
        self.path_block_eviction_levels[pos] = None
        self.path_block_reording[pos] = -1

    @classmethod
    def tag_block_as_empty(cls, block):
        raise NotImplementedError                      # pragma: no cover

    def get_block_info(self, block):
        raise NotImplementedError                      # pragma: no cover

class TreeORAMStorageManagerExplicit(
        TreeORAMStorage):

    block_info_storage_string = \
        TreeORAMStorage.id_storage_string
    block_info_storage_size = \
        struct.calcsize(block_info_storage_string)
    empty_block_info_tag = \
        struct.pack(block_info_storage_string,
                    TreeORAMStorage.\
                    empty_block_id)

    def __init__(self,
                 storage_heap,
                 stash,
                 position_map):
        super(self, TreeORAMStorageManagerExplicit).\
            __init__(storage_heap, stash)
        self.position_map = position_map

    @classmethod
    def tag_block_as_empty(cls, block):
        block[:cls.block_info_storage_size] = \
            cls.empty_block_info_tag

    def get_block_info(self, block):
        id_, = struct.unpack(
            self.block_info_storage_string,
            block[:self.block_info_storage_size])
        return id_, self.position_map[id_]

class TreeORAMStorageManagerImplicit(
        TreeORAMStorage):

    block_info_storage_string = \
        TreeORAMStorage.id_storage_string + "L"
    block_info_storage_size = \
        struct.calcsize(block_info_storage_string)
    empty_block_info_tag = \
        struct.pack(block_info_storage_string,
                    TreeORAMStorage.\
                    empty_block_id, 0)

    @classmethod
    def tag_block_as_empty(cls, block):
        block[:cls.block_info_storage_size] = \
            cls.empty_block_info_tag

    def get_block_info(self, block):
        return struct.unpack(
            self.block_info_storage_string,
            block[:self.block_info_storage_size])
