import struct
import copy

from pyoram.storage.virtualheap import \
    SizedVirtualHeap

class TreeORAM(object):

    _block_info_storage_string = "!QQ"
    _block_info_storage_size = \
        struct.calcsize(_block_info_storage_string)
    _empty_block_id = 0
    _empty_block_info_tag = \
        struct.pack(_block_info_storage_string,
                    _empty_block_id, 0)

    class _LoadedPath(object):
        __slots__ = ("b",
                     "bucket_data",
                     "block_ids",
                     "block_eviction_levels",
                     "blocks_from_stash",
                     "block_reordering")
        def __init__(self):
            self.b = None
            self.bucket_data = []
            self.block_ids = []
            self.block_eviction_levels = []
            self.blocks_from_stash = []
            self.block_reordering = []

    def __init__(self, storage_heap):
        self._stash = {}
        self._storage_heap = storage_heap
        self._vheap = \
            SizedVirtualHeap(
                storage_heap.virtual_heap.k,
                storage_heap.virtual_heap.height,
                blocks_per_bucket=storage_heap.\
                virtual_heap.blocks_per_bucket)
        self._path = self._LoadedPath()

    @property
    def stash(self):
        return self._stash

    @property
    def virtual_heap(self):
        return self._vheap

    def load_path(self, b):

        Z = self._vheap.blocks_per_bucket
        block_size = self._storage_heap.bucket_size // \
                     self._storage_heap.blocks_per_bucket
        lcl = self._vheap.clib.calculate_last_common_level
        k = self._vheap.k

        self._path.b = b
        self._path.bucket_data = \
            [bytearray(bucket) for bucket in self._storage_heap.read_path(b)]
        self._path.block_ids = []
        self._path.block_eviction_levels = []
        self._path.blocks_from_stash = []
        self._path.block_reordering = []
        for bucket in self._path.bucket_data:
            for i in xrange(Z):
                block = bucket[(i * block_size):\
                               ((i+1) * block_size)]
                block_id, block_bucket = struct.unpack(
                    self._block_info_storage_string,
                    block[:self._block_info_storage_size])
                self._path.block_ids.append(block_id)
                if block_id != self._empty_block_id:
                    self._path.block_eviction_levels.append(
                        lcl(k, b, block_bucket))
                else:
                    self._path.block_eviction_levels.append(None)
                self._path.block_reordering.append(None)

    def push_down_path(self):

        blocks_per_bucket = self._vheap.blocks_per_bucket
        block_ids = self._path.block_ids
        block_eviction_levels = self._path.block_eviction_levels
        block_reordering = self._path.block_reordering

        def _do_swap(write_pos, read_pos):
            block_ids[write_pos], block_eviction_levels[write_pos] = \
                block_ids[read_pos], block_eviction_levels[read_pos]
            block_ids[read_pos], block_eviction_levels[read_pos] = \
                self._empty_block_id, None
            block_reordering[write_pos] = read_pos
            block_reordering[read_pos] = -1

        def _next_write_pos(current):
            while (block_eviction_levels[current] is not None):
                current -= 1
                if current < 0:
                    return None, None
            assert block_ids[current] == \
                self._empty_block_id
            return current, current // blocks_per_bucket
        write_pos, write_level = _next_write_pos(
            len(block_eviction_levels)-1)

        def _next_read_pos(current):
            current -= 1
            if current < 0:
                return None
            while (block_eviction_levels[current] is None):
                current -= 1
                if current < 0:
                    return None
            assert block_ids[current] != \
                self._empty_block_id
            return current

        while write_pos is not None:
            read_pos = _next_read_pos(write_pos)
            if read_pos is None:
                break
            while write_level > block_eviction_levels[read_pos]:
                read_pos = _next_read_pos(read_pos)
                if read_pos is None:
                    break
            if read_pos is None:
                break
            _do_swap(write_pos, read_pos)
            write_pos, write_level = _next_write_pos(write_pos)

    def fill_path_from_stash(self):

        b = self._path.b
        lcl = self._vheap.clib.calculate_last_common_level
        k = self._vheap.k
        blocks_per_bucket = self._vheap.blocks_per_bucket

        stash_eviction_levels = {}
        for write_pos in xrange(len(self._path.block_ids)-1,-1,-1):
            write_level = write_pos // blocks_per_bucket
            if self._path.block_ids[write_pos] == self._empty_block_id:
                del_id = None
                for id_ in self._stash:
                    if id_ not in stash_eviction_levels:
                        eviction_level = stash_eviction_levels[id_] = \
                            lcl(k, b, self._stash[id_][0])
                    else:
                        eviction_level = stash_eviction_levels[id_]
                    if write_level <= eviction_level:
                        self._path.block_ids[write_pos] = id_
                        self._path.block_eviction_levels[write_pos] = \
                            eviction_level
                        self._path.blocks_from_stash.append(
                            (write_pos, self._stash[id_]))
                        del_id = id_
                        break
                if del_id is not None:
                    del self._stash[del_id]

    def evict_path(self):

        Z = self._vheap.blocks_per_bucket
        block_size = self._storage_heap.bucket_size // \
                     self._storage_heap.blocks_per_bucket
        b = self._path.b

        bucket_data = self._path.bucket_data
        for i, read_pos in enumerate(
                reversed(self._path.block_reordering)):
            write_pos = len(self._path.block_reordering) - 1 - i
            if (read_pos is not None) and \
               (read_pos != -1):
                wbindex, wboffset = write_pos // Z, write_pos % Z
                rbindex, rboffset = read_pos // Z, read_pos % Z
                bucket_data[wbindex][(wboffset * block_size):\
                                     ((wboffset+1) * block_size)] = \
                self._path.\
                bucket_data[rbindex][(rboffset * block_size):\
                                     ((rboffset+1) * block_size)]

        for write_pos, read_pos in enumerate(
                self._path.block_reordering):
            if read_pos == -1:
                # tag this block as empty
                wbindex, wboffset = write_pos // Z, write_pos % Z
                bucket_data[wbindex][(wboffset * block_size):\
                                     ((wboffset * block_size) + \
                                      self._block_info_storage_size)] = \
                    self._empty_block_info_tag

        for write_pos, block in self._path.blocks_from_stash:
            bindex, boffset = write_pos // Z, write_pos % Z
            bucket_data[bindex][(boffset * block_size):\
                                ((boffset+1) * block_size)] = block

        self._storage_heap.write_path(b, bucket_data)

    def shuffle_access(self,
                       level=None,
                       evict=False):

        self.load_path(self.random_address_at_level(level))
        self.push_down_path()
        self.evict_path()
