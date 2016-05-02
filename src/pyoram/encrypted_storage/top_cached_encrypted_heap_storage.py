__all__ = ('TopCachedEncryptedHeapStorage',)

from pyoram.util.virtual_heap import SizedVirtualHeap
from pyoram.encrypted_storage.encrypted_heap_storage import \
    (EncryptedHeapStorageInterface,
     EncryptedHeapStorage)

import six
from six.moves import xrange

class TopCachedEncryptedHeapStorage(EncryptedHeapStorageInterface):

    def __init__(self,
                 heap_storage,
                 cached_levels=1):
        assert cached_levels >= 1
        self._heap_storage = heap_storage
        vheap = heap_storage.virtual_heap
        self._external_level_start = min(vheap.levels, cached_levels)
        self._cached_buckets = self._heap_storage.block_storage.read_blocks(
            list(xrange(vheap.last_bucket_at_level(self._external_level_start-1)+1)))
        self._subheap_storage = {}
        for b in xrange(vheap.first_bucket_at_level(self._external_level_start),
                        vheap.last_bucket_at_level(self._external_level_start)+1):
            self._subheap_storage[b] = self._heap_storage.clone_device()

    @property
    def key(self):
        return self._heap_storage.key

    #
    # Define HeapStorageInterface Methods
    #

    def clone_device(self, *args, **kwds):
        raise NotImplementedError(
            "This class is not designed for cloning")

    @classmethod
    def compute_storage_size(cls, *args, **kwds):
        return EncryptedHeapStorage.compute_storage_size(*args, **kwds)

    @classmethod
    def setup(cls, *args, **kwds):
        raise NotImplementedError(
            "This class is not designed to be directly setup")

    @property
    def header_data(self):
        return self._heap_storage.header_data

    @property
    def bucket_count(self):
        return self._heap_storage.bucket_count

    @property
    def bucket_size(self):
        return self._heap_storage.bucket_size

    @property
    def blocks_per_bucket(self):
        return self._heap_storage.blocks_per_bucket

    @property
    def storage_name(self):
        return self._heap_storage.storage_name

    @property
    def virtual_heap(self):
        return self._heap_storage.virtual_heap

    @property
    def block_storage(self):
        return self._heap_storage.block_storage

    def update_header_data(self, new_header_data):
        self._heap_storage.update_header_data(new_header_data)

    def close(self):
        self._heap_storage.block_storage.\
            write_blocks(list(xrange(len(self._cached_buckets))),
                         self._cached_buckets)
        for b in self._subheap_storage:
            self._subheap_storage[b].close()
        self._heap_storage.close()

    def read_path(self, b, level_start=0):
        assert 0 <= b < self.virtual_heap.bucket_count()
        bucket_list = self.virtual_heap.Node(b).bucket_path_from_root()
#        print("R", str(bucket_list), level_start)
        if len(bucket_list) <= self._external_level_start:
#            print("\nA")
            return [self._cached_buckets[bb] for bb in bucket_list[level_start:]]
        elif level_start >= self._external_level_start:
#            print("\nB")
            return self._subheap_storage[bucket_list[self._external_level_start]].\
                  block_storage.read_blocks(bucket_list[level_start:])
        else:
#            print("\nC")
            local_buckets = bucket_list[:self._external_level_start]
            external_buckets = bucket_list[self._external_level_start:]
            buckets = []
            for bb in local_buckets[level_start:]:
                buckets.append(self._cached_buckets[bb])
            if len(external_buckets) > 0:
                buckets.extend(
                    self._subheap_storage[external_buckets[0]].\
                    block_storage.read_blocks(external_buckets))
            assert len(buckets) == len(bucket_list[level_start:])
#            import base64
#            print(str([base64.b64encode(b_) for b_ in buckets]))
            return buckets

    def write_path(self, b, buckets, level_start=0):
        assert 0 <= b < self.virtual_heap.bucket_count()
        bucket_list = self.virtual_heap.Node(b).bucket_path_from_root()
#        print("W", str(bucket_list), level_start)
        if len(bucket_list) <= self._external_level_start:
#            print("\nA")
            for bb, bucket in zip(bucket_list[level_start:], buckets):
                self._cached_buckets[bb] = bucket
        elif level_start >= self._external_level_start:
#            print("\nB")
            self._subheap_storage[bucket_list[self._external_level_start]].\
                block_storage.write_blocks(bucket_list[level_start:], buckets)
        else:
#            print("\nC")
            buckets = list(buckets)
            assert len(buckets) == len(bucket_list[level_start:])
            local_buckets = bucket_list[:self._external_level_start]
            external_buckets = bucket_list[self._external_level_start:]
            ndx = -1
            for ndx, bb in enumerate(local_buckets[level_start:]):
                self._cached_buckets[bb] = buckets[ndx]
            if len(external_buckets) > 0:
                self._subheap_storage[external_buckets[0]].\
                    block_storage.write_blocks(external_buckets,
                                               buckets[(ndx+1):])
