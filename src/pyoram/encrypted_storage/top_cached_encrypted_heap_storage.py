__all__ = ('TopCachedEncryptedHeapStorage',)

import logging

from pyoram.util.virtual_heap import SizedVirtualHeap
from pyoram.encrypted_storage.encrypted_heap_storage import \
    (EncryptedHeapStorageInterface,
     EncryptedHeapStorage)

import tqdm
import six
from six.moves import xrange

log = logging.getLogger("pyoram")

class TopCachedEncryptedHeapStorage(EncryptedHeapStorageInterface):
    """
    An encrypted block storage device for accessing memory
    organized as a heap, where the top 1 or more levels can
    be cached in local memory. This achieves two things:

      (1) Reduces the number of buckets that need to be read
          from or written to external storage for a given
          path I/O operation.
      (2) Allows certain block storage devices to achieve
          concurrency across path writes by partioning the
          storage space into independent subheaps starting
          below the cache line.

    This devices takes as input an existing encrypted heap
    storage device. This class should not be cloned or used
    to setup storage, but rather used as a wrapper class for
    an existing heap storage device to speed up a bulk set
    of I/O requests. The original heap storage device should
    not be used after it is wrapped by this class. This
    class will close the original device when closing
    itself.

    The number of cached levels (starting from the root
    bucket at level 0) can be set with the 'cached_levels'
    keyword (>= 1).

    By default, this will create an independent storage
    device capable of reading from and writing to the
    original storage devices memory for each independent
    subheap (if any) below the last cached level. The
    'concurrency_level' keyword can be used to limit the
    number of concurrent devices to some level below the
    cache line (>= 0, <= 'cached_levels').

    Values for 'cached_levels' and 'concurrency_level' will
    be automatically reduced when they are larger than what
    is allowed by the heap size.
    """

    def __init__(self,
                 heap_storage,
                 cached_levels=1,
                 concurrency_level=None,
                 show_status_bar=False):
        assert isinstance(heap_storage, EncryptedHeapStorage)
        assert cached_levels >= 1
        if concurrency_level is None:
            concurrency_level = cached_levels
        assert concurrency_level >= 0
        vheap = heap_storage.virtual_heap
        cached_levels = min(vheap.levels, cached_levels)
        concurrency_level = min(cached_levels, concurrency_level)
        self._external_level = cached_levels
        total_buckets = sum(vheap.bucket_count_at_level(l)
                            for l in xrange(cached_levels))
        self._cached_buckets = [None]*total_buckets

        self._root_device = heap_storage
        # clone before we download the cache so that we can
        # track bytes transferred during read/write requests
        # (separate from the cached download)
        self._concurrent_devices = \
            {vheap.first_bucket_at_level(0): self._root_device.clone_device()}
        for b, bucket in enumerate(
                tqdm.tqdm(self._root_device.bucket_storage.yield_blocks(
                    xrange(vheap.first_bucket_at_level(cached_levels))),
                          desc=("Downloading %s Cached Heap Buckets"
                                % (total_buckets)),
                          total=total_buckets,
                          disable=not show_status_bar)):
            self._cached_buckets[b] = bucket

        # Avoid cloning devices when the cache line is at the root
        # bucket or when the entire heap is cached
        if (concurrency_level > 0) and \
           (concurrency_level <= vheap.last_level):
            for b in xrange(vheap.first_bucket_at_level(concurrency_level),
                            vheap.first_bucket_at_level(concurrency_level+1)):
                try:
                    self._concurrent_devices[b] = self._root_device.clone_device()
                except:                                # pragma: no cover
                    log.error(                         # pragma: no cover
                        "Exception encountered while " # pragma: no cover
                        "cloning device. Closing "     # pragma: no cover
                        "storage.")                    # pragma: no cover
                    self.close()                       # pragma: no cover
                    raise                              # pragma: no cover

        self._subheap_storage = {}
        # Avoid populating this dictionary when the entire
        # heap is cached
        if self._external_level <= vheap.last_level:
            for b in xrange(vheap.first_bucket_at_level(self._external_level),
                            vheap.first_bucket_at_level(self._external_level+1)):
                node = vheap.Node(b)
                while node.bucket not in self._concurrent_devices:
                    node = node.parent_node()
                assert node.bucket >= 0
                assert node.level == concurrency_level
                self._subheap_storage[b] = self._concurrent_devices[node.bucket]

    #
    # Additional Methods
    #

    @property
    def cached_buckets(self):
        return self._cached_buckets

    #
    # Define EncryptedHeapStorageInterface Methods
    #

    @property
    def key(self):
        return self._root_device.key

    @property
    def raw_storage(self):
        return self._root_device.raw_storage

    #
    # Define HeapStorageInterface Methods
    #

    def clone_device(self, *args, **kwds):
        raise NotImplementedError(                     # pragma: no cover
            "Class is not designed for cloning")       # pragma: no cover

    @classmethod
    def compute_storage_size(cls, *args, **kwds):
        return EncryptedHeapStorage.compute_storage_size(*args, **kwds)

    @classmethod
    def setup(cls, *args, **kwds):
        raise NotImplementedError(                     # pragma: no cover
            "Class is not designed to setup storage")  # pragma: no cover

    @property
    def header_data(self):
        return self._root_device.header_data

    @property
    def bucket_count(self):
        return self._root_device.bucket_count

    @property
    def bucket_size(self):
        return self._root_device.bucket_size

    @property
    def blocks_per_bucket(self):
        return self._root_device.blocks_per_bucket

    @property
    def storage_name(self):
        return self._root_device.storage_name

    @property
    def virtual_heap(self):
        return self._root_device.virtual_heap

    @property
    def bucket_storage(self):
        return self._root_device.bucket_storage

    def update_header_data(self, new_header_data):
        self._root_device.update_header_data(new_header_data)

    def close(self):
        self.bucket_storage.\
            write_blocks(list(xrange(len(self._cached_buckets))),
                         self._cached_buckets)
        for b in self._concurrent_devices:
            self._concurrent_devices[b].close()
        self._root_device.close()

    def read_path(self, b, level_start=0):
        assert 0 <= b < self.virtual_heap.bucket_count()
        bucket_list = self.virtual_heap.Node(b).bucket_path_from_root()
        if len(bucket_list) <= self._external_level:
            return [self._cached_buckets[bb] for bb in bucket_list[level_start:]]
        elif level_start >= self._external_level:
            return self._subheap_storage[bucket_list[self._external_level]].\
                bucket_storage.read_blocks(bucket_list[level_start:])
        else:
            local_buckets = bucket_list[:self._external_level]
            external_buckets = bucket_list[self._external_level:]
            buckets = []
            for bb in local_buckets[level_start:]:
                buckets.append(self._cached_buckets[bb])
            if len(external_buckets) > 0:
                buckets.extend(
                    self._subheap_storage[external_buckets[0]].\
                    bucket_storage.read_blocks(external_buckets))
            assert len(buckets) == len(bucket_list[level_start:])
            return buckets

    def write_path(self, b, buckets, level_start=0):
        assert 0 <= b < self.virtual_heap.bucket_count()
        bucket_list = self.virtual_heap.Node(b).bucket_path_from_root()
        if len(bucket_list) <= self._external_level:
            for bb, bucket in zip(bucket_list[level_start:], buckets):
                self._cached_buckets[bb] = bucket
        elif level_start >= self._external_level:
            self._subheap_storage[bucket_list[self._external_level]].\
                bucket_storage.write_blocks(bucket_list[level_start:], buckets)
        else:
            buckets = list(buckets)
            assert len(buckets) == len(bucket_list[level_start:])
            local_buckets = bucket_list[:self._external_level]
            external_buckets = bucket_list[self._external_level:]
            ndx = -1
            for ndx, bb in enumerate(local_buckets[level_start:]):
                self._cached_buckets[bb] = buckets[ndx]
            if len(external_buckets) > 0:
                self._subheap_storage[external_buckets[0]].\
                    bucket_storage.write_blocks(external_buckets,
                                                buckets[(ndx+1):])
    @property
    def bytes_sent(self):
        return sum(device.bytes_sent for device
                   in self._concurrent_devices.values())

    @property
    def bytes_received(self):
        return sum(device.bytes_received for device
                   in self._concurrent_devices.values())
