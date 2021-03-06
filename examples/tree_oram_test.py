import os
import struct
import random

from pyoram.util.virtual_heap import \
    SizedVirtualHeap
from pyoram.encrypted_storage.encrypted_heap_storage import \
    EncryptedHeapStorage
from pyoram.oblivious_storage.tree.tree_oram_helper import \
    TreeORAMStorageManagerPointerAddressing

def main():
    storage_name = "heap.bin"
    print("Storage Name: %s" % (storage_name))

    key_size = 32
    heap_base = 2
    heap_height = 2
    block_size = struct.calcsize("!?LL")
    blocks_per_bucket = 2
    vheap = SizedVirtualHeap(
        heap_base,
        heap_height,
        blocks_per_bucket=blocks_per_bucket)

    print("Block Size: %s" % (block_size))
    print("Blocks Per Bucket: %s" % (blocks_per_bucket))

    position_map = {}
    def initialize(i):
        bucket = bytes()
        for j in range(blocks_per_bucket):
            if (i*j) % 3:
                bucket += struct.pack(
                    "!?LL", False, 0, 0)
            else:
                x = vheap.Node(i)
                while not vheap.is_nil_node(x):
                    x = x.child_node(random.randint(0, heap_base-1))
                x = x.parent_node()
                bucket += struct.pack(
                    "!?LL", True, initialize.id_, x.bucket)
                position_map[initialize.id_] = x.bucket
                initialize.id_ += 1
        return bucket
    initialize.id_ = 1

    with EncryptedHeapStorage.setup(
            storage_name,
            block_size,
            heap_height,
            heap_base=heap_base,
            key_size=key_size,
            blocks_per_bucket=blocks_per_bucket,
            initialize=initialize,
            ignore_existing=True) as f:
        assert storage_name == f.storage_name
        stash = {}
        oram = TreeORAMStorageManagerPointerAddressing(f, stash)

        b = vheap.random_bucket()
        oram.load_path(b)
        print("")
        print(repr(vheap.Node(oram.path_stop_bucket)))
        print(oram.path_block_ids)
        print(oram.path_block_eviction_levels)

        oram.push_down_path()
        print("")
        print(repr(vheap.Node(oram.path_stop_bucket)))
        print(oram.path_block_ids)
        print(oram.path_block_eviction_levels)
        print(oram.path_block_reordering)

        oram.evict_path()
        oram.load_path(b)
        print("")
        print(repr(vheap.Node(oram.path_stop_bucket)))
        print(oram.path_block_ids)
        print(oram.path_block_eviction_levels)

        oram.push_down_path()
        print("")
        print(repr(vheap.Node(oram.path_stop_bucket)))
        print(oram.path_block_ids)
        print(oram.path_block_eviction_levels)
        print(oram.path_block_reordering)
        assert all(x is None for x in oram.path_block_reordering)

    os.remove(storage_name)

if __name__ == "__main__":
    main()                                             # pragma: no cover
