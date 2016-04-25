import struct
import random

from pyoram.storage.virtualheap import \
    SizedVirtualHeap
from pyoram.storage.encrypted_heap_storage import \
    EncryptedHeapStorage
from pyoram.tree.tree_oram import TreeORAMStorageManagerImplicit

storage_name = "heap.bin"
print("Storage Name: %s" % (storage_name))

key_size = 32
heap_base = 2
heap_height = 2
block_size = struct.calcsize("!LL")
blocks_per_bucket = 2
vheap = SizedVirtualHeap(
    heap_base,
    heap_height,
    blocks_per_bucket=blocks_per_bucket)

print("Block Size: %s" % (block_size))
print("Blocks Per Bucket: %s" % (blocks_per_bucket))

id_ = 1
position_map = {}
def initialize(i):
    global id_
    bucket = bytes()
    for j in range(blocks_per_bucket):
        if (i*j) % 3:
            bucket += struct.pack(
                "!LL", 0, 0)
        else:
            x = vheap.Node(i)
            while not vheap.is_nil_node(x):
                x = x.child_node(random.randint(0, heap_base-1))
            x = x.parent_node()
            print(i, id_, repr(x))
            bucket += struct.pack(
                "!LL", id_, x.bucket)
            position_map[id_] = x.bucket
            id_ += 1
    return bucket

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
    oram = TreeORAMStorageManagerImplicit(f, stash)

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
