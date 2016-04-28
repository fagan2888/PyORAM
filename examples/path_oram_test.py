import os
import struct
import random

from pyoram.storage.virtualheap import \
    SizedVirtualHeap
from pyoram.tree.pathoram import PathORAM

def main():
    storage_name = "heap.bin"
    print("Storage Name: %s" % (storage_name))

    key_size = 32
    block_size = 128
    block_count = 2**8
    print("Block Size: %s" % (block_size))
    print("Block Count: %s" % (block_count))

    with PathORAM.setup(storage_name,
                        block_size,
                        block_count,
                        key_size=key_size,
                        ignore_existing=True) as f:
        assert f.storage_name == storage_name
        assert f.block_count == block_count
        assert f.block_size == block_size
        print("Actual Blocks in Storage: %s"
              % (f._oram.storage_heap.bucket_count * \
                 f._oram.storage_heap.blocks_per_bucket))

        for t in range(100):
            for i in range(block_count):
                f.read_block(i)

        stash_size = []
        for t in range(100):
            for i in range(block_count):
                f.read_block(i)
            stash_size.append(len(f.stash))
        print("Avg Stash Size: %s"
              % (sum(stash_size)/float(len(stash_size))))

    os.remove(storage_name)

if __name__ == "__main__":
    main()                                             # pragma: no cover
