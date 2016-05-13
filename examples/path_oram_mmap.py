#
# This example measures the performance of Path ORAM
# when storage is accessed through a local memory-mapped
# file (mmap).
#

import os
import random
import time
import pickle

from pyoram.util.misc import MemorySize
from pyoram.oblivious_storage.tree.path_oram import \
    PathORAM

import tqdm

# Set the storage location and size
storage_name = "heap.bin"
# 4KB block size
block_size = 4000
# one block per bucket in the
# storage heap of height 8
block_count = 2**(8+1)-1

def main():

    print("Storage Name: %s" % (storage_name))
    print("Block Count: %s" % (block_count))
    print("Block Size: %s" % (MemorySize(block_size)))
    print("Total Memory: %s"
          % (MemorySize(block_size*block_count)))
    print("Actual Storage Required: %s"
          % (MemorySize(
              PathORAM.compute_storage_size(
                  block_size,
                  block_count,
                  storage_type='mmap'))))
    print("")

    print("Setting Up Path ORAM Storage")
    setup_start = time.time()
    with PathORAM.setup(storage_name,
                        block_size,
                        block_count,
                        storage_type='mmap',
                        ignore_existing=True,
                        show_status_bar=True) as f:
        print("Total Setup Time: %2.f s"
              % (time.time()-setup_start))
        print("Current Stash Size: %s"
              % len(f.stash))
        print("Total Data Transmission: %s"
              % (MemorySize(f.bytes_sent + f.bytes_received)))
        print("")

    # We close the device and reopen it after
    # setup to reset the bytes sent and bytes
    # received stats.
    with PathORAM(storage_name,
                  f.stash,
                  f.position_map,
                  key=f.key,
                  storage_type='mmap') as f:

        test_count = 100
        start_time = time.time()
        for t in tqdm.tqdm(list(range(test_count)),
                           desc="Running I/O Performance Test"):
            f.read_block(random.randint(0,f.block_count-1))
        stop_time = time.time()
        print("Current Stash Size: %s"
              % len(f.stash))
        print("Access Block Avg. Data Transmitted: %s (%.2fx)"
              % (MemorySize((f.bytes_sent + f.bytes_received)/float(test_count)),
                 (f.bytes_sent + f.bytes_received)/float(test_count)/float(block_size)))
        print("Access Block Avg. Latency: %.2f ms"
              % ((stop_time-start_time)/float(test_count)*1000))
        print("")

if __name__ == "__main__":
    main()                                             # pragma: no cover
    # cleanup because this is a test example
    os.remove(storage_name)
