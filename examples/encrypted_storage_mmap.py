#
# This example measures the performance of encrypted
# storage access through a local memory-mapped file.
#

import os
import random
import time

import pyoram
from pyoram.util.misc import MemorySize
from pyoram.encrypted_storage.encrypted_block_storage import \
    EncryptedBlockStorage

import tqdm

pyoram.config.SHOW_PROGRESS_BAR = True

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
              EncryptedBlockStorage.compute_storage_size(
                  block_size,
                  block_count,
                  storage_type='mmap'))))
    print("")

    print("Setting Up Encrypted Block Storage")
    setup_start = time.time()
    with EncryptedBlockStorage.setup(storage_name,
                                     block_size,
                                     block_count,
                                     storage_type='mmap',
                                     ignore_existing=True) as f:
        print("Total Setup Time: %2.f s"
              % (time.time()-setup_start))
        print("Total Data Transmission: %s"
              % (MemorySize(f.bytes_sent + f.bytes_received)))
        print("")

    # We close the device and reopen it after
    # setup to reset the bytes sent and bytes
    # received stats.
    with EncryptedBlockStorage(storage_name,
                               key=f.key,
                               storage_type='mmap') as f:

        test_count = 1000
        start_time = time.time()
        for t in tqdm.tqdm(list(range(test_count)),
                           desc="Running I/O Performance Test"):
            f.read_block(random.randint(0,f.block_count-1))
        stop_time = time.time()
        print("Access Block Avg. Data Transmitted: %s (%.3fx)"
              % (MemorySize((f.bytes_sent + f.bytes_received)/float(test_count)),
                 (f.bytes_sent + f.bytes_received)/float(test_count)/float(block_size)))
        print("Access Block Avg. Latency: %.2f ms"
              % ((stop_time-start_time)/float(test_count)*1000))
        print("")

    # cleanup because this is a test example
    os.remove(storage_name)

if __name__ == "__main__":
    main()                                             # pragma: no cover
