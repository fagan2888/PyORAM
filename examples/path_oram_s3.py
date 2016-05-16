#
# This example measures the performance of Path ORAM when
# storage is accessed through Amazon Simple Storage Service
# (S3).
#
# In order to run this example, you must provide a valid
# S3 bucket name and have the following variables defined
# in your current environment:
#  - AWS_ACCESS_KEY_ID
#  - AWS_SECRET_ACCESS_KEY
#  - AWS_DEFAULT_REGION
# These can also be set using keywords.
#

import os
import random
import time

import pyoram
from pyoram.util.misc import MemorySize
from pyoram.oblivious_storage.tree.path_oram import \
    PathORAM

import tqdm

pyoram.config.SHOW_PROGRESS_BAR = True

# Set S3 bucket name here
# (by default, we pull this from the environment
# for testing purposes)
bucket_name = os.environ.get('PYORAM_AWS_TEST_BUCKET')

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
                  storage_type='s3'))))
    print("")

    print("Setting Up Path ORAM Storage")
    setup_start = time.time()
    with PathORAM.setup(storage_name,
                        block_size,
                        block_count,
                        storage_type='s3',
                        bucket_name=bucket_name,
                        ignore_existing=True) as f:
        print("Total Setup Time: %.2f s"
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
                  storage_type='s3',
                  bucket_name=bucket_name) as f:

        test_count = 100
        start_time = time.time()
        for t in tqdm.tqdm(list(range(test_count)),
                           desc="Running I/O Performance Test"):
            f.read_block(random.randint(0,f.block_count-1))
        stop_time = time.time()
        print("Current Stash Size: %s"
              % len(f.stash))
        print("Access Block Avg. Data Transmitted: %s (%.3fx)"
              % (MemorySize((f.bytes_sent + f.bytes_received)/float(test_count)),
                 (f.bytes_sent + f.bytes_received)/float(test_count)/float(block_size)))
        print("Fetch Block Avg. Latency: %.2f ms"
              % ((stop_time-start_time)/float(test_count)*1000))
        print("")

if __name__ == "__main__":
    main()                                             # pragma: no cover
