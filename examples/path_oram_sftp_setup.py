#
# This example demonstrates how to setup an instance of Path ORAM
# locally and then transfer the storage to a server using a paramiko
# SSHClient. After executing this file, path_oram_sftp_test.py can be
# executed to run simple I/O performance tests using different caching
# settings.
#
# In order to run this example, you must provide a host
# (server) address along with valid login credentials
#

import os
import random
import time
import pickle

import pyoram
from pyoram.util.misc import MemorySize, save_private_key
from pyoram.oblivious_storage.tree.path_oram import \
    PathORAM

import paramiko
import tqdm

pyoram.config.SHOW_PROGRESS_BAR = True

# Set SSH login credentials here
# (by default, we pull these from the environment
# for testing purposes)
ssh_host = os.environ.get('PYORAM_SSH_TEST_HOST')
ssh_username = os.environ.get('PYORAM_SSH_TEST_USERNAME')
ssh_password = os.environ.get('PYORAM_SSH_TEST_PASSWORD')

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

    print("Setting Up Path ORAM Storage Locally")
    setup_start = time.time()
    with PathORAM.setup(storage_name,
                        block_size,
                        block_count,
                        storage_type='mmap',
                        ignore_existing=True) as f:
        print("Total Setup Time: %.2f s"
              % (time.time()-setup_start))
        print("Current Stash Size: %s"
              % len(f.stash))
        print("Total Data Transmission: %s"
              % (MemorySize(f.bytes_sent + f.bytes_received)))
        print("")

    print("Saving key to file: %s.key"
          % (storage_name))
    save_private_key(storage_name+".key", f.key)
    print("Saving stash to file: %s.stash"
          % (storage_name))
    with open(storage_name+".stash", 'wb') as fstash:
        pickle.dump(f.stash, fstash)
    print("Saving position map to file: %s.position"
          % (storage_name))
    with open(storage_name+".position", 'wb') as fpos:
        pickle.dump(f.position_map, fpos)

    # Start an SSH client using paramiko
    print("Starting SSH Client")
    with paramiko.SSHClient() as ssh:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.load_system_host_keys()
        ssh.connect(ssh_host,
                    username=ssh_username,
                    password=ssh_password)

        sftp = ssh.open_sftp()

        def my_hook(t):
            def inner(b, total):
                t.total = total
                t.update(b - inner.last_b)
                inner.last_b = b
            inner.last_b = 0
            return inner
        with tqdm.tqdm(desc="Transferring Storage",
                       unit='B',
                       unit_scale=True,
                       miniters=1) as t:
            sftp.put(storage_name,
                     storage_name,
                     callback=my_hook(t))
        sftp.close()

    print("Deleting Local Copy of Storage")
    os.remove(storage_name)

if __name__ == "__main__":
    main()                                             # pragma: no cover
