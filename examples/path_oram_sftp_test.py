#
# This example demonstrates how to access an existing Path ORAM
# storage space through an SSH client using the Secure File Transfer
# Protocol (SFTP). This file should not be executed until the
# path_oram_sftp_setup.py example has been executed. The user is
# encouraged to tweak the settings for 'cached_levels',
# 'concurrency_level', and 'threadpool_size' to observe their effect
# on access latency.
#
# In order to run this example, you must provide a host
# (server) address along with valid login credentials
#

import os
import random
import time
import pickle
import multiprocessing

import pyoram
from pyoram.util.misc import MemorySize, load_private_key
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

def main():

    print("Loading key from file: %s.key"
          % (storage_name))
    key = load_private_key(storage_name+".key")
    print("Loading stash from file: %s.stash"
          % (storage_name))
    with open(storage_name+".stash", 'rb') as fstash:
        stash = pickle.load(fstash)
    print("Loading position map from file: %s.position"
          % (storage_name))
    with open(storage_name+".position", 'rb') as fpos:
        position_map = pickle.load(fpos)

    # Start an SSH client using paramiko
    print("Starting SSH Client")
    with paramiko.SSHClient() as ssh:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.load_system_host_keys()
        ssh.connect(ssh_host,
                    username=ssh_username,
                    password=ssh_password)

        with PathORAM(storage_name,
                      stash,
                      position_map,
                      key=key,
                      storage_type='sftp',
                      cached_levels=6,
                      concurrency_level=3,
                      threadpool_size=multiprocessing.cpu_count()*2,
                      sshclient=ssh) as f:

            try:

                test_count = 100
                start_time = time.time()
                for t in tqdm.tqdm(list(range(test_count)),
                                   desc="Running I/O Performance Test"):
                    f.read_block(random.randint(0,f.block_count-1))
                stop_time = time.time()
                print("Current Stash Size: %s"
                      % len(f.stash))
                print("Fetch Block Avg. Latency: %.2f ms"
                      % ((stop_time-start_time)/float(test_count)*1000))
                print("")

            finally:

                print("Saving stash to file: %s.stash"
                      % (storage_name))
                with open(storage_name+".stash", 'wb') as fstash:
                    pickle.dump(f.stash, fstash)
                print("Saving position map to file: %s.position"
                      % (storage_name))
                with open(storage_name+".position", 'wb') as fpos:
                    pickle.dump(f.position_map, fpos)

if __name__ == "__main__":
    main()                                             # pragma: no cover
