import os
import tempfile

from pyoram.storage.virtualheap import \
    SizedVirtualHeap
from pyoram.storage.encrypted_heap_storage import \
    EncryptedHeapStorage

#
# get a unique filename in the current directory
# 
fid, tmpname = tempfile.mkstemp(dir=os.getcwd())
os.close(fid)
os.remove(tmpname)
print("Storage Name: %s" % (tmpname))

key_size = 32
user_header_data = b'a message'
base = 3
height = 2
block_size = 8
blocks_per_bucket=4
initialize = lambda i: \
    bytes(bytearray([i] * block_size * blocks_per_bucket))
vheap = SizedVirtualHeap(
    base,
    height,
    blocks_per_bucket=blocks_per_bucket)

key = EncryptedHeapStorage.setup(
    storage_name=tmpname,
    key_size=key_size,
    user_header_data=user_header_data,
    base=base,
    height=height,
    block_size=block_size,
    blocks_per_bucket=blocks_per_bucket,
    initialize=initialize)
assert os.path.exists(tmpname)

with EncryptedHeapStorage(
        encryption_key=key,
        storage_name=tmpname) as f:                          
    assert tmpname == f.storage_name
    assert f.user_header_data == user_header_data
    print(f.read_path(vheap.random_bucket()))

#
# cleanup
#
os.remove(tmpname)
