import os
import tempfile

from pyoram.util.virtual_heap import \
    SizedVirtualHeap
from pyoram.encrypted_storage.encrypted_heap_storage import \
    EncryptedHeapStorage

def main():
    #
    # get a unique filename in the current directory
    #
    fid, tmpname = tempfile.mkstemp(dir=os.getcwd())
    os.close(fid)
    os.remove(tmpname)
    print("Storage Name: %s" % (tmpname))

    key_size = 32
    header_data = b'a message'
    heap_base = 3
    heap_height = 2
    block_size = 8
    blocks_per_bucket=4
    initialize = lambda i: \
        bytes(bytearray([i] * block_size * blocks_per_bucket))
    vheap = SizedVirtualHeap(
        heap_base,
        heap_height,
        blocks_per_bucket=blocks_per_bucket)

    with EncryptedHeapStorage.setup(
            tmpname,
            block_size,
            heap_height,
            key_size=key_size,
            header_data=header_data,
            heap_base=heap_base,
            blocks_per_bucket=blocks_per_bucket,
            initialize=initialize) as f:
        assert tmpname == f.storage_name
        assert f.header_data == header_data
        print(f.read_path(vheap.random_bucket()))
        key = f.key
    assert os.path.exists(tmpname)

    with EncryptedHeapStorage(tmpname, key=key) as f:
        assert tmpname == f.storage_name
        assert f.header_data == header_data
        print(f.read_path(vheap.random_bucket()))

    #
    # cleanup
    #
    os.remove(tmpname)

if __name__ == "__main__":
    main()                                             # pragma: no cover
