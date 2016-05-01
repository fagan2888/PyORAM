import os
import shutil
import tempfile

from pyoram.util.misc import \
    (save_private_key,
     load_private_key)
from pyoram.util.virtual_heap import \
    SizedVirtualHeap
from pyoram.encrypted_storage.encrypted_block_storage import \
    EncryptedBlockStorage
from pyoram.storage.boto3_s3_wrapper import \
    MockBoto3S3Wrapper

def main():
    thisdir = os.path.dirname(os.path.abspath(__file__))
    local_key_file = os.path.join(thisdir, "s3.key")
    storage_name = "blocks.bin"
    block_size = 4000
    block_count = 100
    bucket_name = thisdir

    with EncryptedBlockStorage.setup(storage_name,
                                     block_size=block_size,
                                     block_count=block_count,
                                     storage_type="s3",
                                     bucket_name=bucket_name,
                                     s3_wrapper=MockBoto3S3Wrapper,
                                     ignore_existing=True) as f:
        save_private_key(local_key_file, f.key)

    with EncryptedBlockStorage(storage_name,
                               key=load_private_key(local_key_file),
                               storage_type="s3",
                               s3_wrapper=MockBoto3S3Wrapper,
                               bucket_name=bucket_name) as f:
        print(f.block_count)
        print(f.block_size)

    os.remove(local_key_file)
    shutil.rmtree(os.path.join(bucket_name, storage_name))

if __name__ == "__main__":
    main()                                             # pragma: no cover
