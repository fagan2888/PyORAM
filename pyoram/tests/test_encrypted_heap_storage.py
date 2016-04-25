import os
import unittest

from pyoram.storage.virtualheap import \
    SizedVirtualHeap
from pyoram.storage.encrypted_heap_storage import \
    EncryptedHeapStorage
from pyoram.crypto.aesctr import AESCTR

from six.moves import xrange

thisdir = os.path.dirname(os.path.abspath(__file__))

class TestEncryptedHeapStorage(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._block_size = 25
        cls._blocks_per_bucket = 3
        cls._heap_base = 4
        cls._heap_height = 2
        cls._bucket_count = \
            ((cls._heap_base**(cls._heap_height+1)) - 1)//(cls._heap_base-1)
        cls._block_count = cls._bucket_count * \
                           cls._blocks_per_bucket
        cls._testfname = cls.__name__ + "_testfile.bin"
        cls._buckets = []
        cls._type_name = "file"
        f = EncryptedHeapStorage.setup(
            cls._testfname,
            block_size=cls._block_size,
            heap_height=cls._heap_height,
            key_size=AESCTR.key_sizes[-1],
            heap_base=cls._heap_base,
            blocks_per_bucket=cls._blocks_per_bucket,
            storage_type=cls._type_name,
            initialize=lambda i: bytes(bytearray([i]) * \
                                       cls._block_size * \
                                       cls._blocks_per_bucket),
            ignore_existing=True)
        f.close()
        cls._key = f.key
        for i in range(cls._bucket_count):
            data = bytearray([i]) * \
                   cls._block_size * \
                   cls._blocks_per_bucket
            cls._buckets.append(data)


    @classmethod
    def tearDownClass(cls):
        try:
            os.remove(cls._testfname)
        except OSError:                                # pragma: no cover
            pass                                       # pragma: no cover
        pass

    def test_setup_fails(self):
        dummy_name = "sdfsdfsldkfjwerwerfsdfsdfsd"
        self.assertEquals(os.path.exists(dummy_name), False)
        with self.assertRaises(ValueError):
            EncryptedHeapStorage.setup(
                os.path.join(thisdir,
                             "baselines",
                             "exists.empty"),
                block_size=10,
                heap_height=1,
                key_size=AESCTR.key_sizes[-1],
                blocks_per_bucket=1,
                storage_type=self._type_name)
        with self.assertRaises(ValueError):
            EncryptedHeapStorage.setup(
                os.path.join(thisdir,
                             "baselines",
                             "exists.empty"),
                block_size=10,
                heap_height=1,
                key_size=AESCTR.key_sizes[-1],
                blocks_per_bucket=1,
                storage_type=self._type_name,
                ignore_existing=False)
        # bad block_size
        with self.assertRaises(ValueError):
            EncryptedHeapStorage.setup(
                dummy_name,
                block_size=0,
                heap_height=1,
                key_size=AESCTR.key_sizes[-1],
                blocks_per_bucket=1,
                storage_type=self._type_name)
        # bad heap_height
        with self.assertRaises(ValueError):
            EncryptedHeapStorage.setup(
                dummy_name,
                block_size=1,
                heap_height=-1,
                blocks_per_bucket=1,
                storage_type=self._type_name)
        # bad blocks_per_bucket
        with self.assertRaises(ValueError):
            EncryptedHeapStorage.setup(
                dummy_name,
                block_size=1,
                heap_height=1,
                key_size=AESCTR.key_sizes[-1],
                blocks_per_bucket=0,
                storage_type=self._type_name)
        # bad heap_base
        with self.assertRaises(ValueError):
            EncryptedHeapStorage.setup(
                dummy_name,
                block_size=1,
                heap_height=1,
                key_size=AESCTR.key_sizes[-1],
                blocks_per_bucket=1,
                heap_base=1,
                storage_type=self._type_name)
        # bad user_header_data
        with self.assertRaises(TypeError):
            EncryptedHeapStorage.setup(
                dummy_name,
                block_size=1,
                heap_height=1,
                key_size=AESCTR.key_sizes[-1],
                blocks_per_bucket=1,
                storage_type=self._type_name,
                user_header_data=2)
        # uses block_count
        with self.assertRaises(ValueError):
            EncryptedHeapStorage.setup(
                dummy_name,
                block_size=1,
                heap_height=1,
                key_size=AESCTR.key_sizes[-1],
                blocks_per_bucket=1,
                block_count=1,
                storage_type=self._type_name)

    def test_setup(self):
        fname = ".".join(self.id().split(".")[1:])
        fname += ".bin"
        fname = os.path.join(thisdir, fname)
        if os.path.exists(fname):
            os.remove(fname)                           # pragma: no cover
        bsize = 10
        heap_height = 2
        blocks_per_bucket = 1
        fsetup = EncryptedHeapStorage.setup(
            fname,
            block_size=bsize,
            heap_height=heap_height,
            key_size=AESCTR.key_sizes[-1],
            blocks_per_bucket=blocks_per_bucket)
        fsetup.close()
        with EncryptedHeapStorage(
                fname,
                key=fsetup.key,
                storage_type=self._type_name) as f:
            self.assertEqual(f.user_header_data, bytes())
            self.assertEqual(fsetup.user_header_data, bytes())
            self.assertEqual(f.key, fsetup.key)
            self.assertEqual(f.blocks_per_bucket,
                             blocks_per_bucket)
            self.assertEqual(fsetup.blocks_per_bucket,
                             blocks_per_bucket)
            self.assertEqual(f.bucket_count,
                             2**(heap_height+1) - 1)
            self.assertEqual(fsetup.bucket_count,
                             2**(heap_height+1) - 1)
            self.assertEqual(f.bucket_size,
                             bsize * blocks_per_bucket)
            self.assertEqual(fsetup.bucket_size,
                             bsize * blocks_per_bucket)
            self.assertEqual(f.storage_name, fname)
            self.assertEqual(fsetup.storage_name, fname)
        os.remove(fname)

    def test_setup_withdata(self):
        fname = ".".join(self.id().split(".")[1:])
        fname += ".bin"
        fname = os.path.join(thisdir, fname)
        if os.path.exists(fname):
            os.remove(fname)                           # pragma: no cover
        bsize = 10
        heap_height = 2
        blocks_per_bucket = 1
        user_header_data = bytes(bytearray([0,1,2]))
        fsetup = EncryptedHeapStorage.setup(
            fname,
            block_size=bsize,
            heap_height=heap_height,
            key_size=AESCTR.key_sizes[-1],
            blocks_per_bucket=blocks_per_bucket,
            user_header_data=user_header_data)
        fsetup.close()
        with EncryptedHeapStorage(
                fname,
                key=fsetup.key,
                storage_type=self._type_name) as f:
            self.assertEqual(f.user_header_data, user_header_data)
            self.assertEqual(fsetup.user_header_data, user_header_data)
            self.assertEqual(f.key, fsetup.key)
            self.assertEqual(f.blocks_per_bucket,
                             blocks_per_bucket)
            self.assertEqual(fsetup.blocks_per_bucket,
                             blocks_per_bucket)
            self.assertEqual(f.bucket_count,
                             2**(heap_height+1) - 1)
            self.assertEqual(fsetup.bucket_count,
                             2**(heap_height+1) - 1)
            self.assertEqual(f.bucket_size,
                             bsize * blocks_per_bucket)
            self.assertEqual(fsetup.bucket_size,
                             bsize * blocks_per_bucket)
            self.assertEqual(f.storage_name, fname)
            self.assertEqual(fsetup.storage_name, fname)
        os.remove(fname)

    def test_init_noexists(self):
        self.assertEqual(
            not os.path.exists(self._testfname+"SDFSDFSDFSFSDFS"),
            True)
        with self.assertRaises(IOError):
            with EncryptedHeapStorage(
                    self._testfname+"SDFSDFSDFSFSDFS",
                    key=self._key,
                    storage_type=self._type_name) as f:
                pass                                   # pragma: no cover

    def test_init_exists(self):
        self.assertEqual(os.path.exists(self._testfname), True)
        with open(self._testfname, 'rb') as f:
            databefore = f.read()
        with EncryptedHeapStorage(
                self._testfname,
                key=self._key,
                storage_type=self._type_name) as f:
            encrypted_size = f.ciphertext_bucket_size * \
                             self._bucket_count
            self.assertEqual(f.key, self._key)
            self.assertEqual(f.bucket_size,
                             self._block_size * \
                             self._blocks_per_bucket)
            self.assertEqual(f.bucket_count,
                             self._bucket_count)
            self.assertEqual(f.storage_name, self._testfname)
            self.assertEqual(f.user_header_data, bytes())
            self.assertNotEqual(self._block_size * \
                                self._blocks_per_bucket,
                                f.ciphertext_bucket_size)
        self.assertEqual(len(databefore) >= encrypted_size, True)
        self.assertEqual(os.path.exists(self._testfname), True)
        with open(self._testfname, 'rb') as f:
            dataafter = f.read()
        self.assertEqual(databefore, dataafter)

    def test_read_path(self):

        with EncryptedHeapStorage(
                self._testfname,
                key=self._key,
                storage_type=self._type_name) as f:
            self.assertEqual(
                f.virtual_heap.first_bucket_at_level(0), 0)
            self.assertNotEqual(
                f.virtual_heap.last_leaf_bucket(), 0)
            for b in range(f.virtual_heap.first_bucket_at_level(0),
                           f.virtual_heap.last_leaf_bucket()+1):
                data = f.read_path(b)
                bucket_path = f.virtual_heap.Node(b).\
                              bucket_path_from_root()
                self.assertEqual(f.virtual_heap.Node(b).level+1,
                                 len(bucket_path))
                for i, bucket in zip(bucket_path, data):
                    self.assertEqual(list(bytearray(bucket)),
                                     list(self._buckets[i]))

    def test_write_path(self):
        data = [bytearray([self._bucket_count]) * \
                self._block_size * \
                self._blocks_per_bucket
                for i in xrange(self._block_count)]
        with EncryptedHeapStorage(
                self._testfname,
                key=self._key,
                storage_type=self._type_name) as f:
            self.assertEqual(
                f.virtual_heap.first_bucket_at_level(0), 0)
            self.assertNotEqual(
                f.virtual_heap.last_leaf_bucket(), 0)
            for b in range(f.virtual_heap.first_bucket_at_level(0),
                           f.virtual_heap.last_leaf_bucket()+1):
                orig = f.read_path(b)
                bucket_path = f.virtual_heap.Node(b).\
                              bucket_path_from_root()
                self.assertNotEqual(len(bucket_path), 0)
                self.assertEqual(f.virtual_heap.Node(b).level+1,
                                 len(bucket_path))
                self.assertEqual(len(orig), len(bucket_path))
                for i, bucket in zip(bucket_path, orig):
                    self.assertEqual(list(bytearray(bucket)),
                                     list(self._buckets[i]))
                f.write_path(b, [bytes(data[i])
                                 for i in bucket_path])

                new = f.read_path(b)
                self.assertEqual(len(new), len(bucket_path))
                for i, bucket in zip(bucket_path, new):
                    self.assertEqual(list(bytearray(bucket)),
                                     list(data[i]))

                f.write_path(b, [bytes(self._buckets[i])
                                 for i in bucket_path])

                orig = f.read_path(b)
                self.assertEqual(len(orig), len(bucket_path))
                for i, bucket in zip(bucket_path, orig):
                    self.assertEqual(list(bytearray(bucket)),
                                     list(self._buckets[i]))

if __name__ == "__main__":
    unittest.main()                                    # pragma: no cover
