import os
import shutil
import unittest2
import tempfile

from pyoram.storage.block_storage import \
    BlockStorageTypeFactory
from pyoram.storage.block_storage_file import \
     BlockStorageFile
from pyoram.storage.block_storage_mmap import \
     BlockStorageMMap
from pyoram.storage.block_storage_s3 import \
     BlockStorageS3
from pyoram.storage.boto3_s3_wrapper import \
    (Boto3S3Wrapper,
     MockBoto3S3Wrapper)

from six.moves import xrange

thisdir = os.path.dirname(os.path.abspath(__file__))

try:
    import boto3
    has_boto3 = True
except:                                                # pragma: no cover
    has_boto3 = False                                  # pragma: no cover

class TestBlockStorageTypeFactory(unittest2.TestCase):

    def test_file(self):
        self.assertIs(BlockStorageTypeFactory('file'),
                      BlockStorageFile)

    def test_mmap(self):
        self.assertIs(BlockStorageTypeFactory('mmap'),
                      BlockStorageMMap)

    def test_s3(self):
        self.assertIs(BlockStorageTypeFactory('s3'),
                      BlockStorageS3)

    def test_invalid(self):
        with self.assertRaises(ValueError):
            BlockStorageTypeFactory(None)

    def test_register_invalid_name(self):
        with self.assertRaises(ValueError):
            BlockStorageTypeFactory.register_device(
                's3', BlockStorageFile)

    def test_register_invalid_type(self):
        with self.assertRaises(TypeError):
            BlockStorageTypeFactory.register_device(
                'new_str_type', str)

class _TestBlockStorage(object):

    _type = None
    _type_kwds = None

    @classmethod
    def _read_storage(cls, name):
        with open(name, 'rb') as f:
            return f.read()

    @classmethod
    def _check_exists(cls, name):
        return os.path.exists(name)

    @classmethod
    def _remove_storage(cls, name):
        if os.path.exists(name):
            if os.path.isdir(name):
                shutil.rmtree(name, ignore_errors=True)
            else:
                os.remove(name)

    @classmethod
    def _get_empty_existing(cls):
        return os.path.join(thisdir,
                            "baselines",
                            "exists.empty")

    @classmethod
    def _get_dummy_noexist(cls):
        fd, name = tempfile.mkstemp(dir=os.getcwd())
        os.close(fd)
        return name

    @classmethod
    def setUpClass(cls):
        assert cls._type is not None
        assert cls._type_kwds is not None
        cls._dummy_name = cls._get_dummy_noexist()
        if cls._check_exists(cls._dummy_name):
            cls._remove_storage(cls._dummy_name)
        if os.path.exists(cls._dummy_name):
            _TestBlockStorage.\
                _remove_storage(cls._dummy_name)       # pragma: no cover
        cls._block_size = 25
        cls._block_count = 5
        cls._testfname = cls.__name__ + "_testfile.bin"
        cls._blocks = []
        f = cls._type.setup(
            cls._testfname,
            block_size=cls._block_size,
            block_count=cls._block_count,
            initialize=lambda i: bytes(bytearray([i])*cls._block_size),
            ignore_existing=True,
            **cls._type_kwds)
        f.close()
        for i in range(cls._block_count):
            data = bytearray([i])*cls._block_size
            cls._blocks.append(data)

    @classmethod
    def tearDownClass(cls):
        cls._remove_storage(cls._testfname)
        cls._remove_storage(cls._dummy_name)

    def test_setup_fails(self):
        self.assertEqual(self._check_exists(self._dummy_name), False)
        with self.assertRaises(ValueError):
            self._type.setup(
                self._get_empty_existing(),
                block_size=10,
                block_count=10,
                **self._type_kwds)
        self.assertEqual(self._check_exists(self._dummy_name), False)
        with self.assertRaises(ValueError):
            self._type.setup(
                self._get_empty_existing(),
                block_size=10,
                block_count=10,
                ignore_existing=False,
                **self._type_kwds)
        self.assertEqual(self._check_exists(self._dummy_name), False)
        with self.assertRaises(ValueError):
            self._type.setup(self._dummy_name,
                             block_size=0,
                             block_count=1,
                             **self._type_kwds)
        self.assertEqual(self._check_exists(self._dummy_name), False)
        with self.assertRaises(ValueError):
            self._type.setup(self._dummy_name,
                             block_size=1,
                             block_count=0,
                             **self._type_kwds)
        self.assertEqual(self._check_exists(self._dummy_name), False)
        with self.assertRaises(TypeError):
            self._type.setup(self._dummy_name,
                             block_size=1,
                             block_count=1,
                             header_data=2,
                             **self._type_kwds)
        self.assertEqual(self._check_exists(self._dummy_name), False)
        # TODO: The multiprocessing module is bad
        #       about handling exceptions raised on the
        #       thread's stack.
        #with self.assertRaises(ValueError):
        #    def _init(i):
        #        raise ValueError
        #    self._type.setup(self._dummy_name,
        #                     block_size=1,
        #                     block_count=1,
        #                     initialize=_init,
        #                     **self._type_kwds)
        #self.assertEqual(self._check_exists(self._dummy_name), False)

    def test_setup(self):
        fname = ".".join(self.id().split(".")[1:])
        fname += ".bin"
        fname = os.path.join(thisdir, fname)
        self._remove_storage(fname)
        bsize = 10
        bcount = 11
        fsetup = self._type.setup(fname, bsize, bcount, **self._type_kwds)
        fsetup.close()
        flen = len(self._read_storage(fname))
        self.assertEqual(
            flen,
            self._type.compute_storage_size(bsize,
                                            bcount))
        self.assertEqual(
            flen >
            self._type.compute_storage_size(bsize,
                                            bcount,
                                            ignore_header=True),
            True)
        with self._type(fname, **self._type_kwds) as f:
            self.assertEqual(f.header_data, bytes())
            self.assertEqual(fsetup.header_data, bytes())
            self.assertEqual(f.block_size, bsize)
            self.assertEqual(fsetup.block_size, bsize)
            self.assertEqual(f.block_count, bcount)
            self.assertEqual(fsetup.block_count, bcount)
            self.assertEqual(f.storage_name, fname)
            self.assertEqual(fsetup.storage_name, fname)
        self._remove_storage(fname)

    def test_setup_withdata(self):
        fname = ".".join(self.id().split(".")[1:])
        fname += ".bin"
        fname = os.path.join(thisdir, fname)
        self._remove_storage(fname)
        bsize = 10
        bcount = 11
        header_data = bytes(bytearray([0,1,2]))
        fsetup = self._type.setup(fname,
                                  bsize,
                                  bcount,
                                  header_data=header_data,
                                  **self._type_kwds)
        fsetup.close()

        flen = len(self._read_storage(fname))
        self.assertEqual(
            flen,
            self._type.compute_storage_size(bsize,
                                            bcount,
                                            header_data=header_data))
        self.assertTrue(len(header_data) > 0)
        self.assertEqual(
            self._type.compute_storage_size(bsize,
                                            bcount) <
            self._type.compute_storage_size(bsize,
                                            bcount,
                                            header_data=header_data),
            True)
        self.assertEqual(
            flen >
            self._type.compute_storage_size(bsize,
                                            bcount,
                                            header_data=header_data,
                                            ignore_header=True),
            True)

        with self._type(fname, **self._type_kwds) as f:
            self.assertEqual(f.header_data, header_data)
            self.assertEqual(fsetup.header_data, header_data)
            self.assertEqual(f.block_size, bsize)
            self.assertEqual(fsetup.block_size, bsize)
            self.assertEqual(f.block_count, bcount)
            self.assertEqual(fsetup.block_count, bcount)
            self.assertEqual(f.storage_name, fname)
            self.assertEqual(fsetup.storage_name, fname)
        self._remove_storage(fname)

    def test_init_noexists(self):
        self.assertEqual(self._check_exists(self._dummy_name), False)
        with self.assertRaises(IOError):
            with self._type(self._dummy_name, **self._type_kwds) as f:
                pass                                   # pragma: no cover

    def test_init_exists(self):
        self.assertEqual(self._check_exists(self._testfname), True)
        databefore = self._read_storage(self._testfname)
        with self._type(self._testfname, **self._type_kwds) as f:
            self.assertEqual(f.block_size, self._block_size)
            self.assertEqual(f.block_count, self._block_count)
            self.assertEqual(f.storage_name, self._testfname)
            self.assertEqual(f.header_data, bytes())
        self.assertEqual(self._check_exists(self._testfname), True)
        dataafter = self._read_storage(self._testfname)
        self.assertEqual(databefore, dataafter)

    def test_read_block(self):
        with self._type(self._testfname, **self._type_kwds) as f:
            for i, data in enumerate(self._blocks):
                self.assertEqual(list(bytearray(f.read_block(i))),
                                 list(self._blocks[i]))
            for i, data in enumerate(self._blocks):
                self.assertEqual(list(bytearray(f.read_block(i))),
                                 list(self._blocks[i]))
            for i, data in reversed(list(enumerate(self._blocks))):
                self.assertEqual(list(bytearray(f.read_block(i))),
                                 list(self._blocks[i]))
            for i, data in reversed(list(enumerate(self._blocks))):
                self.assertEqual(list(bytearray(f.read_block(i))),
                                 list(self._blocks[i]))
        with self._type(self._testfname, **self._type_kwds) as f:
            self.assertEqual(list(bytearray(f.read_block(0))),
                             list(self._blocks[0]))
            self.assertEqual(list(bytearray(f.read_block(self._block_count-1))),
                             list(self._blocks[-1]))

    def test_write_block(self):
        data = bytearray([self._block_count])*self._block_size
        self.assertEqual(len(data) > 0, True)
        with self._type(self._testfname, **self._type_kwds) as f:
            for i in xrange(self._block_count):
                self.assertNotEqual(list(bytearray(f.read_block(i))),
                                    list(data))
            for i in xrange(self._block_count):
                f.write_block(i, bytes(data))
            for i in xrange(self._block_count):
                self.assertEqual(list(bytearray(f.read_block(i))),
                                 list(data))
            for i, block in enumerate(self._blocks):
                f.write_block(i, bytes(block))

    def test_read_blocks(self):
        with self._type(self._testfname, **self._type_kwds) as f:
            data = f.read_blocks(list(xrange(self._block_count)))
            self.assertEqual(len(data), self._block_count)
            for i, block in enumerate(data):
                self.assertEqual(list(bytearray(block)),
                                 list(self._blocks[i]))
            data = f.read_blocks([0])
            self.assertEqual(len(data), 1)
            self.assertEqual(list(bytearray(data[0])),
                             list(self._blocks[0]))
            self.assertEqual(len(self._blocks) > 1, True)
            data = f.read_blocks(list(xrange(1, self._block_count)) + [0])
            self.assertEqual(len(data), self._block_count)
            for i, block in enumerate(data[:-1], 1):
                self.assertEqual(list(bytearray(block)),
                                 list(self._blocks[i]))
            self.assertEqual(list(bytearray(data[-1])),
                             list(self._blocks[0]))

    def test_yield_blocks(self):
        with self._type(self._testfname, **self._type_kwds) as f:
            data = list(f.yield_blocks(list(xrange(self._block_count))))
            self.assertEqual(len(data), self._block_count)
            for i, block in enumerate(data):
                self.assertEqual(list(bytearray(block)),
                                 list(self._blocks[i]))
            data = list(f.yield_blocks([0]))
            self.assertEqual(len(data), 1)
            self.assertEqual(list(bytearray(data[0])),
                             list(self._blocks[0]))
            self.assertEqual(len(self._blocks) > 1, True)
            data = list(f.yield_blocks(list(xrange(1, self._block_count)) + [0]))
            self.assertEqual(len(data), self._block_count)
            for i, block in enumerate(data[:-1], 1):
                self.assertEqual(list(bytearray(block)),
                                 list(self._blocks[i]))
            self.assertEqual(list(bytearray(data[-1])),
                             list(self._blocks[0]))

    def test_write_blocks(self):
        data = [bytearray([self._block_count])*self._block_size
                for i in xrange(self._block_count)]
        with self._type(self._testfname, **self._type_kwds) as f:
            orig = f.read_blocks(list(xrange(self._block_count)))
            self.assertEqual(len(orig), self._block_count)
            for i, block in enumerate(orig):
                self.assertEqual(list(bytearray(block)),
                                 list(self._blocks[i]))
            f.write_blocks(list(xrange(self._block_count)),
                           [bytes(b) for b in data])
            new = f.read_blocks(list(xrange(self._block_count)))
            self.assertEqual(len(new), self._block_count)
            for i, block in enumerate(new):
                self.assertEqual(list(bytearray(block)),
                                 list(data[i]))
            f.write_blocks(list(xrange(self._block_count)),
                           [bytes(b) for b in self._blocks])
            orig = f.read_blocks(list(xrange(self._block_count)))
            self.assertEqual(len(orig), self._block_count)
            for i, block in enumerate(orig):
                self.assertEqual(list(bytearray(block)),
                                 list(self._blocks[i]))

    def test_update_header_data(self):
        fname = ".".join(self.id().split(".")[1:])
        fname += ".bin"
        fname = os.path.join(thisdir, fname)
        self._remove_storage(fname)
        bsize = 10
        bcount = 11
        header_data = bytes(bytearray([0,1,2]))
        fsetup = self._type.setup(fname,
                                  block_size=bsize,
                                  block_count=bcount,
                                  header_data=header_data,
                                  **self._type_kwds)
        fsetup.close()
        new_header_data = bytes(bytearray([1,1,1]))
        with self._type(fname, **self._type_kwds) as f:
            self.assertEqual(f.header_data, header_data)
            f.update_header_data(new_header_data)
            self.assertEqual(f.header_data, new_header_data)
        with self._type(fname, **self._type_kwds) as f:
            self.assertEqual(f.header_data, new_header_data)
        with self.assertRaises(ValueError):
            with self._type(fname, **self._type_kwds) as f:
                f.update_header_data(bytes(bytearray([1,1])))
        with self.assertRaises(ValueError):
            with self._type(fname, **self._type_kwds) as f:
                f.update_header_data(bytes(bytearray([1,1,1,1])))
        with self._type(fname, **self._type_kwds) as f:
            self.assertEqual(f.header_data, new_header_data)
        self._remove_storage(fname)

    def test_locked_flag(self):
        with self._type(self._testfname, **self._type_kwds) as f:
            with self.assertRaises(IOError):
                with self._type(self._testfname, **self._type_kwds) as f1:
                    pass                               # pragma: no cover
            with self.assertRaises(IOError):
                with self._type(self._testfname, **self._type_kwds) as f1:
                    pass                               # pragma: no cover
            with self._type(self._testfname, ignore_lock=True, **self._type_kwds) as f1:
                pass
            with self.assertRaises(IOError):
                with self._type(self._testfname, **self._type_kwds) as f1:
                    pass                               # pragma: no cover
            with self._type(self._testfname, ignore_lock=True, **self._type_kwds) as f1:
                pass
            with self._type(self._testfname, ignore_lock=True, **self._type_kwds) as f1:
                pass
        with self._type(self._testfname, ignore_lock=True, **self._type_kwds) as f:
            pass

    def test_read_block_cloned(self):
        with self._type(self._testfname, **self._type_kwds) as forig:
            with forig.clone_device() as f:
                for i, data in enumerate(self._blocks):
                    self.assertEqual(list(bytearray(f.read_block(i))),
                                     list(self._blocks[i]))
                for i, data in enumerate(self._blocks):
                    self.assertEqual(list(bytearray(f.read_block(i))),
                                     list(self._blocks[i]))
                for i, data in reversed(list(enumerate(self._blocks))):
                    self.assertEqual(list(bytearray(f.read_block(i))),
                                     list(self._blocks[i]))
                for i, data in reversed(list(enumerate(self._blocks))):
                    self.assertEqual(list(bytearray(f.read_block(i))),
                                     list(self._blocks[i]))
            with forig.clone_device() as f:
                self.assertEqual(list(bytearray(f.read_block(0))),
                                 list(self._blocks[0]))
                self.assertEqual(list(bytearray(f.read_block(self._block_count-1))),
                                 list(self._blocks[-1]))

    def test_write_block_cloned(self):
        data = bytearray([self._block_count])*self._block_size
        self.assertEqual(len(data) > 0, True)
        with self._type(self._testfname, **self._type_kwds) as forig:
            with forig.clone_device() as f:
                for i in xrange(self._block_count):
                    self.assertNotEqual(list(bytearray(f.read_block(i))),
                                        list(data))
                for i in xrange(self._block_count):
                    f.write_block(i, bytes(data))
                for i in xrange(self._block_count):
                    self.assertEqual(list(bytearray(f.read_block(i))),
                                     list(data))
                for i, block in enumerate(self._blocks):
                    f.write_block(i, bytes(block))

    def test_read_blocks_cloned(self):
        with self._type(self._testfname, **self._type_kwds) as forig:
            with forig.clone_device() as f:
                data = f.read_blocks(list(xrange(self._block_count)))
                self.assertEqual(len(data), self._block_count)
                for i, block in enumerate(data):
                    self.assertEqual(list(bytearray(block)),
                                     list(self._blocks[i]))
                data = f.read_blocks([0])
                self.assertEqual(len(data), 1)
                self.assertEqual(list(bytearray(data[0])),
                                 list(self._blocks[0]))
                self.assertEqual(len(self._blocks) > 1, True)
                data = f.read_blocks(list(xrange(1, self._block_count)) + [0])
                self.assertEqual(len(data), self._block_count)
                for i, block in enumerate(data[:-1], 1):
                    self.assertEqual(list(bytearray(block)),
                                     list(self._blocks[i]))
                self.assertEqual(list(bytearray(data[-1])),
                                 list(self._blocks[0]))

    def test_write_blocks_cloned(self):
        data = [bytearray([self._block_count])*self._block_size
                for i in xrange(self._block_count)]
        with self._type(self._testfname, **self._type_kwds) as forig:
            with forig.clone_device() as f:
                orig = f.read_blocks(list(xrange(self._block_count)))
                self.assertEqual(len(orig), self._block_count)
                for i, block in enumerate(orig):
                    self.assertEqual(list(bytearray(block)),
                                     list(self._blocks[i]))
                f.write_blocks(list(xrange(self._block_count)),
                               [bytes(b) for b in data])
                new = f.read_blocks(list(xrange(self._block_count)))
                self.assertEqual(len(new), self._block_count)
                for i, block in enumerate(new):
                    self.assertEqual(list(bytearray(block)),
                                     list(data[i]))
                f.write_blocks(list(xrange(self._block_count)),
                               [bytes(b) for b in self._blocks])
                orig = f.read_blocks(list(xrange(self._block_count)))
                self.assertEqual(len(orig), self._block_count)
                for i, block in enumerate(orig):
                    self.assertEqual(list(bytearray(block)),
                                     list(self._blocks[i]))

class TestBlockStorageFile(_TestBlockStorage,
                           unittest2.TestCase):
    _type = BlockStorageFile
    _type_kwds = {}

class TestBlockStorageMMap(_TestBlockStorage,
                           unittest2.TestCase):
    _type = BlockStorageMMap
    _type_kwds = {}

class _TestBlockStorageS3Mock(_TestBlockStorage):
    _type = BlockStorageS3
    _type_kwds = {}

    @classmethod
    def _read_storage(cls, name):
        import glob
        data = bytearray()
        prefix_len = len(os.path.join(name,"b"))
        nblocks = max(int(bfile[prefix_len:]) for bfile in glob.glob(name+"/b*")) + 1
        with open(os.path.join(name, BlockStorageS3._index_name), 'rb') as f:
            data.extend(f.read())
        for i in range(nblocks):
            with open(os.path.join(name, "b"+str(i)), 'rb') as f:
                data.extend(f.read())
        return data

    def test_init_exists_no_bucket(self):
        self.assertEqual(self._check_exists(self._testfname), True)
        databefore = self._read_storage(self._testfname)
        with self._type(self._testfname, **self._type_kwds) as f:
            self.assertEqual(f.block_size, self._block_size)
            self.assertEqual(f.block_count, self._block_count)
            self.assertEqual(f.storage_name, self._testfname)
            self.assertEqual(f.header_data, bytes())
        self.assertEqual(self._check_exists(self._testfname), True)
        dataafter = self._read_storage(self._testfname)
        self.assertEqual(databefore, dataafter)
        kwds = dict(self._type_kwds)
        del kwds['bucket_name']
        with self.assertRaises(ValueError):
            with self._type(self._testfname, **kwds) as f:
                pass                                   # pragma: no cover
        dataafter = self._read_storage(self._testfname)
        self.assertEqual(databefore, dataafter)

    def test_setup_fails_no_bucket(self):
        self.assertEqual(self._check_exists(self._dummy_name), False)
        kwds = dict(self._type_kwds)
        del kwds['bucket_name']
        with self.assertRaises(ValueError):
            self._type.setup(self._dummy_name,
                             block_size=1,
                             block_count=1,
                             **kwds)
        self.assertEqual(self._check_exists(self._dummy_name), False)

    def test_setup_ignore_existing(self):
        self.assertEqual(self._check_exists(self._dummy_name), False)
        with self._type.setup(self._dummy_name,
                              block_size=1,
                              block_count=1,
                              **self._type_kwds) as f:
            pass
        self.assertEqual(self._check_exists(self._dummy_name), True)
        with self.assertRaises(ValueError):
            with self._type.setup(self._dummy_name,
                                  block_size=1,
                                  block_count=1,
                                  **self._type_kwds) as f:
                pass                                   # pragma: no cover
        self.assertEqual(self._check_exists(self._dummy_name), True)
        with self._type.setup(self._dummy_name,
                              block_size=1,
                              block_count=1,
                              ignore_existing=True,
                              **self._type_kwds) as f:
            pass
        self.assertEqual(self._check_exists(self._dummy_name), True)
        self._remove_storage(self._dummy_name)

class TestBlockStorageS3MockNoThreads(_TestBlockStorageS3Mock,
                                      unittest2.TestCase):
    _type_kwds = {'s3_wrapper': MockBoto3S3Wrapper,
                  'bucket_name': '.',
                  'threadpool_size': 0}

class TestBlockStorageS3Mock(_TestBlockStorageS3Mock,
                             unittest2.TestCase):
    _type_kwds = {'s3_wrapper': MockBoto3S3Wrapper,
                  'bucket_name': '.',
                  'threadpool_size': 4}

@unittest2.skipIf((os.environ.get('PYORAM_AWS_TEST_BUCKET') is None) or \
                 (not has_boto3),
                 "No PYORAM_AWS_TEST_BUCKET defined in environment or "
                 "boto3 is not available")
class TestBlockStorageS3(_TestBlockStorage,
                         unittest2.TestCase):
    _type = BlockStorageS3
    _type_kwds = {'bucket_name': os.environ.get('PYORAM_AWS_TEST_BUCKET')}

    @classmethod
    def _read_storage(cls, name):
        data = bytearray()
        s3 = Boto3S3Wrapper(cls._type_kwds['bucket_name'])
        prefix_len = len(name+"/b")
        nblocks = 1 + max(int(obj.key[prefix_len:]) for obj
                          in s3._bucket.objects.filter(Prefix=name+"/b"))
        data.extend(s3.download(name+"/"+BlockStorageS3._index_name))
        for i in range(nblocks):
            data.extend(s3.download(name+"/b"+str(i)))
        return data

    @classmethod
    def _check_exists(cls, name):
        return Boto3S3Wrapper(cls._type_kwds['bucket_name']).exists(name)

    @classmethod
    def _remove_storage(cls, name):
        Boto3S3Wrapper(cls._type_kwds['bucket_name']).clear(name)

    @classmethod
    def _get_empty_existing(cls):
        return "exists.empty"

    @classmethod
    def _get_dummy_noexist(cls):
        s3 = Boto3S3Wrapper(cls._type_kwds['bucket_name'])
        fd, name = tempfile.mkstemp(dir=os.getcwd())
        os.close(fd)
        os.remove(name)
        while s3.exists(name):
            fd, name = tempfile.mkstemp(dir=os.getcwd())
            os.close(fd)
            os.remove(name)
        return name

if __name__ == "__main__":
    unittest2.main()                                    # pragma: no cover
