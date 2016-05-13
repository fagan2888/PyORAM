__all__ = ()

class HeapStorageInterface(object):

    def __enter__(self):
        return self
    def __exit__(self, *args):
        self.close()

    #
    # Abstract Interface
    #

    def clone_device(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover

    @classmethod
    def compute_storage_size(cls, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    @classmethod
    def setup(cls, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover

    @property
    def header_data(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    @property
    def bucket_count(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    @property
    def bucket_size(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    @property
    def blocks_per_bucket(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    @property
    def storage_name(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    @property
    def virtual_heap(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    @property
    def bucket_storage(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover

    def update_header_data(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    def close(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    def read_path(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover
    def write_path(self, *args, **kwds):
        raise NotImplementedError                      # pragma: no cover

    @property
    def bytes_sent(self):
        raise NotImplementedError                      # pragma: no cover
    @property
    def bytes_received(self):
        raise NotImplementedError                      # pragma: no cover
