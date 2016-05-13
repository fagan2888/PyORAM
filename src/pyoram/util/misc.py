import base64

def log2floor(n):
    """
    Returns the exact value of floor(log2(n)).
    No floating point calculations are used.
    Requires positive integer type.
    """
    assert n > 0
    return n.bit_length() - 1

def log2ceil(n):
    """
    Returns the exact value of ceil(log2(n)).
    No floating point calculations are used.
    Requires positive integer type.
    """
    if n == 1:
        return 0
    return log2floor(n-1) + 1

def intdivceil(x, y):
    """
    Returns the exact value of ceil(x // y).
    No floating point calculations are used.
    Requires positive integer types. The result
    is undefined if at least one of the inputs
    is floating point.
    """
    result = x // y
    if (x % y):
        result += 1
    return result

def save_private_key(filename, key):
    with open(filename, "wb") as f:
        f.write(base64.b64encode(key))

def load_private_key(filename):
    with open(filename, "rb") as f:
        return base64.b64decode(f.read())

class MemorySize(object):

    def __init__(self, numbytes):
        assert numbytes >= 0
        self.numbytes = numbytes

    def __str__(self):
        if self.B < 1:
            return "%.3f b" % (self.b)
        if self.KB < 1:
            return "%.3f B" % (self.B)
        if self.MB < 1:
            return "%.3f KB" % (self.KB)
        if self.GB < 1:
            return "%.3f MB" % (self.MB)
        if self.TB < 1:
            return "%.3f GB" % (self.GB)
        return "%.3f TB" % (self.TB)

    @property
    def b(self): return float(self.numbytes)*8
    @property
    def B(self): return float(self.numbytes)

    @property
    def KB(self): return self.B/1000.0
    @property
    def MB(self): return self.KB/1000.0
    @property
    def GB(self): return self.MB/1000.0
    @property
    def TB(self): return self.GB/1000.0

    @property
    def KiB(self): return self.B/1024.0
    @property
    def MiB(self): return self.KiB/1024.0
    @property
    def GiB(self): return self.MiB/1024.0
    @property
    def TiB(self): return self.GiB/1024.0
