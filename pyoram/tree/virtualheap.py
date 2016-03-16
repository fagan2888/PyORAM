import os
import random
import string

from six.moves import range

from pyoram.tree._virtualheap import lib as _clib
from pyoram.util import log2floor

numerals = ''.join([c for c in string.printable \
                  if ((c not in string.whitespace) and \
                      (c != '+') and (c != '-') and \
                      (c != '"') and (c != "'") and \
                      (c != '\\') and (c != '/'))])
numeral_index = dict((c,i) for i,c in enumerate(numerals))

def MaxKLabeled():
    """
    The maximum heap base for which base k labels
    can be produced.
    """
    return len(numerals)

def Base10IntegerToBaseKString(k, x):
    """Convert an integer into a base k string."""
    if not (2 <= k <= MaxKLabeled()):
        raise ValueError("k must be in range [2, %d]: %s"
                         % (MaxKLabeled(), k))
    return ((x == 0) and numerals[0]) or \
        (Base10IntegerToBaseKString(k, x // k).\
         lstrip(numerals[0]) + numerals[x % k])

def BaseKStringToBase10Integer(k, x):
    """Convert a base k string into an integer."""
    assert 1 < k <= MaxKLabeled()
    return sum(numeral_index[c]*(k**i)
               for i, c in enumerate(reversed(x)))

# _clib defines a faster version of this function
def CalculateBucketLevel(k, b):
    """
    Calculate the level in which a 0-based bucket
    lives inside of a k-ary heap.
    """
    assert k >= 2
    if k == 2:
        return log2floor(b+1)
    v = (k - 1) * (b + 1) + 1
    h = 0
    while k**(h+1) < v:
        h += 1
    return h

# _clib defines a faster version of this function
def CalculateLastCommonLevel(k, b1, b2):
    """
    Calculate the highest level after which the
    paths from the root to these buckets diverge.
    """
    l1 = CalculateBucketLevel(k, b1)
    l2 = CalculateBucketLevel(k, b2)
    while l1 > l2:
        b1 = (b1-1)//k
        l1 -= 1
    while l2 > l1:
        b2 = (b2-1)//k
        l2 -= 1
    while b1 != b2:
        b1 = (b1-1)//k
        b2 = (b2-1)//k
        l1 -= 1
    return l1

def CalculateNecessaryHeapHeight(k, n):
    """
    Calculate the necessary k-ary heap height
    to store n buckets.
    """
    assert n >= 1
    return CalculateBucketLevel(k, n-1) + 1

def CalculateBucketCountInHeapWithHeight(k, h):
    """
    Calculate the number of buckets in a
    k-ary heap of height h.
    """
    assert h >= 0
    return ((k**(h+1)) - 1) // (k - 1)

def CalculateBucketCountInHeapAtLevel(k, l):
    """
    Calculate the number of buckets in a
    k-ary heap at level l.
    """
    assert l >= 0
    return k**l

def CalculateLeafBucketCountInHeapWithHeight(k, h):
    """
    Calculate the number of buckets in the
    leaf-level of a k-ary heap of height h.
    """
    return CalculateBucketCountInHeapAtLevel(k, h)

def create_node_type(k):

    class VirtualHeapNode(object):
        __slots__ = ("bucket", "level")
        def __init__(self, bucket):
            assert bucket >= 0
            self.bucket = bucket
            self.level = _clib.CalculateBucketLevel(self.k, self.bucket)

        def __hash__(self):
            return self.bucket.__hash__()
        def __lt__(self, other):
            return self.bucket < other
        def __le__(self, other):
            return self.bucket <= other
        def __eq__(self, other):
            return self.bucket == other
        def __ne__(self, other):
            return self.bucket != other
        def __gt__(self, other):
            return self.bucket > other
        def __ge__(self, other):
            return self.bucket >= other
        def LastCommonLevel(self, n):
            return _clib.CalculateLastCommonLevel(self.k,
                                                  self.bucket,
                                                  n.bucket)
        def ChildNode(self, c):
            assert type(c) is int
            assert 0 <= c < self.k
            return VirtualHeapNode(self.k * self.bucket + 1 + c)
        def ParentNode(self):
            if self.bucket != 0:
                return VirtualHeapNode((self.bucket - 1)//self.k)
            return None
        def AncestorNodeAtLevel(self, level):
            if level > self.level:
                return None
            current = self
            while current.level != level:
                current = current.ParentNode()
            return current
        def GenerateBucketPathToRoot(self):
            bucket = self.bucket
            yield bucket
            while bucket != 0:
                bucket = (bucket - 1)//self.k
                yield bucket
        def BucketPath(self):
            return list(reversed(list(self.GenerateBucketPathToRoot())))

        #
        # Expensive Functions
        #
        def __repr__(self):
            try:
                label = self.Label()
            except ValueError:
                # presumably, k is too large
                label = "<unknown>"
            return ("VirtualHeapNode(k=%s, bucket=%s, level=%s, label=%r)"
                    % (self.k, self.bucket, self.level, label))
        def __str__(self):
            """Returns a tuple (<level>, <bucket offset within level>)."""
            if self.bucket != 0:
                return ("(%s, %s)"
                        % (self.level,
                           self.bucket -
                           CalculateBucketCountInHeapWithHeight(self.k,
                                                                self.level-1)))
            assert self.level == 0
            return "(0, 0)"

        def Label(self):
            assert 0 <= self.bucket
            if self.level == 0:
                return ''
            b_offset = self.bucket - \
                       CalculateBucketCountInHeapWithHeight(self.k,
                                                            self.level-1)
            basek = Base10IntegerToBaseKString(self.k, b_offset)
            return basek.zfill(self.level)

        def IsNodeOnPath(self, n):
            if n.level <= self.level:
                n_label = n.Label()
                if n_label == "":
                    return True
                return self.Label().startswith(n_label)
            return False

    VirtualHeapNode.k = k

    return VirtualHeapNode

class VirtualHeap(object):

    clib = _clib

    def __init__(self, k, bucket_size=1):
        assert 1 < k
        assert bucket_size >= 1
        self._k = k
        self._bucket_size = bucket_size
        self.Node = create_node_type(k)

    @property
    def k(self):
        return self._k
    def NodeLabelToBucket(self, label):
        if len(label) > 0:
            return \
                (CalculateBucketCountInHeapWithHeight(self.k,
                                                      len(label)-1) +
                 BaseKStringToBase10Integer(self.k, label))
        return 0

    #
    # Buckets (0-based integer, equivalent to slot for heap with bucket_size=1)
    #

    def BucketSize(self):
        return self._bucket_size
    def BucketCountAtLevel(self, l):
        return CalculateBucketCountInHeapAtLevel(self.k, l)
    def FirstBucketAtLevel(self, l):
        if l > 0:
            return CalculateBucketCountInHeapWithHeight(self.k, l-1)
        return 0
    def LastBucketAtLevel(self, l):
        return CalculateBucketCountInHeapWithHeight(self.k, l) - 1
    def BucketToSlot(self, b):
        assert b >= 0
        return b * self.BucketSize()
    def RandomBucketUpToLevel(self, l):
        return random.randint(self.FirstBucketAtLevel(0),
                              self.LastBucketAtLevel(l))
    def RandomBucketAtLevel(self, l):
        return random.randint(self.FirstBucketAtLevel(l),
                              self.FirstBucketAtLevel(l+1)-1)

    #
    # Nodes (a class that helps with heap path calculations)
    #

    def RootNode(self):
        return self.FirstNodeAtLevel(0)
    def NodeCountAtLevel(self, l):
        return self.BucketCountAtLevel(l)
    def FirstNodeAtLevel(self, l):
        return self.Node(self.FirstBucketAtLevel(l))
    def LastNodeAtLevel(self, l):
        return self.Node(self.LastBucketAtLevel(l))
    def RandomNodeUpToLevel(self, l):
        return self.Node(self.RandomBucketUpToLevel(l))
    def RandomNodeAtLevel(self, l):
        return self.Node(self.RandomBucketAtLevel(l))

    #
    # Slot (0-based integer)
    #

    def SlotCountAtLevel(self, l):
        return self.BucketCountAtLevel(l) * self.BucketSize()
    def FirstSlotAtLevel(self, l):
        return self.BucketToSlot(self.FirstBucketAtLevel(l))
    def LastSlotAtLevel(self, l):
        return self.BucketToSlot(self.FirstBucketAtLevel(l+1)) - 1
    def SlotToBucket(self, s):
        assert s >= 0
        return s//self.BucketSize()

class SizedVirtualHeap(VirtualHeap):

    def __init__(self, k, height, bucket_size=1):
        super(SizedVirtualHeap, self).\
            __init__(k, bucket_size=bucket_size)
        self._height = height

    #
    # Size properties
    #

    def Levels(self):
        return self._height + 1
    def Height(self):
        return self._height

    #
    # Buckets (0-based integer, equivalent to slot for heap with bucket_size=1)
    #

    def BucketCount(self):
        return CalculateBucketCountInHeapWithHeight(self.k, self.Height())
    def LeafBucketCount(self):
        return CalculateLeafBucketCountInHeapWithHeight(self.k, self.Height())
    def FirstLeafBucket(self):
        return self.FirstBucketAtLevel(self.Height())
    def LastLeafBucket(self):
        return self.LastBucketAtLevel(self.Height())
    def RandomBucket(self):
        return random.randint(self.FirstBucketAtLevel(0),
                              self.LastLeafBucket())
    def RandomLeafBucket(self):
        return self.RandomBucketAtLevel(self.Height())

    #
    # Nodes (a class that helps with heap path calculations)
    #

    def IsNilNode(self, n):
        return n.bucket >= self.BucketCount()
    def NodeCount(self):
        return self.BucketCount()
    def LeafNodeCount(self):
        return self.LeafBucketCount()
    def RandomLeafNode(self):
        return self.Node(self.RandomLeafBucket())
    def RandomNode(self):
        return self.Node(self.RandomBucket())

    #
    # Slot (0-based integer)
    #

    def SlotCount(self):
        return self.BucketCount() * self.BucketSize()
    def LeafSlotCount(self):
        return self.LeafBucketCount() * self.BucketSize()

    #
    # Visualization
    #

    def WriteAsDot(self, f, data=None, max_levels=None):
        "Write the tree in the dot language format to f."
        assert (max_levels is None) or (max_levels >= 0)
        def visit_node(n, levels):
            lbl = "{"
            if data is None:
                if self.k <= MaxKLabeled():
                    lbl = repr(n.Label()).\
                          replace("{","\{").\
                          replace("}","\}").\
                          replace("|","\|").\
                          replace("<","\<").\
                          replace(">","\>")
                else:
                    lbl = str(n)
            else:
                s = self.BucketToSlot(n.bucket)
                for i in range(self.BucketSize()):
                    if data is None:
                        lbl += "{%s}" % (s + i)
                    else:
                        lbl += "{%s}" % (data[s+i])
                    if i + 1 != self.BucketSize():
                        lbl += "|"
            lbl += "}"
            f.write("  %s [penwidth=%s,label=\"%s\"];\n"
                    % (n.bucket, 1, lbl))
            levels += 1
            if (max_levels is None) or (levels <= max_levels):
                for i in range(self.k):
                    cn = n.ChildNode(i)
                    if not self.IsNilNode(cn):
                        visit_node(cn, levels)
                        f.write("  %s -> %s ;\n" % (n.bucket, cn.bucket))

        f.write("// Created by SizedVirtualHeap.WriteAsDot(...)\n")
        f.write("digraph heaptree {\n")
        f.write("node [shape=record]\n")

        if (max_levels is None) or (max_levels > 0):
            visit_node(self.RootNode(), 1)
        f.write("}\n")

    def SaveImageAsPDF(self, filename, data=None, max_levels=None):
        "Write the heap as PDF file."
        assert (max_levels is None) or (max_levels >= 0)
        import os
        if not filename.endswith('.pdf'):
            filename = filename+'.pdf'
        with open('%s.dot' % filename, 'w') as f:
            self.WriteAsDot(f, data=data, max_levels=max_levels)
        if os.system('dot %s.dot -Tpdf -o %s' % (filename, filename)):
            print("DOT -> PDF conversion failed. See DOT file: %s"
                  % (filename+".dot"))
            return False
        else:
            os.remove('%s.dot' % (filename))
            return True
