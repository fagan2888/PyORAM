import os
import subprocess
import unittest

import pyoram
from pyoram.tree.virtualheap import \
    (VirtualHeap,
     SizedVirtualHeap,
     MaxKLabeled,
     CalculateBucketCountInHeapWithHeight,
     CalculateBucketCountInHeapAtLevel,
     CalculateBucketLevel,
     CalculateLastCommonLevel,
     BaseKStringToBase10Integer,
     numerals,
     _clib)

thisdir = os.path.dirname(os.path.abspath(__file__))
baselinedir = os.path.join(thisdir, "baselines")

try:
    has_dot = not subprocess.call(["dot", "-V"])
except:
    has_dot = False

try:
    xrange
except:
    xrange = range

_test_bases = list(xrange(2, 15)) + [MaxKLabeled()+1]
_test_labeled_bases = list(xrange(2, 15)) + [MaxKLabeled()]

def _do_preorder(x):
    if x.level > 2:
        return
    yield x.bucket
    for c in xrange(x.k):
        for b in _do_preorder(x.ChildNode(c)):
            yield b

def _do_postorder(x):
    if x.level > 2:
        return
    for c in xrange(x.k):
        for b in _do_postorder(x.ChildNode(c)):
            yield b
    yield x.bucket

def _do_inorder(x):
    assert x.k == 2
    if x.level > 2:
        return
    for b in _do_inorder(x.ChildNode(0)):
        yield b
    yield x.bucket
    for b in _do_inorder(x.ChildNode(1)):
        yield b

class TestVirtualHeapNode(unittest.TestCase):

    def test_init(self):
        for k in _test_bases:
            vh = VirtualHeap(k)
            node = vh.Node(0)
            self.assertEqual(node.k, k)
            self.assertEqual(node.bucket, 0)
            self.assertEqual(node.level, 0)
            for b in xrange(1, k+1):
                node = vh.Node(b)
                self.assertEqual(node.k, k)
                self.assertEqual(node.bucket, b)
                self.assertEqual(node.level, 1)

    def test_level(self):
        Node = VirtualHeap(2).Node
        self.assertEqual(Node(0).level, 0)
        self.assertEqual(Node(1).level, 1)
        self.assertEqual(Node(2).level, 1)
        self.assertEqual(Node(3).level, 2)
        self.assertEqual(Node(4).level, 2)
        self.assertEqual(Node(5).level, 2)
        self.assertEqual(Node(6).level, 2)
        self.assertEqual(Node(7).level, 3)

        Node = VirtualHeap(3).Node
        self.assertEqual(Node(0).level, 0)
        self.assertEqual(Node(1).level, 1)
        self.assertEqual(Node(2).level, 1)
        self.assertEqual(Node(3).level, 1)
        self.assertEqual(Node(4).level, 2)
        self.assertEqual(Node(5).level, 2)
        self.assertEqual(Node(6).level, 2)
        self.assertEqual(Node(7).level, 2)
        self.assertEqual(Node(8).level, 2)
        self.assertEqual(Node(9).level, 2)
        self.assertEqual(Node(10).level, 2)
        self.assertEqual(Node(11).level, 2)
        self.assertEqual(Node(12).level, 2)
        self.assertEqual(Node(13).level, 3)

    def test_hash(self):
        x1 = VirtualHeap(3).Node(5)
        x2 = VirtualHeap(2).Node(5)
        self.assertNotEqual(id(x1), id(x2))
        self.assertEqual(x1, x2)
        self.assertEqual(x1, x1)
        self.assertEqual(x2, x2)

        all_node_set = set()
        all_node_list = list()
        for k in _test_bases:
            node_set = set()
            node_list = list()
            Node = VirtualHeap(k).Node
            for height in xrange(k+2):
                node = Node(height)
                node_set.add(node)
                all_node_set.add(node)
                node_list.append(node)
                all_node_list.append(node)
            self.assertEqual(sorted(node_set),
                             sorted(node_list))
        self.assertNotEqual(sorted(all_node_set),
                            sorted(all_node_list))
    def test_lt(self):
        Node = VirtualHeap(3).Node
        self.assertEqual(Node(5) < 4, False)
        self.assertEqual(Node(5) < 5, False)
        self.assertEqual(Node(5) < 6, True)

    def test_le(self):
        Node = VirtualHeap(3).Node
        self.assertEqual(Node(5) <= 4, False)
        self.assertEqual(Node(5) <= 5, True)
        self.assertEqual(Node(5) <= 6, True)

    def test_eq(self):
        Node = VirtualHeap(3).Node
        self.assertEqual(Node(5) == 4, False)
        self.assertEqual(Node(5) == 5, True)
        self.assertEqual(Node(5) == 6, False)

    def test_ne(self):
        Node = VirtualHeap(3).Node
        self.assertEqual(Node(5) != 4, True)
        self.assertEqual(Node(5) != 5, False)
        self.assertEqual(Node(5) != 6, True)

    def test_gt(self):
        Node = VirtualHeap(3).Node
        self.assertEqual(Node(5) > 4, True)
        self.assertEqual(Node(5) > 5, False)
        self.assertEqual(Node(5) > 6, False)

    def test_ge(self):
        Node = VirtualHeap(3).Node
        self.assertEqual(Node(5) >= 4, True)
        self.assertEqual(Node(5) >= 5, True)
        self.assertEqual(Node(5) >= 6, False)

    def test_LastCommonLevel_k2(self):
        Node = VirtualHeap(2).Node
        n0 = Node(0)
        n1 = Node(1)
        n2 = Node(2)
        n3 = Node(3)
        n4 = Node(4)
        n5 = Node(5)
        n6 = Node(6)
        n7 = Node(7)
        self.assertEqual(n0.LastCommonLevel(n0), 0)
        self.assertEqual(n0.LastCommonLevel(n1), 0)
        self.assertEqual(n0.LastCommonLevel(n2), 0)
        self.assertEqual(n0.LastCommonLevel(n3), 0)
        self.assertEqual(n0.LastCommonLevel(n4), 0)
        self.assertEqual(n0.LastCommonLevel(n5), 0)
        self.assertEqual(n0.LastCommonLevel(n6), 0)
        self.assertEqual(n0.LastCommonLevel(n7), 0)

        self.assertEqual(n1.LastCommonLevel(n0), 0)
        self.assertEqual(n1.LastCommonLevel(n1), 1)
        self.assertEqual(n1.LastCommonLevel(n2), 0)
        self.assertEqual(n1.LastCommonLevel(n3), 1)
        self.assertEqual(n1.LastCommonLevel(n4), 1)
        self.assertEqual(n1.LastCommonLevel(n5), 0)
        self.assertEqual(n1.LastCommonLevel(n6), 0)
        self.assertEqual(n1.LastCommonLevel(n7), 1)

        self.assertEqual(n2.LastCommonLevel(n0), 0)
        self.assertEqual(n2.LastCommonLevel(n1), 0)
        self.assertEqual(n2.LastCommonLevel(n2), 1)
        self.assertEqual(n2.LastCommonLevel(n3), 0)
        self.assertEqual(n2.LastCommonLevel(n4), 0)
        self.assertEqual(n2.LastCommonLevel(n5), 1)
        self.assertEqual(n2.LastCommonLevel(n6), 1)
        self.assertEqual(n2.LastCommonLevel(n7), 0)

        self.assertEqual(n3.LastCommonLevel(n0), 0)
        self.assertEqual(n3.LastCommonLevel(n1), 1)
        self.assertEqual(n3.LastCommonLevel(n2), 0)
        self.assertEqual(n3.LastCommonLevel(n3), 2)
        self.assertEqual(n3.LastCommonLevel(n4), 1)
        self.assertEqual(n3.LastCommonLevel(n5), 0)
        self.assertEqual(n3.LastCommonLevel(n6), 0)
        self.assertEqual(n3.LastCommonLevel(n7), 2)

        self.assertEqual(n4.LastCommonLevel(n0), 0)
        self.assertEqual(n4.LastCommonLevel(n1), 1)
        self.assertEqual(n4.LastCommonLevel(n2), 0)
        self.assertEqual(n4.LastCommonLevel(n3), 1)
        self.assertEqual(n4.LastCommonLevel(n4), 2)
        self.assertEqual(n4.LastCommonLevel(n5), 0)
        self.assertEqual(n4.LastCommonLevel(n6), 0)
        self.assertEqual(n4.LastCommonLevel(n7), 1)

        self.assertEqual(n5.LastCommonLevel(n0), 0)
        self.assertEqual(n5.LastCommonLevel(n1), 0)
        self.assertEqual(n5.LastCommonLevel(n2), 1)
        self.assertEqual(n5.LastCommonLevel(n3), 0)
        self.assertEqual(n5.LastCommonLevel(n4), 0)
        self.assertEqual(n5.LastCommonLevel(n5), 2)
        self.assertEqual(n5.LastCommonLevel(n6), 1)
        self.assertEqual(n5.LastCommonLevel(n7), 0)

        self.assertEqual(n6.LastCommonLevel(n0), 0)
        self.assertEqual(n6.LastCommonLevel(n1), 0)
        self.assertEqual(n6.LastCommonLevel(n2), 1)
        self.assertEqual(n6.LastCommonLevel(n3), 0)
        self.assertEqual(n6.LastCommonLevel(n4), 0)
        self.assertEqual(n6.LastCommonLevel(n5), 1)
        self.assertEqual(n6.LastCommonLevel(n6), 2)
        self.assertEqual(n6.LastCommonLevel(n7), 0)

        self.assertEqual(n7.LastCommonLevel(n0), 0)
        self.assertEqual(n7.LastCommonLevel(n1), 1)
        self.assertEqual(n7.LastCommonLevel(n2), 0)
        self.assertEqual(n7.LastCommonLevel(n3), 2)
        self.assertEqual(n7.LastCommonLevel(n4), 1)
        self.assertEqual(n7.LastCommonLevel(n5), 0)
        self.assertEqual(n7.LastCommonLevel(n6), 0)
        self.assertEqual(n7.LastCommonLevel(n7), 3)

    def test_LastCommonLevel_k3(self):
        Node = VirtualHeap(3).Node
        n0 = Node(0)
        n1 = Node(1)
        n2 = Node(2)
        n3 = Node(3)
        n4 = Node(4)
        n5 = Node(5)
        n6 = Node(6)
        n7 = Node(7)
        self.assertEqual(n0.LastCommonLevel(n0), 0)
        self.assertEqual(n0.LastCommonLevel(n1), 0)
        self.assertEqual(n0.LastCommonLevel(n2), 0)
        self.assertEqual(n0.LastCommonLevel(n3), 0)
        self.assertEqual(n0.LastCommonLevel(n4), 0)
        self.assertEqual(n0.LastCommonLevel(n5), 0)
        self.assertEqual(n0.LastCommonLevel(n6), 0)
        self.assertEqual(n0.LastCommonLevel(n7), 0)

        self.assertEqual(n1.LastCommonLevel(n0), 0)
        self.assertEqual(n1.LastCommonLevel(n1), 1)
        self.assertEqual(n1.LastCommonLevel(n2), 0)
        self.assertEqual(n1.LastCommonLevel(n3), 0)
        self.assertEqual(n1.LastCommonLevel(n4), 1)
        self.assertEqual(n1.LastCommonLevel(n5), 1)
        self.assertEqual(n1.LastCommonLevel(n6), 1)
        self.assertEqual(n1.LastCommonLevel(n7), 0)

        self.assertEqual(n2.LastCommonLevel(n0), 0)
        self.assertEqual(n2.LastCommonLevel(n1), 0)
        self.assertEqual(n2.LastCommonLevel(n2), 1)
        self.assertEqual(n2.LastCommonLevel(n3), 0)
        self.assertEqual(n2.LastCommonLevel(n4), 0)
        self.assertEqual(n2.LastCommonLevel(n5), 0)
        self.assertEqual(n2.LastCommonLevel(n6), 0)
        self.assertEqual(n2.LastCommonLevel(n7), 1)

        self.assertEqual(n3.LastCommonLevel(n0), 0)
        self.assertEqual(n3.LastCommonLevel(n1), 0)
        self.assertEqual(n3.LastCommonLevel(n2), 0)
        self.assertEqual(n3.LastCommonLevel(n3), 1)
        self.assertEqual(n3.LastCommonLevel(n4), 0)
        self.assertEqual(n3.LastCommonLevel(n5), 0)
        self.assertEqual(n3.LastCommonLevel(n6), 0)
        self.assertEqual(n3.LastCommonLevel(n7), 0)

        self.assertEqual(n4.LastCommonLevel(n0), 0)
        self.assertEqual(n4.LastCommonLevel(n1), 1)
        self.assertEqual(n4.LastCommonLevel(n2), 0)
        self.assertEqual(n4.LastCommonLevel(n3), 0)
        self.assertEqual(n4.LastCommonLevel(n4), 2)
        self.assertEqual(n4.LastCommonLevel(n5), 1)
        self.assertEqual(n4.LastCommonLevel(n6), 1)
        self.assertEqual(n4.LastCommonLevel(n7), 0)

        self.assertEqual(n5.LastCommonLevel(n0), 0)
        self.assertEqual(n5.LastCommonLevel(n1), 1)
        self.assertEqual(n5.LastCommonLevel(n2), 0)
        self.assertEqual(n5.LastCommonLevel(n3), 0)
        self.assertEqual(n5.LastCommonLevel(n4), 1)
        self.assertEqual(n5.LastCommonLevel(n5), 2)
        self.assertEqual(n5.LastCommonLevel(n6), 1)
        self.assertEqual(n5.LastCommonLevel(n7), 0)

        self.assertEqual(n6.LastCommonLevel(n0), 0)
        self.assertEqual(n6.LastCommonLevel(n1), 1)
        self.assertEqual(n6.LastCommonLevel(n2), 0)
        self.assertEqual(n6.LastCommonLevel(n3), 0)
        self.assertEqual(n6.LastCommonLevel(n4), 1)
        self.assertEqual(n6.LastCommonLevel(n5), 1)
        self.assertEqual(n6.LastCommonLevel(n6), 2)
        self.assertEqual(n6.LastCommonLevel(n7), 0)

        self.assertEqual(n7.LastCommonLevel(n0), 0)
        self.assertEqual(n7.LastCommonLevel(n1), 0)
        self.assertEqual(n7.LastCommonLevel(n2), 1)
        self.assertEqual(n7.LastCommonLevel(n3), 0)
        self.assertEqual(n7.LastCommonLevel(n4), 0)
        self.assertEqual(n7.LastCommonLevel(n5), 0)
        self.assertEqual(n7.LastCommonLevel(n6), 0)
        self.assertEqual(n7.LastCommonLevel(n7), 2)

    def test_ChildNode(self):
        root = VirtualHeap(2).Node(0)
        self.assertEqual(list(_do_preorder(root)),
                         [0, 1, 3, 4, 2, 5, 6])
        self.assertEqual(list(_do_postorder(root)),
                         [3, 4, 1, 5, 6, 2, 0])
        self.assertEqual(list(_do_inorder(root)),
                         [3, 1, 4, 0, 5, 2, 6])

        root = VirtualHeap(3).Node(0)
        self.assertEqual(
            list(_do_preorder(root)),
            [0, 1, 4, 5, 6, 2, 7, 8, 9, 3, 10, 11, 12])
        self.assertEqual(
            list(_do_postorder(root)),
            [4, 5, 6, 1, 7, 8, 9, 2, 10, 11, 12, 3, 0])

    def test_ParentNode(self):
        Node = VirtualHeap(2).Node
        self.assertEqual(Node(0).ParentNode(),
                         None)
        self.assertEqual(Node(1).ParentNode(),
                         Node(0))
        self.assertEqual(Node(2).ParentNode(),
                         Node(0))
        self.assertEqual(Node(3).ParentNode(),
                         Node(1))
        self.assertEqual(Node(4).ParentNode(),
                         Node(1))
        self.assertEqual(Node(5).ParentNode(),
                         Node(2))
        self.assertEqual(Node(6).ParentNode(),
                         Node(2))
        self.assertEqual(Node(7).ParentNode(),
                         Node(3))

        Node = VirtualHeap(3).Node
        self.assertEqual(Node(0).ParentNode(),
                         None)
        self.assertEqual(Node(1).ParentNode(),
                         Node(0))
        self.assertEqual(Node(2).ParentNode(),
                         Node(0))
        self.assertEqual(Node(3).ParentNode(),
                         Node(0))
        self.assertEqual(Node(4).ParentNode(),
                         Node(1))
        self.assertEqual(Node(5).ParentNode(),
                         Node(1))
        self.assertEqual(Node(6).ParentNode(),
                         Node(1))
        self.assertEqual(Node(7).ParentNode(),
                         Node(2))
        self.assertEqual(Node(8).ParentNode(),
                         Node(2))
        self.assertEqual(Node(9).ParentNode(),
                         Node(2))
        self.assertEqual(Node(10).ParentNode(),
                         Node(3))
        self.assertEqual(Node(11).ParentNode(),
                         Node(3))
        self.assertEqual(Node(12).ParentNode(),
                         Node(3))
        self.assertEqual(Node(13).ParentNode(),
                         Node(4))

    def test_AncestorNodeAtLevel(self):
        Node = VirtualHeap(2).Node
        self.assertEqual(Node(0).AncestorNodeAtLevel(0),
                         Node(0))
        self.assertEqual(Node(0).AncestorNodeAtLevel(1),
                         None)
        self.assertEqual(Node(1).AncestorNodeAtLevel(0),
                         Node(0))
        self.assertEqual(Node(1).AncestorNodeAtLevel(1),
                         Node(1))
        self.assertEqual(Node(1).AncestorNodeAtLevel(2),
                         None)
        self.assertEqual(Node(3).AncestorNodeAtLevel(0),
                         Node(0))
        self.assertEqual(Node(3).AncestorNodeAtLevel(1),
                         Node(1))
        self.assertEqual(Node(3).AncestorNodeAtLevel(2),
                         Node(3))
        self.assertEqual(Node(3).AncestorNodeAtLevel(3),
                         None)

        Node = VirtualHeap(3).Node
        self.assertEqual(Node(0).AncestorNodeAtLevel(0),
                         Node(0))
        self.assertEqual(Node(0).AncestorNodeAtLevel(1),
                         None)
        self.assertEqual(Node(1).AncestorNodeAtLevel(0),
                         Node(0))
        self.assertEqual(Node(1).AncestorNodeAtLevel(1),
                         Node(1))
        self.assertEqual(Node(1).AncestorNodeAtLevel(2),
                         None)
        self.assertEqual(Node(4).AncestorNodeAtLevel(0),
                         Node(0))
        self.assertEqual(Node(4).AncestorNodeAtLevel(1),
                         Node(1))
        self.assertEqual(Node(4).AncestorNodeAtLevel(2),
                         Node(4))
        self.assertEqual(Node(4).AncestorNodeAtLevel(3),
                         None)

    def test_GenerateBucketPathToRoot(self):
        Node = VirtualHeap(2).Node
        self.assertEqual(list(Node(0).GenerateBucketPathToRoot()),
                         list(reversed([0])))
        self.assertEqual(list(Node(7).GenerateBucketPathToRoot()),
                         list(reversed([0, 1, 3, 7])))
        self.assertEqual(list(Node(8).GenerateBucketPathToRoot()),
                         list(reversed([0, 1, 3, 8])))
        self.assertEqual(list(Node(9).GenerateBucketPathToRoot()),
                         list(reversed([0, 1, 4, 9])))
        self.assertEqual(list(Node(10).GenerateBucketPathToRoot()),
                         list(reversed([0, 1, 4, 10])))
        self.assertEqual(list(Node(11).GenerateBucketPathToRoot()),
                         list(reversed([0, 2, 5, 11])))
        self.assertEqual(list(Node(12).GenerateBucketPathToRoot()),
                         list(reversed([0, 2, 5, 12])))
        self.assertEqual(list(Node(13).GenerateBucketPathToRoot()),
                         list(reversed([0, 2, 6, 13])))
        self.assertEqual(list(Node(14).GenerateBucketPathToRoot()),
                         list(reversed([0, 2, 6, 14])))

    def test_BucketPath(self):
        Node = VirtualHeap(2).Node
        self.assertEqual(Node(0).BucketPath(),
                         [0])
        self.assertEqual(Node(7).BucketPath(),
                         [0, 1, 3, 7])
        self.assertEqual(Node(8).BucketPath(),
                         [0, 1, 3, 8])
        self.assertEqual(Node(9).BucketPath(),
                         [0, 1, 4, 9])
        self.assertEqual(Node(10).BucketPath(),
                         [0, 1, 4, 10])
        self.assertEqual(Node(11).BucketPath(),
                         [0, 2, 5, 11])
        self.assertEqual(Node(12).BucketPath(),
                         [0, 2, 5, 12])
        self.assertEqual(Node(13).BucketPath(),
                         [0, 2, 6, 13])
        self.assertEqual(Node(14).BucketPath(),
                         [0, 2, 6, 14])

    def test_repr(self):
        Node = VirtualHeap(2).Node
        self.assertEqual(
            repr(Node(0)),
            "VirtualHeapNode(k=2, bucket=0, level=0, label='')")
        self.assertEqual(
            repr(Node(7)),
            "VirtualHeapNode(k=2, bucket=7, level=3, label='000')")

        Node = VirtualHeap(3).Node
        self.assertEqual(
            repr(Node(0)),
            "VirtualHeapNode(k=3, bucket=0, level=0, label='')")
        self.assertEqual(
            repr(Node(7)),
            "VirtualHeapNode(k=3, bucket=7, level=2, label='10')")

        Node = VirtualHeap(5).Node
        self.assertEqual(
            repr(Node(25)),
            "VirtualHeapNode(k=5, bucket=25, level=2, label='34')")

        Node = VirtualHeap(MaxKLabeled()).Node
        self.assertEqual(
            repr(Node(0)),
            ("VirtualHeapNode(k=%d, bucket=0, level=0, label='')"
             % (MaxKLabeled())))
        self.assertEqual(MaxKLabeled() >= 2, True)
        self.assertEqual(
            repr(Node(1)),
            ("VirtualHeapNode(k=%d, bucket=1, level=1, label='0')"
             % (MaxKLabeled())))

        Node = VirtualHeap(MaxKLabeled()+1).Node
        self.assertEqual(
            repr(Node(0)),
            ("VirtualHeapNode(k=%d, bucket=0, level=0, label='')"
             % (MaxKLabeled()+1)))
        self.assertEqual(
            repr(Node(1)),
            ("VirtualHeapNode(k=%d, bucket=1, level=1, label='<unknown>')"
             % (MaxKLabeled()+1)))
        self.assertEqual(
            repr(Node(MaxKLabeled()+1)),
            ("VirtualHeapNode(k=%d, bucket=%d, level=1, label='<unknown>')"
             % (MaxKLabeled()+1,
                MaxKLabeled()+1)))
        self.assertEqual(
            repr(Node(MaxKLabeled()+2)),
            ("VirtualHeapNode(k=%d, bucket=%d, level=2, label='<unknown>')"
             % (MaxKLabeled()+1,
                MaxKLabeled()+2)))

    def test_str(self):
        Node = VirtualHeap(2).Node
        self.assertEqual(
            str(Node(0)),
            "(0, 0)")
        self.assertEqual(
            str(Node(7)),
            "(3, 0)")

        Node = VirtualHeap(3).Node
        self.assertEqual(
            str(Node(0)),
            "(0, 0)")
        self.assertEqual(
            str(Node(7)),
            "(2, 3)")

        Node = VirtualHeap(5).Node
        self.assertEqual(
            str(Node(25)),
            "(2, 19)")

    def test_Label(self):

        Node = VirtualHeap(2).Node
        self.assertEqual(Node(0).Label(), "")
        self.assertEqual(Node(1).Label(), "0")
        self.assertEqual(Node(2).Label(), "1")
        self.assertEqual(Node(3).Label(), "00")
        self.assertEqual(Node(4).Label(), "01")
        self.assertEqual(Node(5).Label(), "10")
        self.assertEqual(Node(6).Label(), "11")
        self.assertEqual(Node(7).Label(), "000")
        self.assertEqual(Node(8).Label(), "001")
        self.assertEqual(Node(9).Label(), "010")
        self.assertEqual(Node(10).Label(), "011")
        self.assertEqual(Node(11).Label(), "100")
        self.assertEqual(Node(12).Label(), "101")
        self.assertEqual(Node(13).Label(), "110")
        self.assertEqual(Node(14).Label(), "111")
        self.assertEqual(Node(15).Label(), "0000")
        self.assertEqual(Node(30).Label(), "1111")

        for k in _test_labeled_bases:
            Node = VirtualHeap(k).Node
            for b in xrange(CalculateBucketCountInHeapWithHeight(k, 2)+1):
                label = Node(b).Label()
                level = Node(b).level
                if label == "":
                    self.assertEqual(b, 0)
                else:
                    self.assertEqual(
                        b,
                        BaseKStringToBase10Integer(k, label) + \
                        CalculateBucketCountInHeapWithHeight(k, level-1))

    def test_IsNodeOnPath(self):
        Node = VirtualHeap(2).Node
        self.assertEqual(
            Node(0).IsNodeOnPath(
                Node(0)),
            True)
        self.assertEqual(
            Node(0).IsNodeOnPath(
                Node(1)),
            False)
        self.assertEqual(
            Node(0).IsNodeOnPath(
                Node(2)),
            False)
        self.assertEqual(
            Node(0).IsNodeOnPath(
                Node(3)),
            False)

        Node = VirtualHeap(5).Node
        self.assertEqual(
            Node(20).IsNodeOnPath(
                Node(21)),
            False)
        self.assertEqual(
            Node(21).IsNodeOnPath(
                Node(4)),
            True)

        Node = VirtualHeap(3).Node
        self.assertEqual(
            Node(7).IsNodeOnPath(
                Node(0)),
            True)
        self.assertEqual(
            Node(7).IsNodeOnPath(
                Node(2)),
            True)
        self.assertEqual(
            Node(7).IsNodeOnPath(
                Node(7)),
            True)
        self.assertEqual(
            Node(7).IsNodeOnPath(
                Node(8)),
            False)

class TestVirtualHeap(unittest.TestCase):

    def test_init(self):
        vh = VirtualHeap(2, bucket_size=4)
        self.assertEqual(vh.k, 2)
        self.assertEqual(vh.Node.k, 2)
        self.assertEqual(vh.BucketSize(), 4)
        vh = VirtualHeap(5, bucket_size=7)
        self.assertEqual(vh.k, 5)
        self.assertEqual(vh.Node.k, 5)
        self.assertEqual(vh.BucketSize(), 7)

    def test_NodeLabelToBucket(self):
        vh = VirtualHeap(2)
        self.assertEqual(vh.NodeLabelToBucket(""), 0)
        self.assertEqual(vh.NodeLabelToBucket("0"), 1)
        self.assertEqual(vh.NodeLabelToBucket("1"), 2)
        self.assertEqual(vh.NodeLabelToBucket("00"), 3)
        self.assertEqual(vh.NodeLabelToBucket("01"), 4)
        self.assertEqual(vh.NodeLabelToBucket("10"), 5)
        self.assertEqual(vh.NodeLabelToBucket("11"), 6)
        self.assertEqual(vh.NodeLabelToBucket("000"), 7)
        self.assertEqual(vh.NodeLabelToBucket("001"), 8)
        self.assertEqual(vh.NodeLabelToBucket("010"), 9)
        self.assertEqual(vh.NodeLabelToBucket("011"), 10)
        self.assertEqual(vh.NodeLabelToBucket("100"), 11)
        self.assertEqual(vh.NodeLabelToBucket("101"), 12)
        self.assertEqual(vh.NodeLabelToBucket("110"), 13)
        self.assertEqual(vh.NodeLabelToBucket("111"), 14)
        self.assertEqual(vh.NodeLabelToBucket("0000"), 15)
        self.assertEqual(vh.NodeLabelToBucket("1111"),
                         CalculateBucketCountInHeapWithHeight(2, 4)-1)

        vh = VirtualHeap(3)
        self.assertEqual(vh.NodeLabelToBucket(""), 0)
        self.assertEqual(vh.NodeLabelToBucket("0"), 1)
        self.assertEqual(vh.NodeLabelToBucket("1"), 2)
        self.assertEqual(vh.NodeLabelToBucket("2"), 3)
        self.assertEqual(vh.NodeLabelToBucket("00"), 4)
        self.assertEqual(vh.NodeLabelToBucket("01"), 5)
        self.assertEqual(vh.NodeLabelToBucket("02"), 6)
        self.assertEqual(vh.NodeLabelToBucket("10"), 7)
        self.assertEqual(vh.NodeLabelToBucket("11"), 8)
        self.assertEqual(vh.NodeLabelToBucket("12"), 9)
        self.assertEqual(vh.NodeLabelToBucket("20"), 10)
        self.assertEqual(vh.NodeLabelToBucket("21"), 11)
        self.assertEqual(vh.NodeLabelToBucket("22"), 12)
        self.assertEqual(vh.NodeLabelToBucket("000"), 13)
        self.assertEqual(vh.NodeLabelToBucket("222"),
                         CalculateBucketCountInHeapWithHeight(3, 3)-1)

        for k in xrange(2, MaxKLabeled()+1):
            for h in xrange(5):
                vh = VirtualHeap(k)
                largest_symbol = numerals[k-1]
                self.assertEqual(vh.k, k)
                self.assertEqual(vh.NodeLabelToBucket(""), 0)
                self.assertEqual(vh.NodeLabelToBucket(largest_symbol * h),
                                 CalculateBucketCountInHeapWithHeight(k, h)-1)

    def test_ObjectCountAtLevel(self):
        for k in _test_bases:
            for height in xrange(k+2):
                for bucket_size in xrange(1, 5):
                    vh = VirtualHeap(k, bucket_size=bucket_size)
                    for l in xrange(height+1):
                        cnt = k**l
                        self.assertEqual(vh.BucketCountAtLevel(l), cnt)
                        self.assertEqual(vh.NodeCountAtLevel(l), cnt)
                        self.assertEqual(vh.SlotCountAtLevel(l),
                                         cnt * bucket_size)

    def test_BucketToSlot(self):
        for k in xrange(2, 6):
            for bucket_size in xrange(1, 5):
                heap = VirtualHeap(k, bucket_size=bucket_size)
                for b in xrange(20):
                    self.assertEqual(heap.BucketToSlot(b),
                                     bucket_size * b)

    def test_NodeCountAtLevel(self):
        self.assertEqual(VirtualHeap(2).NodeCountAtLevel(0), 1)
        self.assertEqual(VirtualHeap(2).NodeCountAtLevel(1), 2)
        self.assertEqual(VirtualHeap(2).NodeCountAtLevel(2), 4)
        self.assertEqual(VirtualHeap(2).NodeCountAtLevel(3), 8)
        self.assertEqual(VirtualHeap(2).NodeCountAtLevel(4), 16)

        self.assertEqual(VirtualHeap(3).NodeCountAtLevel(0), 1)
        self.assertEqual(VirtualHeap(3).NodeCountAtLevel(1), 3)
        self.assertEqual(VirtualHeap(3).NodeCountAtLevel(2), 9)
        self.assertEqual(VirtualHeap(3).NodeCountAtLevel(3), 27)
        self.assertEqual(VirtualHeap(3).NodeCountAtLevel(4), 81)

        self.assertEqual(VirtualHeap(4).NodeCountAtLevel(0), 1)
        self.assertEqual(VirtualHeap(4).NodeCountAtLevel(1), 4)
        self.assertEqual(VirtualHeap(4).NodeCountAtLevel(2), 16)
        self.assertEqual(VirtualHeap(4).NodeCountAtLevel(3), 64)
        self.assertEqual(VirtualHeap(4).NodeCountAtLevel(4), 256)

    def test_FirstNodeAtLevel(self):
        self.assertEqual(VirtualHeap(2).FirstNodeAtLevel(0), 0)
        self.assertEqual(VirtualHeap(2).FirstNodeAtLevel(1), 1)
        self.assertEqual(VirtualHeap(2).FirstNodeAtLevel(2), 3)
        self.assertEqual(VirtualHeap(2).FirstNodeAtLevel(3), 7)
        self.assertEqual(VirtualHeap(2).FirstNodeAtLevel(4), 15)

        self.assertEqual(VirtualHeap(3).FirstNodeAtLevel(0), 0)
        self.assertEqual(VirtualHeap(3).FirstNodeAtLevel(1), 1)
        self.assertEqual(VirtualHeap(3).FirstNodeAtLevel(2), 4)
        self.assertEqual(VirtualHeap(3).FirstNodeAtLevel(3), 13)
        self.assertEqual(VirtualHeap(3).FirstNodeAtLevel(4), 40)

        self.assertEqual(VirtualHeap(4).FirstNodeAtLevel(0), 0)
        self.assertEqual(VirtualHeap(4).FirstNodeAtLevel(1), 1)
        self.assertEqual(VirtualHeap(4).FirstNodeAtLevel(2), 5)
        self.assertEqual(VirtualHeap(4).FirstNodeAtLevel(3), 21)
        self.assertEqual(VirtualHeap(4).FirstNodeAtLevel(4), 85)

    def test_LastNodeAtLevel(self):
        self.assertEqual(VirtualHeap(2).LastNodeAtLevel(0), 0)
        self.assertEqual(VirtualHeap(2).LastNodeAtLevel(1), 2)
        self.assertEqual(VirtualHeap(2).LastNodeAtLevel(2), 6)
        self.assertEqual(VirtualHeap(2).LastNodeAtLevel(3), 14)
        self.assertEqual(VirtualHeap(2).LastNodeAtLevel(4), 30)

        self.assertEqual(VirtualHeap(3).LastNodeAtLevel(0), 0)
        self.assertEqual(VirtualHeap(3).LastNodeAtLevel(1), 3)
        self.assertEqual(VirtualHeap(3).LastNodeAtLevel(2), 12)
        self.assertEqual(VirtualHeap(3).LastNodeAtLevel(3), 39)
        self.assertEqual(VirtualHeap(3).LastNodeAtLevel(4), 120)

        self.assertEqual(VirtualHeap(4).LastNodeAtLevel(0), 0)
        self.assertEqual(VirtualHeap(4).LastNodeAtLevel(1), 4)
        self.assertEqual(VirtualHeap(4).LastNodeAtLevel(2), 20)
        self.assertEqual(VirtualHeap(4).LastNodeAtLevel(3), 84)
        self.assertEqual(VirtualHeap(4).LastNodeAtLevel(4), 340)

    def test_RandomNodeUpToLevel(self):
        for k in xrange(2,6):
            heap = VirtualHeap(k)
            for l in xrange(4):
                for t in xrange(2 * CalculateBucketCountInHeapWithHeight(k, l)):
                    node = heap.RandomNodeUpToLevel(l)
                    self.assertEqual(node.level <= l, True)

    def test_RandomNodeAtLevel(self):
        for k in xrange(2,6):
            heap = VirtualHeap(k)
            for l in xrange(4):
                for t in xrange(2 * CalculateBucketCountInHeapAtLevel(k, l)):
                    node = heap.RandomNodeAtLevel(l)
                    self.assertEqual(node.level == l, True)

    def test_FirstSlotAtLevel(self):
        for bucket_size in xrange(1, 5):
            self.assertEqual(VirtualHeap(2, bucket_size=bucket_size).\
                             FirstSlotAtLevel(0), 0 * bucket_size)
            self.assertEqual(VirtualHeap(2, bucket_size=bucket_size).\
                             FirstSlotAtLevel(1), 1 * bucket_size)
            self.assertEqual(VirtualHeap(2, bucket_size=bucket_size).\
                             FirstSlotAtLevel(2), 3 * bucket_size)
            self.assertEqual(VirtualHeap(2, bucket_size=bucket_size).\
                             FirstSlotAtLevel(3), 7 * bucket_size)
            self.assertEqual(VirtualHeap(2, bucket_size=bucket_size).\
                             FirstSlotAtLevel(4), 15 * bucket_size)

            self.assertEqual(VirtualHeap(3, bucket_size=bucket_size).\
                             FirstSlotAtLevel(0), 0 * bucket_size)
            self.assertEqual(VirtualHeap(3, bucket_size=bucket_size).\
                             FirstSlotAtLevel(1), 1 * bucket_size)
            self.assertEqual(VirtualHeap(3, bucket_size=bucket_size).\
                             FirstSlotAtLevel(2), 4 * bucket_size)
            self.assertEqual(VirtualHeap(3, bucket_size=bucket_size).\
                             FirstSlotAtLevel(3), 13 * bucket_size)
            self.assertEqual(VirtualHeap(3, bucket_size=bucket_size).\
                             FirstSlotAtLevel(4), 40 * bucket_size)

            self.assertEqual(VirtualHeap(4, bucket_size=bucket_size).\
                             FirstSlotAtLevel(0), 0 * bucket_size)
            self.assertEqual(VirtualHeap(4, bucket_size=bucket_size).\
                             FirstSlotAtLevel(1), 1 * bucket_size)
            self.assertEqual(VirtualHeap(4, bucket_size=bucket_size).\
                             FirstSlotAtLevel(2), 5 * bucket_size)
            self.assertEqual(VirtualHeap(4, bucket_size=bucket_size).\
                             FirstSlotAtLevel(3), 21 * bucket_size)
            self.assertEqual(VirtualHeap(4, bucket_size=bucket_size).\
                             FirstSlotAtLevel(4), 85 * bucket_size)

    def test_LastSlotAtLevel(self):
        for bucket_size in xrange(1, 5):
            self.assertEqual(VirtualHeap(2, bucket_size=bucket_size).\
                             LastSlotAtLevel(0), 0 * bucket_size + (bucket_size-1))
            self.assertEqual(VirtualHeap(2, bucket_size=bucket_size).\
                             LastSlotAtLevel(1), 2 * bucket_size + (bucket_size-1))
            self.assertEqual(VirtualHeap(2, bucket_size=bucket_size).\
                             LastSlotAtLevel(2), 6 * bucket_size + (bucket_size-1))
            self.assertEqual(VirtualHeap(2, bucket_size=bucket_size).\
                             LastSlotAtLevel(3), 14 * bucket_size + (bucket_size-1))
            self.assertEqual(VirtualHeap(2, bucket_size=bucket_size).\
                             LastSlotAtLevel(4), 30 * bucket_size + (bucket_size-1))

            self.assertEqual(VirtualHeap(3, bucket_size=bucket_size).\
                             LastSlotAtLevel(0), 0 * bucket_size + (bucket_size-1))
            self.assertEqual(VirtualHeap(3, bucket_size=bucket_size).\
                             LastSlotAtLevel(1), 3 * bucket_size + (bucket_size-1))
            self.assertEqual(VirtualHeap(3, bucket_size=bucket_size).\
                             LastSlotAtLevel(2), 12 * bucket_size + (bucket_size-1))
            self.assertEqual(VirtualHeap(3, bucket_size=bucket_size).\
                             LastSlotAtLevel(3), 39 * bucket_size + (bucket_size-1))
            self.assertEqual(VirtualHeap(3, bucket_size=bucket_size).\
                             LastSlotAtLevel(4), 120 * bucket_size + (bucket_size-1))

            self.assertEqual(VirtualHeap(4, bucket_size=bucket_size).\
                             LastSlotAtLevel(0), 0 * bucket_size + (bucket_size-1))
            self.assertEqual(VirtualHeap(4, bucket_size=bucket_size).\
                             LastSlotAtLevel(1), 4 * bucket_size + (bucket_size-1))
            self.assertEqual(VirtualHeap(4, bucket_size=bucket_size).\
                             LastSlotAtLevel(2), 20 * bucket_size + (bucket_size-1))
            self.assertEqual(VirtualHeap(4, bucket_size=bucket_size).\
                             LastSlotAtLevel(3), 84 * bucket_size + (bucket_size-1))
            self.assertEqual(VirtualHeap(4, bucket_size=bucket_size).\
                             LastSlotAtLevel(4), 340 * bucket_size + (bucket_size-1))

    def test_SlotToBucket(self):
        self.assertEqual(VirtualHeap(2, bucket_size=1).SlotToBucket(0), 0)
        self.assertEqual(VirtualHeap(2, bucket_size=1).SlotToBucket(1), 1)
        self.assertEqual(VirtualHeap(2, bucket_size=1).SlotToBucket(2), 2)
        self.assertEqual(VirtualHeap(2, bucket_size=1).SlotToBucket(3), 3)
        self.assertEqual(VirtualHeap(2, bucket_size=1).SlotToBucket(4), 4)

        self.assertEqual(VirtualHeap(2, bucket_size=2).SlotToBucket(0), 0)
        self.assertEqual(VirtualHeap(2, bucket_size=2).SlotToBucket(1), 0)
        self.assertEqual(VirtualHeap(2, bucket_size=2).SlotToBucket(2), 1)
        self.assertEqual(VirtualHeap(2, bucket_size=2).SlotToBucket(3), 1)
        self.assertEqual(VirtualHeap(2, bucket_size=2).SlotToBucket(4), 2)

        self.assertEqual(VirtualHeap(2, bucket_size=3).SlotToBucket(0), 0)
        self.assertEqual(VirtualHeap(2, bucket_size=3).SlotToBucket(1), 0)
        self.assertEqual(VirtualHeap(2, bucket_size=3).SlotToBucket(2), 0)
        self.assertEqual(VirtualHeap(2, bucket_size=3).SlotToBucket(3), 1)
        self.assertEqual(VirtualHeap(2, bucket_size=3).SlotToBucket(4), 1)

        self.assertEqual(VirtualHeap(2, bucket_size=4).SlotToBucket(0), 0)
        self.assertEqual(VirtualHeap(2, bucket_size=4).SlotToBucket(1), 0)
        self.assertEqual(VirtualHeap(2, bucket_size=4).SlotToBucket(2), 0)
        self.assertEqual(VirtualHeap(2, bucket_size=4).SlotToBucket(3), 0)
        self.assertEqual(VirtualHeap(2, bucket_size=4).SlotToBucket(4), 1)

    def test_RootNode(self):
        for k in range(2, 6):
            for bucket_size in range(1, 5):
                heap = VirtualHeap(k, bucket_size=bucket_size)
                root = heap.RootNode()
                self.assertEqual(root, 0)
                self.assertEqual(root.bucket, 0)
                self.assertEqual(root.level, 0)
                self.assertEqual(root.ParentNode(), None)

class TestSizedVirtualHeap(unittest.TestCase):

    def test_init(self):
        vh = SizedVirtualHeap(2, 8, bucket_size=4)
        self.assertEqual(vh.k, 2)
        self.assertEqual(vh.Node.k, 2)
        self.assertEqual(vh.BucketSize(), 4)
        vh = SizedVirtualHeap(5, 9, bucket_size=7)
        self.assertEqual(vh.k, 5)
        self.assertEqual(vh.Node.k, 5)
        self.assertEqual(vh.BucketSize(), 7)

    def test_Levels(self):
        vh = SizedVirtualHeap(2, 3, bucket_size=4)
        self.assertEqual(vh.Levels(), 4)
        vh = SizedVirtualHeap(5, 6, bucket_size=7)
        self.assertEqual(vh.Levels(), 7)

    def test_Height(self):
        vh = SizedVirtualHeap(2, 3, bucket_size=4)
        self.assertEqual(vh.Height(), 3)
        vh = SizedVirtualHeap(5, 6, bucket_size=7)
        self.assertEqual(vh.Height(), 6)

    def test_ObjectCount(self):
        for k in _test_bases:
            for height in xrange(k+2):
                for bucket_size in xrange(1, 5):
                    vh = SizedVirtualHeap(k, height, bucket_size=bucket_size)
                    cnt = (((k**(height+1))-1)//(k-1))
                    self.assertEqual(vh.BucketCount(), cnt)
                    self.assertEqual(vh.NodeCount(), cnt)
                    self.assertEqual(vh.SlotCount(), cnt * bucket_size)

    def test_LeafObjectCount(self):
        for k in _test_bases:
            for height in xrange(k+2):
                for bucket_size in xrange(1, 5):
                    vh = SizedVirtualHeap(k, height, bucket_size=bucket_size)
                    self.assertEqual(vh.LeafBucketCount(),
                                     vh.BucketCountAtLevel(vh.Height()))
                    self.assertEqual(vh.LeafNodeCount(),
                                     vh.NodeCountAtLevel(vh.Height()))
                    self.assertEqual(vh.LeafSlotCount(),
                                     vh.SlotCountAtLevel(vh.Height()))

    def _assert_file_equals_baselines(self, fname, bname):
        with open(fname)as f:
            flines = f.readlines()
        with open(bname) as f:
            blines = f.readlines()
        self.assertListEqual(flines, blines)
        os.remove(fname)

    def test_WriteAsDot(self):

        for k, h, b, maxl in [(2, 3, 1, None),
                              (2, 3, 2, None),
                              (3, 3, 1, None),
                              (3, 3, 2, None),
                              (3, 10, 2, 4)]:
            if maxl is None:
                label = "k%d_h%d_b%d" % (k, h, b)
            else:
                label = "k%d_h%d_b%d" % (k, maxl-1, b)
            heap = SizedVirtualHeap(k, h, bucket_size=b)

            fname = label+".dot"
            with open(os.path.join(thisdir, fname), "w") as f:
                heap.WriteAsDot(f, max_levels=maxl)
            self._assert_file_equals_baselines(
                os.path.join(thisdir, fname),
                os.path.join(baselinedir, fname))

            data = list(range(heap.SlotCount()))
            fname = label+"_data.dot"
            with open(os.path.join(thisdir, fname), "w") as f:
                heap.WriteAsDot(f, data=data, max_levels=maxl)
            self._assert_file_equals_baselines(
                os.path.join(thisdir, fname),
                os.path.join(baselinedir, fname))

    def test_SaveImageAsPDF(self):

        for k, h, b, maxl in [(2, 3, 1, None),
                              (2, 3, 2, None),
                              (3, 3, 1, None),
                              (3, 3, 2, None),
                              (3, 10, 2, 4)]:
            if maxl is None:
                label = "k%d_h%d_b%d" % (k, h, b)
            else:
                label = "k%d_h%d_b%d" % (k, maxl-1, b)
            heap = SizedVirtualHeap(k, h, bucket_size=b)

            fname = label+".pdf"
            if os.path.exists(os.path.join(thisdir, fname)):
                os.remove(os.path.join(thisdir, fname))
            rc = heap.SaveImageAsPDF(os.path.join(thisdir, label),
                                     max_levels=maxl)

            if not has_dot:
                self.assertEqual(rc, False)
            else:
                self.assertEqual(rc, True)
                self.assertEqual(
                    os.path.exists(os.path.join(thisdir, fname)), True)
                try:
                    os.remove(os.path.join(thisdir, fname))
                except OSError:
                    pass

            data = list(range(heap.SlotCount()))
            fname = label+"_data.pdf"
            if os.path.exists(os.path.join(thisdir, fname)):
                os.remove(os.path.join(thisdir, fname))
            rc = heap.SaveImageAsPDF(os.path.join(thisdir, fname),
                                     data=data,
                                     max_levels=maxl)
            if not has_dot:
                self.assertEqual(rc, False)
            else:
                self.assertEqual(rc, True)
                self.assertEqual(
                    os.path.exists(os.path.join(thisdir, fname)), True)
                try:
                    os.remove(os.path.join(thisdir, fname))
                except OSError:
                    pass
class TestMisc(unittest.TestCase):

    def test_MaxKLabeled(self):
        MaxKLabeled()

    def test_CalculateBucketLevel(self):
        self.assertEqual(CalculateBucketLevel(2, 0), 0)
        self.assertEqual(CalculateBucketLevel(2, 1), 1)
        self.assertEqual(CalculateBucketLevel(2, 2), 1)
        self.assertEqual(CalculateBucketLevel(2, 3), 2)
        self.assertEqual(CalculateBucketLevel(2, 4), 2)
        self.assertEqual(CalculateBucketLevel(2, 5), 2)
        self.assertEqual(CalculateBucketLevel(2, 6), 2)
        self.assertEqual(CalculateBucketLevel(2, 7), 3)

        self.assertEqual(CalculateBucketLevel(3, 0), 0)
        self.assertEqual(CalculateBucketLevel(3, 1), 1)
        self.assertEqual(CalculateBucketLevel(3, 2), 1)
        self.assertEqual(CalculateBucketLevel(3, 3), 1)
        self.assertEqual(CalculateBucketLevel(3, 4), 2)
        self.assertEqual(CalculateBucketLevel(3, 5), 2)
        self.assertEqual(CalculateBucketLevel(3, 6), 2)
        self.assertEqual(CalculateBucketLevel(3, 7), 2)
        self.assertEqual(CalculateBucketLevel(3, 8), 2)
        self.assertEqual(CalculateBucketLevel(3, 9), 2)
        self.assertEqual(CalculateBucketLevel(3, 10), 2)
        self.assertEqual(CalculateBucketLevel(3, 11), 2)
        self.assertEqual(CalculateBucketLevel(3, 12), 2)
        self.assertEqual(CalculateBucketLevel(3, 13), 3)

        self.assertEqual(CalculateBucketLevel(4, 0), 0)
        self.assertEqual(CalculateBucketLevel(4, 1), 1)
        self.assertEqual(CalculateBucketLevel(4, 2), 1)
        self.assertEqual(CalculateBucketLevel(4, 3), 1)
        self.assertEqual(CalculateBucketLevel(4, 4), 1)

        self.assertEqual(CalculateBucketLevel(4, 5), 2)
        self.assertEqual(CalculateBucketLevel(4, 6), 2)
        self.assertEqual(CalculateBucketLevel(4, 7), 2)
        self.assertEqual(CalculateBucketLevel(4, 8), 2)

        self.assertEqual(CalculateBucketLevel(4, 9), 2)
        self.assertEqual(CalculateBucketLevel(4, 10), 2)
        self.assertEqual(CalculateBucketLevel(4, 11), 2)
        self.assertEqual(CalculateBucketLevel(4, 12), 2)

        self.assertEqual(CalculateBucketLevel(4, 13), 2)
        self.assertEqual(CalculateBucketLevel(4, 14), 2)
        self.assertEqual(CalculateBucketLevel(4, 15), 2)
        self.assertEqual(CalculateBucketLevel(4, 16), 2)

        self.assertEqual(CalculateBucketLevel(4, 17), 2)
        self.assertEqual(CalculateBucketLevel(4, 18), 2)
        self.assertEqual(CalculateBucketLevel(4, 19), 2)
        self.assertEqual(CalculateBucketLevel(4, 20), 2)

        self.assertEqual(CalculateBucketLevel(4, 21), 3)

    def test_clib_CalculateBucketLevel(self):
        for k in _test_bases:
            for b in xrange(CalculateBucketCountInHeapWithHeight(k, 3)+1):
                self.assertEqual(_clib.CalculateBucketLevel(k, b),
                                 CalculateBucketLevel(k, b))
        for k, b in [(89, 14648774),
                     (89, 14648775),
                     (90, 14648774),
                     (90, 14648775)]:
            self.assertEqual(_clib.CalculateBucketLevel(k, b),
                             CalculateBucketLevel(k, b))

    def test_clib_CalculateLastCommonLevel(self):
        for k in range(2, 8):
            for b1 in xrange(CalculateBucketCountInHeapWithHeight(k, 3)+1):
                for b2 in xrange(CalculateBucketCountInHeapWithHeight(k, 3)+1):
                    self.assertEqual(_clib.CalculateLastCommonLevel(k, b1, b2),
                                     CalculateLastCommonLevel(k, b1, b2))
        for k in [89,90]:
            for b1 in [0, 100, 10000, 14648774, 14648775]:
                for b2 in [0, 100, 10000, 14648774, 14648775]:
                    self.assertEqual(_clib.CalculateLastCommonLevel(k, b1, b2),
                                     CalculateLastCommonLevel(k, b1, b2))

if __name__ == "__main__":
    unittest.main()                                    # pragma: no cover
