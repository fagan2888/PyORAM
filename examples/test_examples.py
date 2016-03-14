import os
import glob
import sys
import unittest

import pyoram

thisfile = os.path.abspath(__file__)
thisdir = os.path.dirname(thisfile)
examples = glob.glob(os.path.join(thisdir,"*.py"))

assert thisfile in examples
examples.remove(thisfile)
assert thisfile not in examples

def _execute_example(filename):
    assert os.path.exists(filename)
    rc = os.system(sys.executable+' '+filename)
    assert not rc

test_methods = {}
for fname in examples:
    basename = os.path.basename(fname)
    assert basename.endswith('.py')
    assert len(basename) >= 3
    basename = basename[:-3]
    test_methods['test_'+basename] = lambda self: _execute_example(fname)
assert len(test_methods) == len(examples)

Test = type("Test", (unittest.TestCase,), test_methods)

if __name__ == "__main__":
    unittest.main() # pragma: no cover
