import cffi

#
# C functions that speed up commonly
# executed heap calculations in tree-based
# orams
#

ffi = cffi.FFI()
ffi.cdef(
"""
int CalculateBucketLevel(unsigned int k,
                         unsigned long b);
int CalculateLastCommonLevel(unsigned int k,
                             unsigned long b1,
                             unsigned long b2);
""")

ffi.set_source("pyoram.tree._virtualheap",
"""
#include <stdio.h>
#include <stdlib.h>

int CalculateBucketLevel(unsigned int k,
                         unsigned long b)
{
   unsigned int h;
   unsigned long pow;
   if (k == 2) {
      // This is simply log2floor(b+1)
      h = 0;
      b += 1;
      while (b >>= 1) {++h;}
      return h;
   }
   b = (k - 1) * (b + 1) + 1;
   h = 0;
   pow = k;
   while (pow < b) {++h; pow *= k;}
   return h;
}

int CalculateLastCommonLevel(unsigned int k,
                             unsigned long b1,
                             unsigned long b2)
{
   int level1, level2;
   level1 = CalculateBucketLevel(k, b1);
   level2 = CalculateBucketLevel(k, b2);
   if (level1 != level2) {
      if (level1 > level2) {
         while (level1 != level2) {
            b1 = (b1 - 1)/k;
            --level1;
         }
      }
      else {
         while (level2 != level1) {
            b2 = (b2 - 1)/k;
            --level2;
         }
      }
   }
   while (b1 != b2) {
      b1 = (b1 - 1)/k;
      b2 = (b2 - 1)/k;
      --level1;
   }
   return level1;
}
""")

if __name__ == "__main__":
    ffi.compile()
