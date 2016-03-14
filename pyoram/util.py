
# floor(log2(n))
def log2floor(n):
    assert n > 0
    return n.bit_length() - 1

# ceil(log2(n))
def log2ceil(n):
    assert n > 0
    if n == 1:
        return 0
    return log2floor(n-1) + 1
