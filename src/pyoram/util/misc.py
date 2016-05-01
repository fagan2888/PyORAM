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
