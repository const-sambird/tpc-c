import random

def urand(x, y) -> int:
    return random.randrange(x, y + 1)

def NURand(A: int, C: int, x: int, y: int) -> int:
    '''
    Generates a non-uniform random number, according to clause 2.1.6 of the TPC-C specification.
    
    From the TPC-C specification:

    ```
    NURand(A, x, y) = (((random (0, A) | random(x, y)) + C) % (y - x + 1)) + x
    ```

    Where:

    - `exp-1 | exp-2` stands for the bitwise logical OR operation between exp -1 and exp-2
    - `exp-1 % exp-2` stands for exp-1 modulo exp-2
    - `random(x, y)` stands for randomly selected within [x .. y]
    - `A` is a constant chosen according to the size of the range [x .. y]
        for C_LAST, the range is [0 .. 999] and A = 255
        for C_ID, the range is [1 .. 3000] and A = 1023
        for OL_I_ID, the range is [1 .. 100000] and A = 8191
    - `C` is a run-time constant randomly chosen within [0 .. A] that can be varied without altering performance.
        The same C value, per field (C_LAST, C_ID, and OL_I_ID), must be used by all emulated terminals.
    '''
    return (((urand(0, A) | urand(x, y)) + C) % (y - x + 1)) + x

def NURand_CLAST(x, y):
    A = 255
    C = random.randrange(0, 999 + 1)
    return NURand(A, C, x, y)

def NURand_CID(x, y):
    A = 1023
    C = random.randrange(1, 3000 + 1)
    return NURand(A, C, x, y)

def NURand_OLIID(x, y):
    A = 8191
    C = random.randrange(1, 100000 + 1)
    return NURand(A, C, x, y)

def alphastr(minlen, maxlen):
    strlen = urand(minlen, maxlen)
    str = ''
    BASIS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'

    for _ in range(strlen):
        char = random.choice(BASIS)
        str += char

    return str

def numstr(minlen, maxlen):
    strlen = urand(minlen, maxlen)
    str = ''
    BASIS = '0123456789'

    for _ in range(strlen):
        char = random.choice(BASIS)
        str += char

    return str

def lastname(num):
    num = '%03d' % num
    TOKENS = ['BAR', 'OUGHT', 'ABLE', 'PRI', 'PRES', 'ESE', 'ANTI', 'CALLY', 'ATION', 'EING']
    name = ''

    for digit in num:
        name += TOKENS[int(digit)]

    return name
