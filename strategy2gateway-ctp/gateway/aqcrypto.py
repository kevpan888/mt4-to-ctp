"""astockquant wire crypto (clean-room).

Two distinct primitives:

1. Config/login family (self-inverse, ciphertext feedback):
       c[i] = p[i] ^ ((kb[i & 15] + prev) & 0xff)
       prev = c[i]
   Used for config files and for the 28-byte login packet with the
   static 16-byte key baked into the reference client.

2. Session channel cipher (plaintext feedback, persistent 16-nybble counter):
       enc: c[i] = p[i] ^ ((TABLE[(cnt+i) & 15] + sext(prev)) & 0xff); prev=p[i]
       dec: p[i] = c[i] ^ ((TABLE[(cnt+i) & 15] + sext(prev)) & 0xff); prev=p[i]
       prev is a *signed* byte. TABLE = MD5(password) raw digest.
   One cipher instance per direction of one TCP connection.
"""

import hashlib

# Static 16-byte key extracted from the reference client binary.
STATIC_KB = bytes([
    0x41, 0xb6, 0x7f, 0x58, 0x38, 0x0c, 0xf0, 0x2d,
    0x7b, 0x39, 0x08, 0xfe, 0x21, 0xbb, 0x41, 0x58,
])


def config_encrypt(plain, kb=STATIC_KB):
    """c[i] = p[i] ^ (kb[i&15] + c[i-1]); feedback = output ciphertext."""
    out = bytearray(len(plain))
    prev = 0
    for i, b in enumerate(plain):
        prev = b ^ ((kb[i & 15] + prev) & 0xff)
        out[i] = prev
    return bytes(out)


def config_decrypt(cipher, kb=STATIC_KB):
    """p[i] = c[i] ^ (kb[i&15] + c[i-1]); feedback = input ciphertext."""
    out = bytearray(len(cipher))
    prev = 0
    for i, b in enumerate(cipher):
        out[i] = b ^ ((kb[i & 15] + prev) & 0xff)
        prev = b
    return bytes(out)


def _sext(b):
    return b - 256 if b >= 128 else b


class ChannelCipher:
    """Session stream cipher for one direction of one TCP connection."""

    def __init__(self, table, prev=0, counter=0):
        self.table = bytes(table)
        self.prev = prev & 0xff
        self.counter = counter

    def _process(self, data, feedback_is_output):
        # Plaintext feedback: for encrypt prev is the input byte, for
        # decrypt prev is the output byte (both are the plaintext byte).
        out = bytearray(len(data))
        prev = _sext(self.prev)
        cnt = self.counter
        t = self.table
        for i, b in enumerate(data):
            k = (t[(cnt + i) & 15] + prev) & 0xff
            p = b ^ k
            out[i] = p
            prev = _sext(p if feedback_is_output else b)
        if out:
            self.prev = out[-1] if feedback_is_output else data[-1]
        self.counter = (cnt + len(data)) & 0xffffffff
        return bytes(out)

    def encrypt(self, data):
        return self._process(data, False)

    def decrypt(self, data):
        return self._process(data, True)


def session_table(password):
    """The 16-byte session table is the raw MD5 digest of the password."""
    if isinstance(password, str):
        password = password.encode("latin-1")
    return hashlib.md5(password).digest()


def self_encrypt_table(table):
    """Login-packet self-obfuscation of the TABLE:
       e[i] = t[i] ^ ((t[i] + sext(prev)) & 0xff); prev = t[i]; prev0 = 0.
       e[0] is always 0."""
    out = bytearray(16)
    prev = 0
    for i in range(16):
        t = table[i]
        out[i] = t ^ ((t + _sext(prev)) & 0xff)
        prev = t
    return bytes(out)


def recover_table(enc):
    """Inverse of self_encrypt_table. t[0] cannot be recovered from e
    (e[0]==0 for every t[0]), so all chains are returned; typically the
    caller validates a chain against the greeting (u16==0x0190) or the
    first up frame (cmd 0x08)."""
    results = []

    def dfs(i, t):
        if i == 16:
            results.append(bytes(t))
            return
        e = enc[i]
        if i == 0:
            cands = range(256)
        else:
            prev = t[i - 1]
            cands = [x for x in range(256)
                     if (x ^ ((x + _sext(prev)) & 0xff)) == e]
        for x in cands:
            t.append(x)
            dfs(i + 1, t)
            t.pop()

    dfs(0, bytearray())
    return results
