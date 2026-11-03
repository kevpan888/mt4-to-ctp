"""astockquant wire LZSS decompressor (clean-room port).

aPLib-style byte compressor. Wire body trailer: last two bytes 0x00,0x00
and the third-last byte 0x11.

Three loop entries after matches:
  TOP        : c < 0x10 -> 3-byte short match
  CMD        : c < 0x10 -> literal block of c+3
  POST_TRAIL : c < 0x10 -> 2-byte short match
"""

import os as _os

_TRACE = _os.environ.get("LZSS_TRACE")

_TOP, _CMD, _POST = 0, 1, 2


def _tr(msg):
    if _TRACE:
        print(msg)


def _extend_count(src, p, end, base):
    """Zero-byte extended length: sum(0xff per zero) + next byte + base."""
    acc = 0
    while True:
        if p >= end:
            return None
        b = src[p]
        p += 1
        if b != 0:
            return acc + b + base, p
        acc += 0xff


def _copy_match(out, dist, n):
    w = len(out)
    if dist > w:
        return False
    for k in range(n):
        out.append(out[w - dist + k])
    return True


def decompress(src, max_out):
    n = len(src)
    # prologue: srclen >= 3, trailer [.., 0x11, 0x00, 0x00]
    if n < 3:
        return None
    if src[n - 1] != 0 or src[n - 2] != 0 or src[n - 3] != 0x11:
        return None
    out = bytearray()
    p = 0

    # ---- initial literal run: b0 - 0x11; b0 <= 0x11 means zero run and
    #      the same byte is re-read as a CMD command ----
    b0 = src[0]
    _tr("b0=%02x" % b0)
    if b0 > 0x11:
        run = b0 - 0x11
        p = 1
        if run > max_out or p + run + 1 > n:
            return None
        out += src[p:p + run]
        p += run
        if run < 4:
            entry = _POST
        else:
            entry = _TOP
    else:
        entry = _CMD

    while True:
        # ---------- read command byte per entry state ----------
        if entry == _CMD:
            if p >= n:
                return None
            c = src[p]
            p += 1
            if c < 0x10:
                # literal block, total length c+3 (extended when c == 0)
                if c == 0:
                    r = _extend_count(src, p, n, 0x0F)
                    if r is None:
                        return None
                    lit, p = r
                else:
                    lit = c
                length = lit + 3
                _tr("p=%d LIT len=%d" % (p, length))
                if len(out) + length > max_out or p + length > n:
                    _tr("  LIT fail")
                    return None
                out += src[p:p + length]
                p += length
                entry = _TOP
                continue
        else:
            if p >= n:
                return None
            c = src[p]
            p += 1
            if c < 0x10:
                if entry == _TOP:
                    # 3-byte short match
                    if p >= n:
                        return None
                    ob = src[p]
                    p += 1
                    dist = 0x801 + ob * 4 + (c >> 2)
                    _tr("p=%d SHORT3 c=%02x ob=%02x dist=%d out=%d"
                        % (p, c, ob, dist, len(out)))
                    if not _copy_match(out, dist, 3) or \
                            len(out) > max_out:
                        _tr("  SHORT3 fail")
                        return None
                    trail = ob & 3
                    if trail == 0:
                        entry = _CMD
                    else:
                        if p + trail > n or \
                                len(out) + trail > max_out:
                            return None
                        out += src[p:p + trail]
                        p += trail
                        entry = _POST
                    continue
                # POST_TRAIL: 2-byte short match
                if p >= n:
                    return None
                ob = src[p]
                p += 1
                d = ob * 4 + (c >> 2)
                if d + 1 > len(out) or len(out) + 2 > max_out:
                    return None
                w = len(out)
                out.append(out[w - d - 1])
                out.append(out[w - d])
                trail = c & 3
                if trail == 0:
                    entry = _CMD
                else:
                    if p + trail > n or \
                            len(out) + trail > max_out:
                        return None
                    out += src[p:p + trail]
                    p += trail
                    entry = _POST
                continue

        # ---------- long match (c >= 0x10) ----------
        if c < 0x20:
            part = (c & 8) * 0x800
            length = c & 7
            if length == 0:
                r = _extend_count(src, p, n, 7)
                if r is None:
                    return None
                length, p = r
            if p + 2 > n:
                return None
            word = src[p] | (src[p + 1] << 8)
            p += 2
            pre = part + (word >> 2)
            if pre == 0:
                # end marker: wire form is 10 11 00 00 (the final three
                # bytes are the prologue trailer). length is ignored.
                if len(out) != max_out or p != n:
                    return None
                return bytes(out)
            dist = pre + 0x4000
            total = length + 2
        elif c < 0x40:
            length = c & 0x1F
            if length == 0:
                r = _extend_count(src, p, n, 0x1F)
                if r is None:
                    return None
                length, p = r
            if p + 2 > n:
                return None
            word = src[p] | (src[p + 1] << 8)
            p += 2
            dist = 1 + (word >> 2)
            total = length + 2
        else:
            if p >= n:
                return None
            ob = src[p]
            p += 1
            dist = 1 + ((c >> 2) & 7) + ob * 8
            total = (c >> 5) + 1

        _tr("p=%d LONG c=%02x dist=%d n=%d out=%d"
            % (p, c, dist, total, len(out)))
        if not _copy_match(out, dist, total) or len(out) > max_out:
            _tr("  LONG fail")
            return None

        # trailing literal count (low 2 bits):
        #   word forms (<0x40): low byte of the parameter word
        #   byte form (>=0x40): the command byte itself
        trail = src[p - 2] & 3 if c < 0x40 else (c & 3)
        if trail:
            if p + trail > n or len(out) + trail > max_out:
                return None
            out += src[p:p + trail]
            p += trail
            entry = _POST
        else:
            entry = _CMD


def compress(body):
    """Literal-only encoder; output is accepted by decompress() above.

    No match search (size-optimal compression is not needed on this
    wire): the whole body is emitted as literals, then the end marker
    (long-match word form with pre == 0, wire bytes 10 11 00 00 whose
    last three bytes double as the prologue trailer).

    Layout by body size n:
      n == 0      -> 11 00 00
                     (b0=0x11 -> CMD entry; 0x11 word=0 -> pre==0 end)
      1..238      -> [0x11+n] body 10 11 00 00
                     (initial literal run; run<4 enters POST, else TOP,
                      both read 0x10 as the long-match end marker)
      >=239       -> 00 [00*z][last] body 10 11 00 00
                     (b0=0 -> CMD entry, c=0 extended LIT: block length
                      = 0xff*z + last + 0x0F + 3, last != 0)
    """
    n = len(body)
    if n == 0:
        return b"\x11\x00\x00"
    if n <= 238:
        return bytes([0x11 + n]) + bytes(body) + b"\x10\x11\x00\x00"
    rem = n - 3 - 0x0F            # extended count target minus base
    z, last = divmod(rem, 0xFF)
    if last == 0:
        z -= 1
        last = 0xFF
    return (b"\x00" + b"\x00" * z + bytes([last]) + bytes(body)
            + b"\x10\x11\x00\x00")
