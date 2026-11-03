"""astockquant main-channel wire grammar (clean-room).

Stream layout per TCP connection:
  UP:
    [28B login: u8 type(raw) + 27B config-cipher]
    then an interleaved token stream:
      - encrypted tokens (advance the channel cipher): [u8 cmd][body]
      - RAW blocks that do NOT advance the channel cipher:
          cmd 0x01: 1B + 0x250 config-cipher account request
          cmd 0x11: 1B + 0x110 raw public-key blob
  DOWN:
    [1B raw status] [7B encrypted greeting: u8 tick,u16 0x0190,u32 srvval]
    optional raw cmd1 answer: 1B ack + 0x20 config-cipher
    then encrypted traffic: per-request acks / fixed reads / LZSS frames,
    plus unsolicited push frames (ticks).
"""

import struct

# Encrypted upstream commands with fixed body size.
UP_FIXED_BODY = {
    0x04: 0x14,   # ack only; u32 8 + 0x10 blob
    0x08: 0x10,   # symbols (incremental hash); reply frame, 0x790 step
    0x09: 0x20,
    0x0A: 0x10,   # reply frame, 0x50 step
    0x1E: 0x04,   # trades sync; reply frame 0x3600 + n*0xe0
    0x22: 0x08,   # history; reply frame n*0xe0
    0x9D: 0x04,   # ping (no reply)
    0xBE: 0x60,   # trade request
    0xBF: 0x04,
}

# One-byte encrypted commands.
UP_ONEBYTE = {0x10, 0x21, 0xD0, 0x02}   # 0x02: periodic keepalive
# (proven live: the stream realigns when 0x02 is consumed as 1B;
# symmetric to the downlink 0x02 no-op marker)

# Raw blocks that skip the channel cipher entirely.
UP_RAW = {
    0x01: 0x250,   # account/config request, body config-cipher
    0x11: 0x110,   # public-key blob
}

# Variable encrypted body from the already-decrypted header:
# returns total frame length (cmd included) or None.
def enc_frame_len(hdr, avail):
    c = hdr[0]
    if c in UP_ONEBYTE:
        return 1
    if c in UP_FIXED_BODY:
        n = 1 + UP_FIXED_BODY[c]
        return n if avail >= n else 0
    if c in (0x96, 0x9C):
        if len(hdr) < 3:
            return 0
        (cnt,) = struct.unpack_from("<H", hdr, 1)
        n = 3 + (2 if c == 0x96 else 0x16) * cnt
        return n if avail >= n else 0
    if c == 0x0F:  # 00 u32 ver u32 build u8 namelen name
        if len(hdr) < 11:
            return 0
        n = 11 + hdr[10]
        if n > 0x400:
            return None
        return n if avail >= n else 0
    if c == 0x07:  # 0xe0 struct + u32 bloblen + blob
        if len(hdr) < 1 + 0xE0 + 4:
            return 0
        (bl,) = struct.unpack_from("<I", hdr, 1 + 0xE0)
        if bl > 0x10000:
            return None
        n = 1 + 0xE0 + 4 + bl
        return n if avail >= n else 0
    if c == 0x05:  # u32 pathlen + path
        if len(hdr) < 5:
            return 0
        (bl,) = struct.unpack_from("<I", hdr, 1)
        if bl > 0x10000:
            return None
        n = 5 + bl
        return n if avail >= n else 0
    return None


CMD_NAMES = {
    0x00: "CMD0", 0x01: "ACCT_RAW", 0x02: "KEEPALIVE", 0x04: "CMD4",
    0x05: "FILE", 0x07: "CMD7", 0x08: "SYMBOLS", 0x09: "CMD9",
    0x0A: "GROUPS", 0x0F: "INFO", 0x10: "REQ10", 0x11: "PUBKEY_RAW",
    0x1E: "TRADES_SYNC", 0x21: "REQ21", 0x22: "HISTORY",
    0x87: "REQUOTE", 0x96: "SUB_SYMBOLS", 0x97: "TICK",
    0x9C: "SUB_QUOTES", 0x9D: "PING", 0xBE: "TRADE_REQ",
    0xBF: "TRADE_ACK", 0xD0: "LOGOUT",
}


def cmd_name(c):
    return CMD_NAMES.get(c, "0x%02X" % c)
