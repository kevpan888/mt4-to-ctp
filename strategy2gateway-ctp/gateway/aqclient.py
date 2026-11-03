"""Standalone astockquant broker-protocol client (clean-room).

M0: direct connect -> 28B login -> 1B status + 7B greeting ->
    0x9D ping every 10s (client FUN_00505a70 semantics).
M1: init token sequence replayed from gold standard
    (10 / 08 / 0a / 21 / 22 / 1e / 96 / 9c), conditional-ack init
    answers, then push loop: 0x97 ticks, 0x9c quote stats, LZSS
    frames, 0x02 idle markers.

Credentials: env AQ_LOGIN / AQ_PASS (or CLI). Never written to disk.
"""
import argparse
import binascii
import os
import select
import socket
import struct
import sys
import threading
import time

from aqcrypto import (STATIC_KB, ChannelCipher, config_encrypt,
                      self_encrypt_table, session_table)
import aqproto
import lzss

LOGIN_LEN = 28
GREET_LEN = 7
VER = 0x0190
BUILD = 0x00E1

# 0x1e answer: fixed user+group block, then TradeRecords appended.
USER_BLOCK = 0x3600
# TradeRecord (open position / history order / balance op), 0xE0 bytes:
#   +0x00 u32 ticket          +0x04 u32 login
#   +0x08 char[12] symbol     +0x18 u32 cmd (6=balance op)
#   +0x1C u32 volume(0.01 lots; M4 probe-proven, see _parse_trade)
#   +0x20 u32 open_time
#   +0x28 f64 cur close px    +0x30 f64 sl       +0x38 f64 tp
#   +0x40 u32 close_time      +0x50 f64 lots_raw (min-lot clamp, NOT truth)
#   +0x78 f64 open_price      +0x80 f64 profit
#   +0x94 char[44] comment    +0xC0 f64 margin_rate
#   +0xC8 u32 modify_time
TRADE_REC = 0xE0
# 0x9c chart bar record, 28 bytes (new-format frames, build >= 0x145):
#   +0x00 u32 bar_time
#   +0x04 u32 open (integer points; price = value / 10**digits)
#   +0x08 i32 high_offset / +0x0C i32 low_offset / +0x10 i32 close_offset
#         (all signed deltas from open, in points; OHLC-validated)
#   +0x14 u32 tick volume
#   +0x18 u32 aux (observed f32 ~avg/turnover; kept raw)
BAR_REC = 28

# ---------------- M3: trade transaction (upstream 0xbe) ----------------
# Empirical wire truth from gold capture 20261003_135658 conn2/3/4:
# the server expects DEDICATED trade connections (28B login -> greeting
# -> 0xbe requests with 0x02 keepalives -> 0x0d; NO init tokens there).
# Cross-checked with server log "market buy/sell ... order #1001x" and
# crc arithmetic.  Sending 0xbe on the quote/init connection (after
# 0x96/0x9c subscriptions) is rejected by the server's pumping loop:
# "unknown command BE in pumping [intruder]" + disconnect.
#   [0xBE][0x60 body]  body:
#   +0x00 u16 type   0x42=open 0x43=close (wire; server-side switch
#                    renders internal +9-shifted cases 0x4b..0x53)
#   +0x02 u16 cmd    0=buy 1=sell (log-proven: cmd0 -> "market buy"
#                    #10010 @15001=ask; cmd1 -> "market sell" #10011
#                    @15000=bid)
#   +0x04 u32 order  ticket >=1 for close (server validates)
#   +0x10 u32 reserved (zero)
#   +0x0C char[12] symbol (wire-proven byte-exact vs conn2 up_plain)
#   +0x18 u32 volume in 0.01-lot units (1.0 lot = 100, wire-proven)
#   +0x1C f64 price  0.0 accepted for market open (server fills at its
#                    own feed; close needs a real price > epsilon)
#   +0x24 f64 sl     +0x2C f64 tp
#   +0x34 u32 reserved
#   +0x38 char[31] comment
#   +0x58 u32 expiration (pendings only)
#   +0x5C u32 crc = login + sum(body[0..0x5b])
#   (verified byte-exact vs conn2 request: 88200 + (0x42+'ag'+100)
#   = 0x159f6 = captured crc f6590100)
# Replies (downstream on the trade conn):
#   [0xBE][u32 x][u8 reqtype][u8 status]
#     status=0        -> payload: frame [u32 P][u32 W]+LZSS, 0xE0 records
#     status=0x8e/8f  -> progress notes, more replies follow
#     other status    -> error, stop (0x84=132 market closed,
#                        0x88=136 off quotes, 0x83=131 invalid volume,
#                        0x86=134 not enough money, ...)
#   status=0 with reqtype=0 -> 0x10 raw prices (requote challenge)
#   [0xBF][u32 x][u8 status] plain reject
# Client 0xbe wire types (server dispatcher FUN_004188f0 switches on
# the RAW first byte - NO +9 shift; FUN_0044f890 with cases 0x4b..0x53
# is the manager channel):
#   0x42 open market (cmd 0/1)          FUN_0044a0f0
#   0x43 place pending (cmd 2..5!)      FUN_0044aa70
#   0x44 close INSTANT-execution symbol FUN_0044b7d0 (price>0, mode==1)
#   0x46 close MARKET-execution symbol  FUN_0044c320 (price==0.0,
#       mode==2; order@+4=ticket; cmd/symbol/price overwritten from
#       the order record; volume<=order volume for partial close)
TT_OPEN, TT_PEND, TT_CLOSE, TT_CLOSE_MKT = 0x42, 0x43, 0x44, 0x46
# 0x47 modify (FUN_0044e9c0): cmd MUST equal the existing order's cmd.
#   position (cmd 0/1): price MUST be 0.0 (live px -> error 0x01), only
#   sl/tp change; pending (cmd 2..5): price is the NEW pending price and
#   expiration@0x58 may change.
# 0x48 delete pending (FUN_0044f1f0): only ticket + ownership checked,
#   existing order must be a pending (cmd >= 2).
# 0x4a close ALL orders (probe-proven: server log
#   "close all orders 'ag' completed, N order(s) remained")
TT_MODIFY, TT_DELETE = 0x47, 0x48
# Pending placement (FUN_0044aa70): cmd 2..5, price/sl/tp all MUST be
# > 0-or-0 within bounds (price > eps, unlike market open's price==0);
# expiration u32@0x58 != 0 without group permission bit 3 -> 0x93
# (ERR_TRADE_EXPIRATION_DENIED), so GTC uses 0.
OP_BUY, OP_SELL, OP_BUY_LIMIT, OP_SELL_LIMIT, OP_BUY_STOP, OP_SELL_STOP \
    = range(6)


def hexs(b, n=48):
    b = bytes(b)
    s = binascii.hexlify(b[:n]).decode()
    if len(b) > n:
        s += "..(+%d)" % (len(b) - n)
    return s


def login_packet(login_u32, table, flag=0):
    """28-byte login: 1B raw type=0 + 27B config-cipher(STATIC_KB).
    flag=0 quote/init conn, flag=1 trade conn (gold-proven: conn1
    ends ..1e10000, trade conns conn2/3/4 end ..1e10001; flag=0 makes
    the server treat 0xbe as intruder and reset)."""
    body = bytearray(27)
    body[0] = int(time.time()) & 0xFF          # time_lo
    body[1] = 0
    body[2:18] = self_encrypt_table(table)      # self-enc TABLE
    struct.pack_into("<I", body, 18, login_u32)
    struct.pack_into("<H", body, 22, VER)
    struct.pack_into("<H", body, 24, BUILD)
    body[26] = flag
    return bytes([0]) + config_encrypt(bytes(body), STATIC_KB)


class AQClient:
    def __init__(self, host, port, login, password, verbose=False,
                 dump_dir=None):
        self.host = host
        self.port = port
        self.login = login
        self.table = session_table(password)
        self.verbose = verbose
        self.dump_dir = dump_dir
        self.dump_seq = 0
        if dump_dir:
            os.makedirs(dump_dir, exist_ok=True)
        self.sock = None
        self.send_lock = threading.Lock()
        self.up_cph = None          # send cipher
        self.dn_cph = None          # recv cipher
        self.expect = []            # init answer queue
        self.pending = bytearray()  # decrypted down stream
        self.halt = False
        self.last_down = time.time()
        self.n_ticks = 0
        self.t0 = time.time()
        self.sym_names = {}         # index -> name (best effort)
        self.sym_digits = {}        # name -> price digits
        self.last_tick = {}         # index -> (time, bid, ask)
        self.trade_events = []      # (t, desc) down 0xbe/0xbf replies
        self.account = None         # parsed 0x1e user block
        self.positions = []         # open positions (cmd 0/1, close_time=0)
        self.orders = []            # live pending orders (cmd 2..5)
        self.history = []           # closed records from 0x22 frame
        self.frames = []            # decompressed (cmd, data) stash
        # (name, period) -> {bar_time: bar dict}; filled by 0x9c frames
        self.bars = {}
        # (name, period) -> {"events": set(threading.Event)}; concurrent
        # request_bars on the same key coalesce onto ONE wire request and
        # all callers are signalled by _ingest_bars.
        self._bar_waiters = {}
        self._bar_send_lock = threading.Lock()
        # M6: push listeners, invoked on the pump thread.  Callbacks
        # MUST be cheap/non-blocking (gateway drops on a full queue).
        self._listeners_lock = threading.Lock()
        self._tick_listeners = {}    # qid -> fn(dict)
        self._bar_listeners = {}     # qid -> fn(name, period, bar, new)
        self._listener_seq = 0
        # M7: news/mail inbox fed by 0x98 pushes (server-side push wire)
        self.news = []

    def log(self, msg):
        print("[%7.2f] %s" % (time.time() - self.t0, msg), flush=True)

    # ---------------- low-level ----------------
    def send_raw(self, data):
        with self.send_lock:
            self.sock.sendall(data)

    def send_enc(self, cmd, body=b""):
        pl = bytes([cmd]) + body
        with self.send_lock:
            self.sock.sendall(self.up_cph.encrypt(pl))
        if self.verbose:
            self.log("UP 0x%02x %-11s n=%d %s"
                     % (cmd, aqproto.cmd_name(cmd), len(pl), hexs(body, 24)))

    # ---------------- session ----------------
    def connect(self):
        self.sock = socket.create_connection((self.host, self.port),
                                             timeout=10)
        self.sock.settimeout(1.0)
        self.send_raw(login_packet(self.login, self.table))
        # status byte + greeting
        st = self._recv_exact(1)[0]
        if st != 0:
            raise RuntimeError("login status=0x%02x (rejected)" % st)
        self.dn_cph = ChannelCipher(self.table)
        g = self.dn_cph.decrypt(self._recv_exact(GREET_LEN))
        ver, sval = struct.unpack_from("<HI", g, 1)
        self.up_cph = ChannelCipher(self.table)
        self.log("LOGIN ok login=%d ver=0x%04x srvval=0x%08x greeting=%s"
                 % (self.login, ver, sval, g.hex()))
        if ver != VER:
            self.log("WARN greeting ver 0x%04x != 0x%04x" % (ver, VER))
        self.last_down = time.time()

    def _recv_exact(self, n):
        buf = bytearray()
        while len(buf) < n:
            b = self.sock.recv(n - len(buf))
            if not b:
                raise ConnectionError("closed while waiting %d bytes" % n)
            buf += b
        return bytes(buf)

    # ---------------- init sequence (gold-standard replay) ----------
    def handshake(self, symbols=("ag",)):
        def sym_sub(name, period, last, flags=0):
            rec = name.encode("latin-1")[:12].ljust(12, b"\x00")
            return rec + struct.pack("<IIH", period, last, flags)

        now = int(time.time())
        self.send_enc(0x10)
        self.expect += [("ack10", 0x10)]
        # cache hashes replayed verbatim from the gold standard; if the
        # server disagrees it pushes the full tables as 0x9x frames.
        self.send_enc(0x08, bytes.fromhex(
            "3ff3bde75d68d251b76baa6af34ce177"))
        self.expect += [("ack_frame", 0x08)]
        self.send_enc(0x0A, bytes.fromhex(
            "b9c5a32c7db52b43d65277e439598bb7"))
        self.expect += [("ack_frame", 0x0A)]
        self.send_enc(0x21)
        self.expect += [("ack21", 0x21)]
        self.send_enc(0x22, struct.pack("<II", 0, now))
        self.expect += [("ack_frame", 0x22)]
        self.send_enc(0x1E, struct.pack("<I", 0))
        self.expect += [("ack_frame", 0x1E)]
        # subscribe every symbol index 0..1023
        self.send_enc(0x96, struct.pack("<H", 1024)
                      + b"".join(struct.pack("<H", i)
                                 for i in range(1024)))
        # quote subscription per symbol (no sync reply)
        body = b"".join(sym_sub(s, 1, now) for s in symbols)
        self.send_enc(0x9C, struct.pack("<H", len(symbols)) + body)
        self.log("handshake sent (8 tokens), expect queue=%d"
                 % len(self.expect))

    # ---------------- receive loop ----------------
    def _advance_decrypt(self, raw):
        cp = ChannelCipher(self.table, self.dn_cph.prev,
                           self.dn_cph.counter)
        pl = cp.decrypt(raw)
        self.dn_cph = cp
        return pl

    def _parse_stream(self):
        p = self.pending
        while p and not self.halt:
            if self.expect:
                if not self._step_init(p):
                    return
            elif not self._step_push(p):
                return
        if not self.expect and not self.halt:
            pass

    # ---- init answers: strictly serial, conditional on ack byte ----
    def _step_init(self, p):
        want, cmd = self.expect[0]
        if want == "ack10":
            if not p:
                return False
            ack = p[0]
            del p[:1]
            self.log("DN ack(0x10) = %d" % ack)
            if ack == 0:
                self.expect[0:1] = [("byte", cmd), ("frame", cmd)]
            else:
                self.expect.pop(0)
            return True
        if want in ("ack_frame", "ack21"):
            if not p:
                return False
            ack = p[0]
            del p[:1]
            self.log("DN ack(cmd=0x%02x) = %d" % (cmd, ack))
            if ack == 0:
                self.expect[0:1] = [("rec21", cmd)] if want == "ack21" \
                    else [("frame", cmd)]
            else:
                self.expect.pop(0)
            return True
        if want == "byte":
            if not p:
                return False
            self.log("DN byte = 0x%02x" % p[0])
            del p[:1]
            self.expect.pop(0)
            return True
        if want == "rec21":     # u32 cnt + cnt*0x44
            if len(p) < 4:
                return False
            cnt = struct.unpack_from("<I", p, 0)[0]
            total = 4 + cnt * 0x44
            if len(p) < total:
                return False
            self.log("DN 0x21 records=%d n=%d" % (cnt, total))
            self._dump("init21_rec", p[:total])
            del p[:total]
            self.expect.pop(0)
            return True
        if want == "frame":
            vf = self._frame_at(p)
            if vf is None:
                return False
            P, W = vf
            body = bytes(p[8:8 + W])
            # frame body is the pure LZSS/stored payload - no inner cmd
            if P > len(body):
                plain = self._try_lzss(body, P)
            else:
                plain = body
            self.log("DN FRAME cmd=0x%02x P=%d W=%d lzss=%s"
                     % (cmd, P, W, bool(plain)))
            self._dump("init%02x" % cmd, plain or body)
            self._stash(plain or body, cmd)
            del p[:8 + W]
            self.expect.pop(0)
            return True
        self.log("unknown expect %r; halt" % want)
        self.halt = True
        return False

    # ---- push loop ----
    def _step_push(self, p):
        cmd = p[0]
        if cmd == 0x97:                     # ticks
            if len(p) < 2:
                return False
            cnt = p[1]
            total = 2 + cnt * 0x0E
            if len(p) < total:
                return False
            for i in range(cnt):
                pre, t, bid, ask = struct.unpack_from("<HIff", p,
                                                      2 + i * 0x0E)
                # synthetic 08:00:00 ticks carry bid=0 + denormal ask
                # (~1e-38, prints as 0.00000) - require a real two-sided
                # quote and never let an older tick overwrite a newer one
                if bid > 1e-6 and ask > 1e-6:
                    prev = self.last_tick.get(pre)
                    if prev is None or t >= prev[0]:
                        self.last_tick[pre] = (t, bid, ask)
                        self._emit_tick({"index": pre,
                                         "symbol": self.sym_names.get(
                                             pre, "#%d" % pre),
                                         "time": t, "bid": bid,
                                         "ask": ask})
                self.n_ticks += 1
                if self.n_ticks <= 20 or self.n_ticks % 100 == 0:
                    name = self.sym_names.get(pre, "#%d" % pre)
                    self.log("TICK %-12s %s bid=%.5f ask=%.5f"
                             % (name, time.strftime("%H:%M:%S",
                                                    time.localtime(t)),
                                bid, ask))
            del p[:total]
            return True
        if cmd == 0x9C:                     # chart bars
            # [1:13] name, u32 period@13, u32 window-start@17,
            # u32 cnt@21, cnt x 28B bars (M5: 000001 D1 two 512-bar
            # frames newest->oldest + one live placeholder frame)
            if len(p) < 25:
                return False
            period, win_start, cnt = struct.unpack_from("<III", p, 13)
            total = 25 + cnt * BAR_REC
            if cnt > 0x10000 or len(p) < total:
                return False
            name = bytes(p[1:13]).split(b"\x00")[0].decode(
                "latin-1", "replace")
            raw = bytes(p[25:25 + cnt * BAR_REC])
            n_ing = self._ingest_bars(name, period, win_start, raw)
            if self.verbose:
                self.log("PUSH 0x9c sym=%r per=%d cnt=%d ingested=%d"
                         % (name, period, cnt, n_ing))
            del p[:total]
            return True
        if cmd in (0x99, 0x9A, 0x9B, 0xAB):  # LZSS push frame
            if len(p) < 9:
                return False
            vf = self._frame_at(p[1:])
            if vf is None:
                return False
            P, W = vf
            plain = self._try_lzss(bytes(p[9:9 + W]), P)
            self.log("PUSH 0x%02x P=%d W=%d lzss=%s %s"
                     % (cmd, P, W, bool(plain), hexs(p[9:9 + 32])))
            if cmd == 0x9B:
                self._handle_trade_notify(plain or b"")
            self._dump("push%02x" % cmd, plain or bytes(p[9:9 + W]))
            self._stash(plain or bytes(p[9:9 + W]), cmd)
            del p[:1 + 8 + W]
            return True
        if cmd == 0x9D:                     # u32 + frame
            if len(p) < 5:
                return False
            vf = self._frame_at(p[5:])
            if vf is None:
                return False
            P, W = vf
            u32 = struct.unpack_from("<I", p, 1)[0]
            body9d = bytes(p[5 + 8:5 + 8 + W])
            if P > len(body9d):
                plain = self._try_lzss(body9d, P)
            else:
                plain = body9d
            self.log("PUSH 0x9d u32=%d P=%d W=%d lzss=%s"
                     % (u32, P, W, bool(plain)))
            self._dump("push9d_u%d" % u32, plain or body9d)
            self._stash(plain or body9d, 0x9D)
            del p[:5 + 8 + W]
            return True
        if cmd == 0x98:                     # news/mail, var length
            # reference-client receive-side wire (authoritative):
            #   [u32 id][u32 time][char[32] category]
            #   [u16 n1 2..255][subject(n1-1)][u8 lang]
            #   [u16 n2 0..63][keywords(n2)]
            #   [u16 n3 0..255][extra(n3)]
            #   [u32 body_size]: <1 -> end; 1..0x8000 ->
            #   [u32 comp_size][LZSS(comp_size) -> body(body_size)]
            if len(p) < 1 + 0x2A:
                return False
            n1 = struct.unpack_from("<H", p, 1 + 0x28)[0]
            if not (2 <= n1 < 0x100):
                return False
            off = 1 + 0x2A + (n1 - 1) + 1     # skip subj + lang byte
            if len(p) < off + 2:
                return False
            n2 = struct.unpack_from("<H", p, off)[0]
            if n2 >= 0x40:
                return False
            off += 2 + n2
            if len(p) < off + 2:
                return False
            n3 = struct.unpack_from("<H", p, off)[0]
            if n3 >= 0x100:
                return False
            off += 2 + n3
            if len(p) < off + 4:
                return False
            body_size = struct.unpack_from("<i", p, off)[0]
            off += 4
            comp = b""
            if 1 <= body_size <= 0x8000:
                if len(p) < off + 4:
                    return False
                comp_size = struct.unpack_from("<i", p, off)[0]
                off += 4
                if comp_size < 1 or comp_size > 0x100000:
                    return False
                if len(p) < off + comp_size:
                    return False
                comp = bytes(p[off:off + comp_size])
                off += comp_size
            total = off
            self._handle_news(p[1:total], body_size, comp)
            self._dump("push98_news", p[:total])
            del p[:total]
            return True
        if cmd == 0xBE:                     # trade reply / record push
            return self._step_push_be(p)
        if cmd == 0xBF:                     # plain reject [u32][u8]
            if len(p) < 6:
                return False
            x, st = struct.unpack_from("<IB", p, 1)
            self.log("DOWN 0xbf x=0x%08x status=0x%02x" % (x, st))
            self.trade_events.append((time.time(), "bf", x, st))
            self._dump("downbf", p[:6])
            del p[:6]
            return True
        if cmd == 0x02:                     # idle marker
            del p[:1]
            return True
        if cmd == 0x0D:
            self.log("PUSH 0x0d disconnect marker")
            del p[:1]
            self.halt = True
            return True
        self.log("PUSH unknown cmd 0x%02x: %s" % (cmd, hexs(p[:24])))
        self.halt = True
        return False

    # ---------------- helpers ----------------
    def _dump(self, tag, data):
        if not self.dump_dir:
            return
        self.dump_seq += 1
        fn = os.path.join(self.dump_dir,
                          "%03d_%s.bin" % (self.dump_seq, tag))
        try:
            with open(fn, "wb") as f:
                f.write(bytes(data))
        except OSError as e:
            self.log("dump write failed %s: %r" % (fn, e))
            return
        self.log("DUMP #%d %-18s n=%-6d -> %s"
                 % (self.dump_seq, tag, len(data),
                    os.path.basename(fn)))

    @staticmethod
    def _frame_at(p):
        """[u32 P][u32 W] at start; returns (P, W) or None if incomplete
        / invalid."""
        if len(p) < 8:
            return None
        P, W = struct.unpack_from("<II", p, 0)
        if not (1 <= W <= P <= 0x1000000):
            return None
        if len(p) < 8 + W:
            return None
        return P, W

    @staticmethod
    def _try_lzss(body, P):
        # identity only when sizes match exactly (server stores small
        # payloads uncompressed with comp_size == body_size); P > len
        # means LZSS; P < len is malformed -> decompress fails -> None
        if P == len(body):
            return None
        try:
            out = lzss.decompress(body, P)
            return out
        except Exception:
            return None

    def _stash(self, data, cmd=None):
        self.frames.append((cmd, data))
        # best-effort symbol table: 0x790-step records, name at offset 0
        step = 0x790
        if len(data) >= step and len(data) % step == 0:
            for i in range(len(data) // step):
                nm = data[i * step:i * step + 12].split(b"\x00")[0]
                try:
                    nm_s = nm.decode("latin-1")
                except Exception:
                    continue
                self.sym_names[i] = nm_s
                # ConSymbol +0x68 = u32 digits (probe-proven: ag=0,
                # A-share names like 000001=2)
                dig = struct.unpack_from("<I", data, i * step + 0x68)[0]
                if 0 <= dig <= 12:
                    self.sym_digits[nm_s] = dig
            if self.sym_names:
                self.log("symbol table: %d names (step 0x%x)"
                         % (len(self.sym_names), step))
        if cmd == 0x1E and len(data) >= USER_BLOCK:
            self._parse_user(data)
        elif cmd == 0x22 and len(data) % TRADE_REC == 0:
            self.history = [self._parse_trade(data, i * TRADE_REC)
                            for i in range(len(data) // TRADE_REC)]
            self.log("history: %d records" % len(self.history))

    # ---------------- M2 structured parsing ----------------
    @staticmethod
    def _cstr(data, off, size):
        return data[off:off + size].split(b"\x00")[0].decode(
            "latin-1", "replace")

    @classmethod
    def _parse_trade(cls, data, base):
        u32 = lambda o: struct.unpack_from("<I", data, base + o)[0]
        f64 = lambda o: struct.unpack_from("<d", data, base + o)[0]
        cmd = u32(0x18)
        px_28 = f64(0x28)
        px_78 = f64(0x78)
        # wire quirk (M8 live-proven, ag pending #10065): for a
        # pending order (cmd 2..5) the ORDER PRICE lives at f64@0x28
        # (the position's "current close" slot) while f64@0x78 carries
        # the live market quote; for positions it is the other way
        # around (@0x78 = open price, @0x28 = live close).
        pending = cmd in (OP_BUY_LIMIT, OP_SELL_LIMIT,
                          OP_BUY_STOP, OP_SELL_STOP)
        return {
            "ticket": u32(0),
            "login": u32(4),
            "symbol": cls._cstr(data, base + 8, 12),
            "cmd": cmd,                # 0=buy 1=sell 2..5 pending 6=bal
            # +0x1C is the VOLUME in 0.01-lot points (probe-proven:
            # a 0.5-lot ag position carries 50 here; the old "rate%"
            # reading was a 1.0-lot-only coincidence).  The close
            # guard compares the request volume against this field.
            "volume": u32(0x1C),
            "lots": u32(0x1C) / 100.0,
            "open_time": u32(0x20),
            "cur_close": px_28,        # position: live close; pending:
                                       # the requested order price
            "sl": f64(0x30),
            "tp": f64(0x38),
            "close_time": u32(0x40),
            "lots_raw": f64(0x50),     # min-lot-clamped, NOT truth
            "open_price": px_78,       # position: open px; pending:
                                       # live market quote
            # Unified price: pending order price or position open price.
            "price": px_28 if pending else px_78,
            "profit": f64(0x80),       # balance ops: deposit amount
            "comment": cls._cstr(data, base + 0x94, 0x2C),
            "margin_rate": f64(0xC0),
            "mod_time": u32(0xC8),
        }

    def _parse_user(self, data):
        self.account = {
            "login": self.login,
            "name": self._cstr(data, 4, 0x40),
            "leverage": struct.unpack_from("<I", data, 0x44)[0],
            "balance": struct.unpack_from("<d", data, 0x48)[0],
            "group": self._cstr(data, 0xF4, 16),
        }
        tail = data[USER_BLOCK:]
        n = len(tail) // TRADE_REC
        if len(tail) % TRADE_REC:
            self.log("WARN 0x1e tail %d bytes not multiple of 0x%x"
                     % (len(tail), TRADE_REC))
        recs = [self._parse_trade(data, USER_BLOCK + i * TRADE_REC)
                for i in range(n)]
        # 0x1e tail carries BOTH open positions and live pending orders.
        self.positions = [r for r in recs if r["cmd"]
                          not in (OP_BUY_LIMIT, OP_SELL_LIMIT,
                                  OP_BUY_STOP, OP_SELL_STOP)]
        self.orders = [r for r in recs if r["cmd"]
                       in (OP_BUY_LIMIT, OP_SELL_LIMIT,
                           OP_BUY_STOP, OP_SELL_STOP)]
        self.log("ACCOUNT %s login=%d group=%r leverage=1:%d "
                 "balance=%.2f positions=%d pending=%d"
                 % (self.account["name"], self.account["login"],
                    self.account["group"], self.account["leverage"],
                    self.account["balance"], len(self.positions),
                    len(self.orders)))
        for p in self.positions:
            self.log("  POS #%d %s %s lots=%.2f open=%.5f cur=%.5f "
                     "profit=%.2f sl=%.5f tp=%.5f"
                     % (p["ticket"], p["symbol"],
                        "buy" if p["cmd"] == 0 else "sell",
                        p["lots"], p["open_price"], p["cur_close"],
                        p["profit"], p["sl"], p["tp"]))

    # ---------------- queries (M2 API) ----------------
    def get_account(self):
        """Account dict + computed equity."""
        if not self.account:
            return None
        acc = dict(self.account)
        acc["equity"] = acc["balance"] + sum(
            p["profit"] for p in self.positions)
        return acc

    def get_positions(self):
        return list(self.positions)

    def get_orders(self):
        """Live pending orders (cmd 2..5, not yet triggered)."""
        return list(self.orders)

    def get_history(self):
        return list(self.history)

    # ---------------- chart bars (0x9c) ----------------
    def _ingest_bars(self, name, period, win_start, raw):
        """Upsert one downstream 0x9c frame into self.bars.
        Called on the pump thread; signals any request_bars waiter.
        Returns number of non-padding bars ingested."""
        scale = 10.0 ** self.sym_digits.get(name, 0)
        store = self.bars.setdefault((name, period), {})
        n = 0
        events = []
        for i in range(len(raw) // BAR_REC):
            r = raw[i * BAR_REC:(i + 1) * BAR_REC]
            t = struct.unpack_from("<I", r, 0)[0]
            if t == 0:                    # live placeholder padding
                continue
            op = struct.unpack_from("<I", r, 4)[0]
            hd, ld, cd = struct.unpack_from("<iii", r, 8)
            vol = struct.unpack_from("<I", r, 20)[0]
            bar = {"time": t,
                   "open": op / scale,
                   "high": (op + hd) / scale,
                   "low": (op + ld) / scale,
                   "close": (op + cd) / scale,
                   "volume": vol}
            # sanity gate: the server occasionally hands out garbage
            # slots (wild epoch, non-positive / crossed prices) that
            # would poison chart auto-scaling
            if (t < 946684800 or t > time.time() + 604800
                    or bar["open"] <= 0 or bar["high"] <= 0
                    or bar["low"] < 0 or bar["high"] < bar["low"]):
                continue
            is_new = t not in store
            store[t] = bar
            events.append((bar, is_new))
            n += 1
        w = self._bar_waiters.get((name, period))
        if w is not None:
            for e in tuple(w["events"]):
                e.set()
        elif events:
            # No outstanding request_bars for this key -> this is a LIVE
            # push (flags=0 subscription).  Bulk history answers arrive
            # while a waiter is registered and must not flood listeners.
            self._emit_bars(name, period, events)
        return n

    def request_bars(self, symbol, period, timeout=12.0,
                     first_wait=8.0, idle=1.5):
        """Send a 0x9c chart request and wait for the answer frames.

        Socket I/O stays on the pump thread: this only sends and blocks
        on an Event that _ingest_bars signals.  The server answers with
        up to two 512-bar frames (newest first) plus a live placeholder;
        the FIRST frame can take >1s to arrive, then subsequent frames
        follow back-to-back - wait long for the first one and only use
        the short `idle` window to detect completion after that.
        Returns all cached bars for the key, sorted by time.
        """
        name = symbol[:12]
        key = (name, int(period))
        ev = threading.Event()
        with self._bar_send_lock:
            w = self._bar_waiters.get(key)
            if w is None:
                # Owner: register and put exactly ONE request on the wire.
                self._bar_waiters[key] = {"events": {ev}}
                owner = True
                rec = (name.encode("latin-1").ljust(12, b"\x00")
                       + struct.pack("<IIH", int(period), 0, 0))
                self.send_enc(0x9C, struct.pack("<H", 1) + rec)
                self.log("BARS request sent sym=%s per=%d" % (name, period))
            else:
                # Coalesce: an identical request is in flight (seeding vs.
                # bar_refresher race).  Piggyback on its answer instead of
                # overwriting the waiter, which used to strand the owner.
                w["events"].add(ev)
                owner = False
        try:
            t_end = time.time() + timeout
            if owner:
                if ev.wait(min(first_wait, timeout)):
                    ev.clear()
                    while time.time() < t_end:
                        remaining = t_end - time.time()
                        if not ev.wait(min(idle, remaining)):
                            break        # idle window: transfer done
                        ev.clear()
            elif ev.wait(min(first_wait, timeout)):
                # First shared frame arrived; let back-to-back frames land.
                time.sleep(min(idle, max(0.0, t_end - time.time())))
        finally:
            with self._bar_send_lock:
                w2 = self._bar_waiters.get(key)
                if w2 is not None:
                    w2["events"].discard(ev)
                    if owner:
                        self._bar_waiters.pop(key, None)
        bars = self.get_bars(name, period)
        self.log("BARS request done sym=%s per=%d got=%d owner=%s"
                 % (name, period, len(bars), owner))
        return bars

    def get_bars(self, symbol, period, from_ts=0, limit=0):
        """Cached bars for (symbol, period), oldest first; optional
        lower time bound and tail limit (0 = unlimited)."""
        store = self.bars.get((symbol, int(period)), {})
        out = [store[t] for t in sorted(store) if t >= from_ts]
        return out[-limit:] if limit else out

    # ---------------- M6: push listeners ----------------
    def on_tick(self, fn):
        """Register fn(tick_dict) on the pump thread; returns qid."""
        with self._listeners_lock:
            self._listener_seq += 1
            qid = self._listener_seq
            self._tick_listeners[qid] = fn
        return qid

    def on_bar(self, fn):
        """Register fn(name, period, bar, is_new) on the pump thread;
        returns qid.  Only LIVE frames fire it - the bulk history answer
        of request_bars is suppressed while its waiter is active."""
        with self._listeners_lock:
            self._listener_seq += 1
            qid = self._listener_seq
            self._bar_listeners[qid] = fn
        return qid

    def remove_listener(self, qid):
        with self._listeners_lock:
            self._tick_listeners.pop(qid, None)
            self._bar_listeners.pop(qid, None)

    def _emit_tick(self, tick):
        with self._listeners_lock:
            fns = list(self._tick_listeners.values())
        for fn in fns:
            try:
                fn(tick)
            except Exception as e:
                self.log("tick listener error: %r" % e)

    def _emit_bars(self, name, period, events):
        with self._listeners_lock:
            fns = list(self._bar_listeners.values())
        for bar, is_new in events:
            for fn in fns:
                try:
                    fn(name, period, bar, is_new)
                except Exception as e:
                    self.log("bar listener error: %r" % e)

    # ---- downstream 0xbe: [u32 x][u8 sub][u8 kind] + payload ----
    def _step_push_be(self, p):
        if len(p) < 7:
            return False
        x, sub, kind = struct.unpack_from("<IBB", p, 1)
        if kind != 0:
            self.log("DOWN 0xbe x=0x%08x sub=0x%02x kind=0x%02x (status)"
                     % (x, sub, kind))
            self.trade_events.append((time.time(), "be", x, sub, kind))
            self._dump("downbe_s%02x" % kind, p[:7])
            del p[:7]
            return True
        hdr = p[:7]
        if sub == 0:                        # 0x10 raw prices challenge
            if len(p) < 7 + 0x10:
                return False
            raw = bytes(p[7:7 + 0x10])
            f32 = struct.unpack("<4f", raw)
            f64 = struct.unpack("<2d", raw)
            self.log("DOWN 0xbe x=0x%08x sub=0 kind=0 PRICES "
                     "f32=%s f64=%s" % (x, f32, f64))
            self.trade_events.append((time.time(), "be_prices", x, raw))
            self._dump("downbe_prices", hdr + raw)
            del p[:7 + 0x10]
            return True
        if 0x40 <= sub <= 0x43:             # one 0xE0 record in a frame
            vf = self._frame_at(p[7:])
            if vf is None:
                return False
            P, W = vf
            body = bytes(p[7 + 8:7 + 8 + W])
            plain = self._try_lzss(body, P) if P > len(body) else body
            self.log("DOWN 0xbe x=0x%08x sub=0x%02x kind=0 RECORD P=%d "
                     "W=%d n=%d" % (x, sub, P, W,
                                    len(plain or body) // TRADE_REC))
            self._dump("downbe_rec%02x" % sub, hdr + (plain or body))
            del p[:7 + 8 + W]
            data = plain or body
            if len(data) % TRADE_REC == 0 and len(data) >= TRADE_REC:
                for i in range(len(data) // TRADE_REC):
                    self._upsert_trade(self._parse_trade(data,
                                                         i * TRADE_REC))
            return True
        if 0x44 <= sub <= 0x46:             # 8B + 8B + frame of records
            if len(p) < 7 + 16:
                return False
            vf = self._frame_at(p[7 + 16:])
            if vf is None:
                return False
            P, W = vf
            body = bytes(p[7 + 16 + 8:7 + 16 + 8 + W])
            plain = self._try_lzss(body, P) if P > len(body) else body
            extra = bytes(p[7:7 + 16])
            self.log("DOWN 0xbe x=0x%08x sub=0x%02x kind=0 REC44 "
                     "e=%s P=%d W=%d n=%d"
                     % (x, sub, extra.hex(), P, W,
                        len(plain or body) // TRADE_REC))
            self._dump("downbe_rec%02x" % sub, hdr + extra + (plain or body))
            del p[:7 + 16 + 8 + W]
            data = plain or body
            if len(data) % TRADE_REC == 0:
                for i in range(len(data) // TRADE_REC):
                    self._upsert_trade(self._parse_trade(data,
                                                         i * TRADE_REC))
            return True
        self.log("DOWN 0xbe x=0x%08x sub=0x%02x kind=0 UNKNOWN: %s"
                 % (x, sub, hexs(p[7:], 32)))
        self._dump("downbe_unk%02x" % sub, p[:64])
        del p[:7]
        return True

    def _upsert_trade(self, rec):
        """Classify by cmd + close_time:
          cmd 2..5 + close_time==0 -> live pending order (self.orders)
          cmd 0/1  + close_time==0 -> open position (self.positions)
          close_time!=0            -> history (closed pos, or a pending
                                     that got canceled/expired/filled)"""
        pend = rec["cmd"] in (OP_BUY_LIMIT, OP_SELL_LIMIT,
                              OP_BUY_STOP, OP_SELL_STOP)
        if rec["close_time"]:
            self.positions = [q for q in self.positions
                              if q["ticket"] != rec["ticket"]]
            self.orders = [q for q in self.orders
                           if q["ticket"] != rec["ticket"]]
            if not any(h["ticket"] == rec["ticket"]
                       for h in self.history):
                self.history.append(rec)
            act = "CLOSED/REMOVED"
        elif pend:
            self.orders = [q for q in self.orders
                           if q["ticket"] != rec["ticket"]]
            self.orders.append(rec)
            act = "PENDING"
        else:
            self.positions = [q for q in self.positions
                              if q["ticket"] != rec["ticket"]]
            self.orders = [q for q in self.orders
                           if q["ticket"] != rec["ticket"]]
            self.positions.append(rec)
            act = "OPENED"
        side = {0: "buy", 1: "sell", 2: "buy limit", 3: "sell limit",
                4: "buy stop", 5: "sell stop"}.get(rec["cmd"], "?")
        self.log("  %s #%d %s %s lots=%.2f open=%.5f cur=%.5f "
                 "profit=%.2f comment=%r"
                 % (act, rec["ticket"], rec["symbol"], side,
                    rec["lots"], rec["open_price"], rec["cur_close"],
                    rec["profit"], rec["comment"]))

    # ---------------- M7: 0x98 news/mail push ----------------
    def _handle_news(self, d, body_size=-1, comp=b""):
        """Parse a 0x98 news record (reference-client receive side,
        authoritative wire layout; body may be LZSS-compressed when
        body_size > 0 and comp is non-empty):
          +0x00 u32 id      +0x04 u32 time   +0x08 char[32] category
          +0x28 u16 n1 (2..255 incl NUL) -> subject[n1-1]
          +0x2A+(n1-1) u8  language
          +0x2B+(n1-1) u16 n2 (0..63)    -> keywords[n2]
          +...         u16 n3 (0..255)   -> extra[n3]
          +...         i32 body_size     <1=none, 1..0x8000 ->
          +...         i32 comp_size     [LZSS(comp_size)->body(body_size)]"""
        def _cstr(b):
            return b.split(b"\x00")[0].decode("latin-1", "replace")
        nid, ts = struct.unpack_from("<II", d, 0)
        category = _cstr(d[8:0x28])
        n1 = struct.unpack_from("<H", d, 0x28)[0]
        off = 0x2A
        subject = _cstr(d[off:off + max(n1 - 1, 0)]); off += n1 - 1
        lang = d[off]; off += 1
        n2 = struct.unpack_from("<H", d, off)[0]; off += 2
        keywords = d[off:off + n2].decode("latin-1", "replace"); off += n2
        n3 = struct.unpack_from("<H", d, off)[0]; off += 2
        extra = d[off:off + n3].decode("latin-1", "replace"); off += n3
        body = ""
        if body_size > 0 and comp:
            plain = self._try_lzss(comp, body_size)
            body = (plain or comp).decode("latin-1", "replace")
        rec = {"id": nid, "time": ts, "category": category,
               "subject": subject, "language": lang,
               "keywords": keywords, "extra": extra, "body": body}
        self.news.append(rec)
        self.log("NEWS %s [%s] %s (%dB body)"
                 % (time.strftime("%H:%M:%S", time.localtime(ts)),
                    category, subject, body_size if body_size > 0 else 0))

    def get_news(self):
        return list(self.news)

    # ---------------- M8: 0x9b trade notifications ----------------
    def _handle_trade_notify(self, data):
        """0x9b = trade change broadcast, 0x30 header + one full 0xE0
        TradeRecord (0x110=272B; live-captured on ag):
          @0x00 u32 seq with 0x80 marker (server monotonic counter)
          @0x04 u32 action: 0 add, 2 update, 1 delete
          @0x08 char[16] server name, misc u32s up to 0x30
          @0x30        standard 0xE0 client record, same layout as the
                       0x1e/0xbe records -> reuse _parse_trade verbatim
        A delete of a pending/closed record carries close_time!=0, so
        _upsert_trade routes it to history; action==1 with close_time 0
        is still removed unconditionally."""
        RB, REC = 0x30, 0xE0
        if len(data) < RB + 0x18:
            return
        action = struct.unpack_from("<I", data, 4)[0]
        ticket = struct.unpack_from("<I", data, RB)[0]
        if ticket < 1:
            return
        seq = struct.unpack_from("<I", data, 0)[0]
        if len(data) >= RB + REC:
            rec = self._parse_trade(data, RB)
        else:
            rec = None
        self.log("  9b notify seq=0x%02x action=%d #%d"
                 % (seq, action, ticket))
        if action == 1:
            self.orders = [o for o in self.orders
                           if o["ticket"] != ticket]
            self.positions = [q for q in self.positions
                              if q["ticket"] != ticket]
            if rec is not None and rec["close_time"]:
                self._upsert_trade(rec)     # -> history with real fields
            return
        if rec is not None:
            self._upsert_trade(rec)

    # ---------------- M3 trade (upstream 0xbe, dedicated conn) ------
    def trade_body(self, ttype, cmd, symbol, lots, price=0.0, sl=0.0,
                   tp=0.0, order=0, comment="", ie_dev=0, expiration=0):
        """0x60 TradeTransInfo + crc; layout byte-verified vs gold
        capture: symbol@0x0C volume@0x18 price@0x1C crc@0x5C.
        +0x08 orderby, +0x34 ie_deviation (slippage, points),
        +0x58 u32 expiration (unix ts; 0=GTC)."""
        b = bytearray(0x60)
        struct.pack_into("<HH", b, 0, ttype, cmd)
        struct.pack_into("<II", b, 4, order, 0)
        b[0x0C:0x18] = symbol.encode("latin-1")[:12].ljust(12, b"\x00")
        struct.pack_into("<I", b, 0x18, int(round(lots * 100)))
        struct.pack_into("<d", b, 0x1C, price)
        struct.pack_into("<d", b, 0x24, sl)
        struct.pack_into("<d", b, 0x2C, tp)
        struct.pack_into("<I", b, 0x34, ie_dev)
        c = comment.encode("latin-1")[:31]
        b[0x38:0x38 + len(c)] = c
        struct.pack_into("<I", b, 0x58, int(expiration))
        crc = (self.login + sum(b[:0x5C])) & 0xFFFFFFFF
        struct.pack_into("<I", b, 0x5C, crc)
        return bytes(b)

    def _side_price(self, symbol, cmd):
        """quote conn live tick: buy -> ask, sell -> bid."""
        idx = next((i for i, n in self.sym_names.items()
                    if n == symbol), None)
        if idx is not None and idx in self.last_tick:
            t, bid, ask = self.last_tick[idx]
            return ask if cmd == OP_BUY else bid
        return 0.0

    def _close_price(self, rec):
        """instant-close price for rec: closing a buy happens at bid,
        closing a sell at ask (opposite of the opening sides)."""
        idx = next((i for i, n in self.sym_names.items()
                    if n == rec["symbol"]), None)
        if idx is not None and idx in self.last_tick:
            t, bid, ask = self.last_tick[idx]
            return bid if rec["cmd"] == OP_BUY else ask
        return rec["cur_close"] or 0.0

    def run_m3(self, symbol="ag", lots=1.0, settle=4.0):
        """M3 live probe: quote conn for context, then a
        dedicated trade connection: open market buy -> close it."""
        self.connect()
        self.handshake((symbol,))
        threading.Thread(target=self.ping_loop, daemon=True).start()

        def pump_for(seconds):
            t_end = time.time() + seconds
            while time.time() < t_end and not self.halt:
                try:
                    self.sock.settimeout(min(1.0, max(0.1, t_end
                                                      - time.time())))
                    self._pump()
                except ConnectionError as e:
                    self.log("link down: %r" % e)
                    return False
            return not self.halt

        pump_for(settle)
        acc = self.get_account()
        if acc:
            self.log("M3 pre-trade balance=%.2f equity=%.2f positions=%d"
                     % (acc["balance"], acc["equity"],
                        len(self.positions)))
        known = {p["ticket"] for p in self.positions}
        known |= {h["ticket"] for h in self.history}

        tc = TradeConn(self)
        tc.connect()

        def request_retry(body_fn, tag, retries=6):
            """Gold conn3 pattern: first 0xbe (price 0) answers 0x88
            off quotes, resend with the live ask/bid fills.  body_fn
            is re-evaluated each attempt to refresh the price."""
            recs, ev = [], []
            for attempt in range(1, retries + 1):
                recs, ev = tc.request(body_fn(), tag=tag + (
                    " r%d" % attempt if attempt > 1 else ""))
                last = ev[-1] if ev else ("?", 0)
                if last[0] == "final":
                    return recs, ev
                if last[0] == "error" and last[1] in (0x87, 0x88, 0x8A):
                    pump_for(0.8)          # refresh quote-conn ticks
                    continue
                break
            return recs, ev

        try:
            # the live server runs ag in MARKET execution: open price
            # MUST be 0 (any explicit price -> status 0x03; 0 -> 0x8e
            # placed -> 0x8f -> 0x00+rec when the symbol session is
            # open, 0x88 off quotes otherwise)
            recs, ev = request_retry(
                lambda: self.trade_body(TT_OPEN, OP_BUY, symbol,
                                        lots, price=0.0),
                "OPEN %s buy %.2f" % (symbol, lots))
            self.log("M3 open: recs=%s ev=%r"
                     % ([(r["ticket"], "buy" if r["cmd"] == 0 else "sell",
                          r["open_price"], r["close_time"]) for r in recs],
                        ev))
            fresh = [r for r in recs
                     if r["ticket"] not in known and not r["close_time"]]
            if not fresh:
                self.log("M3: no new open position confirmed; stop")
                return
            pump_for(1.0)
            # market-execution close: wire 0x46, price exactly 0.0,
            # order@+4 = ticket (server overwrites cmd/symbol/price
            # from its own order record; FUN_0044c320)
            crecs, cev = request_retry(
                lambda: self.trade_body(TT_CLOSE_MKT,
                                        fresh[0]["cmd"],
                                        fresh[0]["symbol"],
                                        fresh[0]["lots"],
                                        price=0.0,
                                        order=fresh[0]["ticket"]),
                "CLOSE #%d mkt" % fresh[0]["ticket"])
            if cev and cev[-1] == ("error", 0x03):
                # instant-execution symbols need 0x44 + a real close
                # price (bid for buy, ask for sell)
                crecs, cev = request_retry(
                    lambda: self.trade_body(TT_CLOSE,
                                            fresh[0]["cmd"],
                                            fresh[0]["symbol"],
                                            fresh[0]["lots"],
                                            price=self._close_price(
                                                fresh[0]),
                                            order=fresh[0]["ticket"]),
                    "CLOSE #%d inst" % fresh[0]["ticket"])
            self.log("M3 close: recs=%s ev=%r"
                     % ([(r["ticket"], r["close_time"], r["profit"])
                         for r in crecs], cev))
        finally:
            tc.close()
        pump_for(2.0)
        self.log("M3 events=%r" % (self.trade_events,))

    def run_closeall(self, settle=4.0):
        """Close every open position.  Market-execution symbols take
        wire 0x46 with price exactly 0.0; on 0x03 fall back to 0x44
        with a live instant-close price."""
        self.connect()
        self.handshake()
        threading.Thread(target=self.ping_loop, daemon=True).start()
        t_end = time.time() + settle
        while time.time() < t_end and not self.halt:
            try:
                self.sock.settimeout(min(1.0, max(0.1, t_end
                                                  - time.time())))
                self._pump()
            except ConnectionError as e:
                self.log("link down: %r" % e)
                return
        poss = self.get_positions()
        if not poss:
            self.log("no open positions")
        else:
            tc = TradeConn(self)
            tc.connect()
            try:
                for p in poss:
                    recs, ev = tc.request(
                        self.trade_body(TT_CLOSE_MKT, p["cmd"],
                                        p["symbol"], p["lots"],
                                        price=0.0, order=p["ticket"]),
                        tag="CLOSE #%d mkt" % p["ticket"])
                    if ev and ev[-1] == ("error", 0x03):
                        recs, ev = tc.request(
                            self.trade_body(TT_CLOSE, p["cmd"],
                                            p["symbol"], p["lots"],
                                            price=self._close_price(p),
                                            order=p["ticket"]),
                            tag="CLOSE #%d inst" % p["ticket"])
            finally:
                tc.close()
        t_end = time.time() + 2.0
        while time.time() < t_end and not self.halt:
            try:
                self.sock.settimeout(0.5)
                self._pump()
            except ConnectionError as e:
                self.log("link down: %r" % e)
                break
        acc = self.get_account()
        if acc:
            self.log("SUMMARY balance=%.2f equity=%.2f positions=%d "
                     "history=%d"
                     % (acc["balance"], acc["equity"],
                        len(self.positions), len(self.history)))
            for h in self.history[-len(poss):]:
                self.log("  HIST #%d %s %s lots=%.2f open=%.5f "
                         "close=%.5f profit=%.2f"
                         % (h["ticket"], h["symbol"],
                            "buy" if h["cmd"] == 0 else "sell",
                            h["lots"], h["open_price"],
                            h["cur_close"], h["profit"]))

    # ---------------- heartbeat ----------------
    def ping_loop(self, period=2.5):
        # Gold capture (conn1 uplink): the reference client keeps the
        # quote link alive with bare 0x02 idle markers (~261/session).  Without
        # them the server stops answering NEW 0x9c chart requests after
        # ~20-30s even though existing tick pushes keep flowing.
        # (0x9d is NOT the quote keepalive - it appears only inside data.)
        while not self.halt:
            time.sleep(period)
            if self.halt:
                return
            try:
                self.send_enc(0x02)
            except Exception as e:
                self.log("keepalive failed: %r" % e)
                return

    def run(self, duration=60, symbols=("ag",)):
        self.connect()
        self.handshake(symbols)
        threading.Thread(target=self.ping_loop, daemon=True).start()
        t_end = time.time() + duration
        while time.time() < t_end and not self.halt:
            try:
                self.sock.settimeout(min(1.0, max(0.1, t_end
                                                  - time.time())))
                self._pump()
            except ConnectionError as e:
                self.log("link down: %r" % e)
                break
        self.log("session end: ticks=%d frames=%d names=%d "
                 "history=%d" % (self.n_ticks, len(self.frames),
                                 len(self.sym_names),
                                 len(self.history)))
        if self.account:
            acc = self.get_account()
            self.log("SUMMARY account=%d name=%r balance=%.2f "
                     "equity=%.2f positions=%d"
                     % (acc["login"], acc["name"], acc["balance"],
                        acc["equity"], len(self.positions)))

    def _pump(self):
        try:
            b = self.sock.recv(65536)
        except socket.timeout:
            if time.time() - self.last_down > 20:
                self.log("WARN: no downlink for 20s")
            return
        if not b:
            raise ConnectionError("closed by server")
        self.last_down = time.time()
        pl = self._advance_decrypt(b)
        self.pending += pl
        self._parse_stream()


class TradeConn:
    """Dedicated trade connection (gold capture conn2/3/4):
    28B login -> status+greeting -> 0xbe requests with 0x02 keepalives
    -> 0x0d graceful logout.  NO init tokens, NO subscriptions here -
    the server's quote-pumping loop rejects 0xbe as intruder."""

    def __init__(self, cli):
        self.cli = cli
        self.sock = None
        self.up = None            # send cipher
        self.dn = None            # recv cipher
        self.pending = bytearray()

    def log(self, msg):
        self.cli.log("TRADE " + msg)

    def connect(self):
        self.sock = socket.create_connection(
            (self.cli.host, self.cli.port), timeout=10)
        self.sock.settimeout(0.5)
        self.sock.sendall(login_packet(self.cli.login, self.cli.table,
                                       flag=1))
        st = self._recv_exact(1)[0]
        if st != 0:
            raise RuntimeError("trade login status=0x%02x (rejected)" % st)
        self.dn = ChannelCipher(self.cli.table)
        g = self.dn.decrypt(self._recv_exact(GREET_LEN))
        self.up = ChannelCipher(self.cli.table)
        self.log("login ok greeting=%s" % g.hex())

    def _recv_exact(self, n):
        buf = bytearray()
        while len(buf) < n:
            b = self.sock.recv(n - len(buf))
            if not b:
                raise ConnectionError("trade conn closed")
            buf += b
        return bytes(buf)

    def is_alive(self):
        """无侵入探测连接是否已被服务端关闭。

        服务端断开后 socket 变为可读且 peek 返回空；不消费任何数据，
        避免破坏解密流。用于发单前主动重连，减少“请求已发送后才发现断线”。
        """
        if self.sock is None:
            return False
        try:
            r, _, _ = select.select([self.sock], [], [], 0)
            if not r:
                return True
            return bool(self.sock.recv(1, socket.MSG_PEEK))
        except (OSError, ValueError):
            return False

    def _send(self, cmd, body=b""):
        self.sock.sendall(self.up.encrypt(bytes([cmd]) + body))

    def _decrypt_more(self, raw):
        cp = ChannelCipher(self.cli.table, self.dn.prev, self.dn.counter)
        self.dn = cp
        self.pending += cp.decrypt(raw)

    def request(self, body, tag=""):
        """Send one 0xbe request; pump replies until final record /
        error / reject.  Returns (records, events)."""
        crc = struct.unpack_from("<I", body, 0x5C)[0]
        self.log("UP 0xbe %s crc=0x%08x price=%r body=%s"
                 % (tag, crc, struct.unpack_from("<d", body, 0x1C)[0],
                    hexs(body, 96)))
        self._send(0xBE, body)
        events, records = [], []
        sent_type = struct.unpack_from("<H", body, 0)[0]
        # Delete (0x48) sends ONLY 0x8e/0x8f progress and no record
        # frame: the server routes the confirmation via its notify
        # chain -> push on the quote conn (0x9b), skipping the direct
        # trade-conn reply.  A short idle window after 0x8f
        # therefore means success; everything else waits 25s.
        deadline = time.time() + (4.0 if sent_type == TT_DELETE else 25.0)
        last_flow = time.time()
        while time.time() < deadline:
            if time.time() - last_flow > 8.0:
                self._send(0x02)          # keepalive pattern
                last_flow = time.time()
            try:
                b = self.sock.recv(4096)
            except socket.timeout:
                continue
            if not b:
                raise ConnectionError("trade conn closed by server")
            self._decrypt_more(b)
            p = self.pending
            while p:
                cmd = p[0]
                if cmd == 0xBE:
                    if len(p) < 7:
                        break
                    x, rsub, kind = struct.unpack_from("<IBB", p, 1)
                    if kind == 0 and rsub == 0:
                        if len(p) < 7 + 0x10:
                            break
                        px = struct.unpack("<4f", bytes(p[7:7 + 0x10]))
                        self.log("DOWN x=%d reqtype=0 kind=0 PRICES "
                                 "(requote) f32=%s" % (x, px))
                        events.append(("prices", px))
                        del p[:7 + 0x10]
                        return records, events + [("final", "prices")]
                    if kind == 0 and 0x44 <= rsub <= 0x46:
                        # close/modify/delete echoes carry a 16B extra
                        # block before the frame header (same layout as
                        # the quote-conn REC44 pushes)
                        if len(p) < 7 + 16 + 8:
                            break
                        vf = AQClient._frame_at(p[7 + 16:])
                        if vf is None:
                            break
                        P, W = vf
                        if len(p) < 7 + 16 + 8 + W:
                            break
                        extra = bytes(p[7:7 + 16])
                        fbody = bytes(p[7 + 16 + 8:7 + 16 + 8 + W])
                        data = None
                        if P > len(fbody):
                            try:
                                data = lzss.decompress(fbody, P)
                            except Exception:
                                data = None
                        data = data or fbody
                        self.log("DOWN x=%d reqtype=0x%02x kind=0 "
                                 "REC44 e=%s P=%d W=%d recs=%d"
                                 % (x, rsub, extra.hex(), P, W,
                                    len(data) // TRADE_REC))
                        for i in range(len(data) // TRADE_REC):
                            records.append(
                                AQClient._parse_trade(data,
                                                       i * TRADE_REC))
                        del p[:7 + 16 + 8 + W]
                        return records, events + [("final", "ok")]
                    if kind == 0:
                        # reply frame starts right after the 7B header
                        # for open echoes (0x40..0x43) or after a 16B
                        # extra block for close/modify/delete echoes
                        # (0x44+) - try both offsets
                        offs = (7,) if rsub < 0x44 else (7 + 16, 7)
                        vf = None
                        foff = None
                        for off in offs:
                            vf = AQClient._frame_at(p[off:])
                            if vf is not None:
                                foff = off
                                break
                        if vf is None:
                            break
                        P, W = vf
                        if len(p) < foff + 8 + W:
                            break
                        fbody = bytes(p[foff + 8:foff + 8 + W])
                        data = None
                        if P > len(fbody):
                            try:
                                data = lzss.decompress(fbody, P)
                            except Exception:
                                data = None
                        data = data or fbody
                        self.log("DOWN x=%d reqtype=0x%02x kind=0 FRAME "
                                 "off=%d P=%d W=%d recs=%d"
                                 % (x, rsub, foff, P, W,
                                    len(data) // TRADE_REC))
                        for i in range(len(data) // TRADE_REC):
                            records.append(
                                AQClient._parse_trade(data,
                                                       i * TRADE_REC))
                        del p[:foff + 8 + W]
                        return records, events + [("final", "ok")]
                    if kind in (0x8E, 0x8F):
                        self.log("DOWN x=%d progress 0x%02x" % (x, kind))
                        events.append(("progress", kind))
                        del p[:7]
                        last_flow = time.time()
                        continue
                    self.log("DOWN x=%d reqtype=0x%02x ERROR 0x%02x (%d: "
                             "%s)" % (x, rsub, kind, kind,
                                      ERR_TEXT.get(kind, "?")))
                    events.append(("error", kind))
                    del p[:7]
                    return records, events
                if cmd == 0xBF:
                    if len(p) < 6:
                        break
                    x, st = struct.unpack_from("<IB", p, 1)
                    self.log("DOWN 0xbf x=%d reject status=0x%02x (%s)"
                             % (x, st, ERR_TEXT.get(st, "?")))
                    events.append(("reject", st))
                    del p[:6]
                    return records, events
                if cmd == 0x02:               # idle/keepalive marker
                    del p[:1]
                    last_flow = time.time()
                    continue
                self.log("DOWN unknown 0x%02x: %s" % (cmd, hexs(p[:24])))
                events.append(("unknown", cmd))
                return records, events
        if (sent_type == TT_DELETE
                and ("progress", 0x8F) in events
                and not any(e[0] in ("error", "reject") for e in events)):
            self.log("DELETE confirmed by progress-only reply "
                     "(record via 0x9b notify)")
            return records, events + [("final", "ok")]
        events.append(("timeout", 0))
        return records, events

    def close(self):
        try:
            if self.up is not None:
                self._send(0x0D)
                time.sleep(0.2)
            if self.sock:
                self.sock.close()
        except OSError:
            pass


ERR_TEXT = {
    0x82: "130 invalid stops",
    0x83: "131 invalid volume",
    0x84: "132 market closed",
    0x85: "133 trade disabled",
    0x86: "134 not enough money",
    0x87: "135 price changed",
    0x88: "136 off quotes",
    0x89: "137 broker busy",
    0x8A: "138 requote",
    0x8B: "139 order locked",
    0x8C: "140 long/short not allowed",
    0x8D: "141 too many requests",
    0x93: "147 expiration denied",
    0x8E: "142 order placed",
    0x8F: "143 order filled/partial",
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="117.72.80.248")
    ap.add_argument("--port", type=int, default=443)
    ap.add_argument("--login", type=int,
                    default=int(os.environ.get("AQ_LOGIN", "0")))
    ap.add_argument("--password",
                    default=os.environ.get("AQ_PASS", ""))
    ap.add_argument("--symbols", default="ag")
    ap.add_argument("--duration", type=float, default=60)
    ap.add_argument("--dump", default="",
                    help="directory to dump every decompressed frame")
    ap.add_argument("--m3", action="store_true",
                    help="M3 live trade probe: open then close 1 lot")
    ap.add_argument("--closeall", action="store_true",
                    help="close every open position and reconcile")
    ap.add_argument("--lots", type=float, default=1.0)
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    if not args.login or not args.password:
        ap.error("credentials required: AQ_LOGIN/AQ_PASS env or CLI")
    c = AQClient(args.host, args.port, args.login, args.password,
                  verbose=args.verbose,
                  dump_dir=args.dump or None)
    try:
        if args.m3:
            c.run_m3(symbol=args.symbols.split(",")[0], lots=args.lots)
        elif args.closeall:
            c.run_closeall()
        else:
            c.run(duration=args.duration,
                  symbols=[s for s in args.symbols.split(",") if s])
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
