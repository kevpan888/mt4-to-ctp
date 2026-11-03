#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""详细的实时行情三别名验证脚本。"""
import socket, time, select, sys

def main():
    host = "127.0.0.1"
    port = 4444
    duration = 20
    subscription = "ru2701"

    s = socket.socket()
    s.settimeout(10)
    s.connect((host, port))
    print(f"Connected to {host}:{port}")

    def wait_token(tok, t=5):
        s.setblocking(False)
        buf = b""
        dl = time.time() + t
        while time.time() < dl:
            r, _, _ = select.select([s], [], [], 0.2)
            if r:
                c = s.recv(65536)
                if not c: return False
                buf += c
                if tok in buf: return True
        return False

    wait_token(b"Login:", 5); s.sendall(b"localhost\r\n")
    wait_token(b"Password:", 5); s.sendall(b"localhost\r\n")
    wait_token(b"granted", 5); time.sleep(0.05)
    s.sendall(("> Symbols:" + subscription + "\r\n").encode())
    time.sleep(0.5)

    symbols = {}
    # 同一时间戳下收集所有 (bid, ask) 对
    ts_groups = {}

    start = time.time()
    while time.time() - start < duration:
        r, _, _ = select.select([s], [], [], 0.3)
        if not r: continue
        try:
            data = s.recv(65536)
        except OSError:
            break
        if not data: break
        for line in data.decode("utf-8", "ignore").splitlines():
            parts = line.split()
            if len(parts) < 4: continue
            sym = parts[0].upper()
            try:
                bid = float(parts[2])
                ask = float(parts[3])
            except ValueError:
                continue
            symbols[sym] = symbols.get(sym, 0) + 1
            # 找时间戳（通常是最后一列，9位以上整数）
            ts = None
            for p in reversed(parts[4:]):
                try:
                    v = float(p)
                    if v > 1e12:
                        ts = int(v); break
                except ValueError:
                    continue
            if ts:
                if ts not in ts_groups:
                    ts_groups[ts] = []
                ts_groups[ts].append((sym, bid, ask))

    s.close()

    print(f"\n=== Symbols received ({len(symbols)} unique) ===")
    for sym, cnt in sorted(symbols.items()):
        print(f"  {sym}: {cnt}")

    # 验证订阅过滤
    if subscription == "*":
        expected_none = []  # 订阅 * 不过滤
    else:
        prod = ""
        for ch in subscription.upper():
            if not ch.isdigit(): prod += ch
            else: break
        expected = {subscription.upper(), prod, subscription.upper() + "S"}
        got = set(symbols.keys())
        extra = got - expected
        missing = expected - got
        if extra:
            print(f"\n[FAIL] 收到未订阅品种: {sorted(extra)}")
        else:
            print(f"\n[PASS] 订阅过滤正确，无未预期品种")
        if missing:
            print(f"[INFO] 未收到（可能无行情）: {sorted(missing)}")

    # 交叉别名一致性检查
    print(f"\n=== Cross-Alias Consistency ===")
    mismatches = 0
    total_checked = 0
    for ts, entries in sorted(ts_groups.items()):
        # 按品种前缀分组：RU2701, RU, RU2701S 都是 RU 前缀
        prefix_map = {}
        for sym, bid, ask in entries:
            # 提取品种前缀（去掉尾部的 S 和数字）
            prefix = sym.rstrip("S")
            while prefix and prefix[-1].isdigit():
                prefix = prefix[:-1]
            if prefix not in prefix_map:
                prefix_map[prefix] = set()
            prefix_map[prefix].add((bid, ask))
        for prefix, bid_ask_set in prefix_map.items():
            if len(bid_ask_set) > 1:
                print(f"  [MISMATCH] ts={ts} prefix={prefix}: {bid_ask_set}")
                mismatches += 1
            total_checked += 1
    if mismatches == 0:
        print(f"  All {total_checked} cross-alias groups have consistent bid/ask")
    else:
        print(f"  {mismatches}/{total_checked} groups have mismatches")

    print(f"\n=== FINAL ===")
    if subscription != "*":
        got = set(symbols.keys())
        prod = ""
        for ch in subscription.upper():
            if not ch.isdigit(): prod += ch
            else: break
        expected = {subscription.upper(), prod, subscription.upper() + "S"}
        if expected.issubset(got) and (got - expected) == set():
            print("PASS - Subscription filter works + all 3 aliases present")
            return 0
        else:
            print("FAIL")
            return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())
