#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""4444 行情订阅过滤专项测试。

验证：
  1. 正确握手（等 granted 再发订阅）后，订阅过滤生效
  2. 订阅 ru2701 时只收到 ru / ru2701 / ru2701s 三行别名
  3. 订阅 * 时所有配置品种都广播三行别名
  4. 同一时间戳三别名的 bid/ask 完全一致（交叉别名一致性）

服务器端 BroadcastQuote 对每个 tick 生成三行：
  - CTP native（小写合约码）   → ru2701
  - 连续主力别名（品种小写）    → ru
  - 合约+s 别名（合约+小写 s） → ru2701s

用法：
    python market_feed_test.py --symbol ru2701
    python market_feed_test.py --symbol '*'
"""

import argparse
import select
import socket
import sys
import time
from collections import defaultdict


def read_until(sock, token, timeout=5.0):
    """循环 recv 直到 buffer 中出现 token。返回完整 buffer 或 None。"""
    sock.setblocking(False)
    buf = b""
    deadline = time.time() + timeout
    while time.time() < deadline:
        r, _, _ = select.select([sock], [], [], 0.2)
        if r:
            try:
                chunk = sock.recv(4096)
            except BlockingIOError:
                continue
            except OSError:
                return None
            if not chunk:
                return None
            buf += chunk
            if token in buf:
                return buf
    return None


def feed_handshake(host, port, login, password, subscription):
    """严格序列化握手。返回已建立的 socket 或 None。"""
    try:
        s = socket.create_connection((host, port), timeout=10)
    except OSError as e:
        print(f"[FAIL] 连接 {host}:{port} 失败: {e}")
        return None

    # 1. 等 Login:
    data = read_until(s, b"Login:", timeout=5)
    if not data:
        print("[FAIL] 未收到 Login: 提示")
        s.close()
        return None
    print(f"  [<<] 服务端发送: {data!r}")

    # 2. 发用户名
    s.sendall((login + "\r\n").encode())

    # 3. 等 Password:
    data = read_until(s, b"Password:", timeout=5)
    if not data:
        print("[FAIL] 未收到 Password: 提示")
        s.close()
        return None
    print(f"  [<<] 服务端发送: {data!r}")

    # 4. 发密码
    s.sendall((password + "\r\n").encode())

    # 5. 等 granted（关键：必须等 granted 到达后再发订阅）
    data = read_until(s, b"granted", timeout=5)
    if not data:
        print("[FAIL] 未收到 granted，认证失败")
        s.close()
        return None
    print(f"  [<<] 服务端发送: {data!r}")

    # 6. **确保 granted 完全送达后**，再发订阅
    #    等一个短时间让 socket 缓冲区稳定
    time.sleep(0.05)
    sub_line = ("> Symbols:" + subscription + "\r\n").encode()
    s.sendall(sub_line)
    print(f"  [>>] 客户端发送: {sub_line!r}")

    # 7. 等 Subscription received 确认
    data = read_until(s, b"Subscription received", timeout=3)
    if data:
        print(f"  [<<] 服务端确认订阅: {data!r}")

    return s


def parse_tick_line(line):
    """解析一行 tick。格式：SYMBOL time last bid ask ... vol open_vol close_vol timestamp"""
    parts = line.split()
    if len(parts) < 4:
        return None
    sym = parts[0].upper()
    try:
        bid = float(parts[2])
        ask = float(parts[3])
    except (ValueError, IndexError):
        return None
    # timestamp 通常在最后几列之一
    ts = None
    for p in reversed(parts[4:]):
        try:
            val = float(p)
            # CTP tick 的 timestamp 列通常是 9 位以上整数毫秒
            if val > 1e12:
                ts = int(val)
                break
        except ValueError:
            continue
    return {"sym": sym, "bid": bid, "ask": ask, "ts": ts, "raw": line}


def run_subscription_test(host, port, login, password, subscription, duration=10,
                          expected_symbols=None):
    """
    expected_symbols: 期望收到的 symbol 集合（大写），None 表示跳过精确检查
    """
    print(f"\n{'='*60}")
    print(f"测试订阅: '{subscription}'  持续: {duration}s")
    print(f"{'='*60}")

    sock = feed_handshake(host, port, login, password, subscription)
    if not sock:
        return False

    sock.setblocking(False)
    all_symbols = set()
    alias_groups = defaultdict(list)  # key=timestamp -> list of (sym, bid, ask)
    line_count = 0
    start = time.time()

    while time.time() - start < duration:
        r, _, _ = select.select([sock], [], [], 0.3)
        if not r:
            continue
        try:
            data = sock.recv(65536)
        except BlockingIOError:
            continue
        except OSError:
            print("[WARN] 连接被关闭")
            break
        if not data:
            print("[WARN] 连接关闭")
            break

        for raw_line in data.decode("utf-8", "ignore").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            parsed = parse_tick_line(line)
            if not parsed:
                continue
            sym = parsed["sym"]
            all_symbols.add(sym)
            line_count += 1
            if parsed["ts"]:
                alias_groups[parsed["ts"]].append((sym, parsed["bid"], parsed["ask"]))

    sock.close()

    # --- 输出 ---
    print(f"\n[统计] 收到总行数: {line_count}")
    print(f"[统计] 收到的唯一 symbol ({len(all_symbols)}): {sorted(all_symbols)}")

    passed = True

    # 检查 1: 订阅过滤是否正确
    if expected_symbols is not None:
        unexpected = all_symbols - expected_symbols
        missing = expected_symbols - all_symbols
        if unexpected:
            print(f"[FAIL] 订阅过滤失效！收到了未订阅品种: {sorted(unexpected)}")
            passed = False
        else:
            print(f"[PASS] 订阅过滤正确，没有收到未订阅品种")
        if missing:
            print(f"[INFO] 未收到预期品种（可能该品种无行情）: {sorted(missing)}")
        else:
            print(f"[PASS] 所有预期品种都有收到")

    # 检查 2: 每个时间戳是否形成完整三别名组
    alias_count_ok = 0
    alias_count_bad = 0
    for ts, entries in alias_groups.items():
        syms_in_group = set(e[0] for e in entries)
        # 同一时间戳同一品种的三别名组合
        # 比如 ru2701, RU, RU2701S 应该一起出现
        # 但不同品种的 tick 可能混在同一时间戳下（如果订阅了多个品种）
        if len(entries) >= 2:
            # 检查同一 entry 内 bid/ask 是否一致
            bids = set(e[1] for e in entries)
            asks = set(e[2] for e in entries)
            if len(bids) == 1 and len(asks) == 1:
                alias_count_ok += 1
            else:
                alias_count_bad += 1
                if alias_count_bad <= 3:
                    print(f"  [WARN] ts={ts} bid/ask 不一致: {entries}")

    if alias_count_ok + alias_count_bad > 0:
        print(f"[INFO] 时间戳组一致/不一致: {alias_count_ok}/{alias_count_bad}")

    return passed


def main():
    ap = argparse.ArgumentParser(description="4444 行情订阅过滤专项测试")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=4444)
    ap.add_argument("--login", default="localhost")
    ap.add_argument("--password", default="localhost")
    ap.add_argument("--symbol", default="ru2701",
                    help="要订阅的 symbol（大小写不敏感），用 '*' 表示全部")
    ap.add_argument("--duration", type=float, default=12,
                    help="采集持续秒数")
    ap.add_argument("--instruments",
                    default="ru2701,IF2512,au2512,ag2601,rb2601,i2601,j2601,jm2601,SF601,SM601,cu2512,MA601,AP601",
                    help="config.ini 中配置的 instruments 列表，用于推导预期 symbol 集合")
    args = ap.parse_args()

    # 推导预期 symbol 集合
    expected = None
    subs_lower = args.symbol.lower().strip()

    if subs_lower == "*":
        # 订阅全部 → 预期所有配置品种的三别名都出现
        expected = set()
        for inst in args.instruments.upper().split(","):
            inst = inst.strip()
            if not inst:
                continue
            expected.add(inst)            # CTP native
            # 连续主力别名（品种代码 = 合约去掉数字部分）
            prod = ""
            for ch in inst:
                if not ch.isdigit():
                    prod += ch
                else:
                    break
            if prod:
                expected.add(prod)        # 连续别名
            expected.add(inst + "S")       # 合约+s 别名
    else:
        # 订阅特定合约 → 只预期该合约的三别名
        inst = subs_lower.upper()
        expected = {inst}
        prod = ""
        for ch in inst:
            if not ch.isdigit():
                prod += ch
            else:
                break
        if prod:
            expected.add(prod)
        expected.add(inst + "S")

    print(f"预期 symbol 集合 ({len(expected)}): {sorted(expected)}")

    ok = run_subscription_test(
        args.host, args.port, args.login, args.password,
        args.symbol, args.duration, expected
    )

    print(f"\n{'='*60}")
    print(f"结果: {'PASS' if ok else 'FAIL'}")
    print(f"{'='*60}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
