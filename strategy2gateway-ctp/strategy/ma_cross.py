#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""一分钟双均线策略 demo。

数据来源：SimpleCTPConsole 的 4444 行情端口（文本协议，按 tick 聚合 1 分钟 K 线）。
下单出口：SimpleCTPConsole 的 4445 指令端口（10 列 CSV）。

信号规则：快线上穿慢线 -> 买入开仓；快线下穿慢线 -> 卖出平仓。
默认只做多；--allow-short 打开后反手会先平后开。

用法示例：
    python ma_cross.py                      # 默认 ru2701，5/20 均线
    python ma_cross.py --fast 3 --slow 8 --qty 1
    python ma_cross.py --dry-run            # 只打印信号，不发单
"""

import argparse
import os
import queue
import select
import socket
import sys
import threading
import time

# ---------------------------------------------------------------- 配置读取


def load_ini(path):
    """读取 SimpleCTPConsole 的 config.ini，取行情/指令端口与登录名。"""
    cfg = {}
    if not os.path.isfile(path):
        return cfg
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.split("#", 1)[0].split(";", 1)[0].strip()
            if not line or "=" not in line:
                continue
            k, v = line.split("=", 1)
            cfg[k.strip().lower()] = v.strip()
    return cfg


# ---------------------------------------------------------------- 行情客户端


class FeedClient(threading.Thread):
    """连接 4444，握手、订阅，按 tick 聚合成 bar_seconds 秒的 K 线。"""

    def __init__(self, host, port, login, password, symbol, bar_seconds, on_bar):
        super().__init__(daemon=True)
        self.host = host
        self.port = port
        self.login = login
        self.password = password
        self.symbol = symbol
        self.bar_seconds = bar_seconds
        self.on_bar = on_bar

        self._stop = threading.Event()
        self._sock = None
        self._bar_key = None
        self._bar_open = 0.0
        self._bar_high = 0.0
        self._bar_low = 0.0
        self._bar_close = 0.0
        self._bar_count = 0

    def stop(self):
        self._stop.set()
        if self._sock is not None:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self._sock.close()
            except OSError:
                pass

    def _read_until(self, token, timeout=10.0):
        """读到出现 token 为止（用于横跨 Login:/Password:/granted 三段提示）。"""
        deadline = time.time() + timeout
        buf = b""
        while time.time() < deadline:
            r, _, _ = select.select([self._sock], [], [], 0.5)
            if not r:
                continue
            chunk = self._sock.recv(4096)
            if not chunk:
                return False
            buf += chunk
            if token.encode() in buf:
                return True
        return False

    def run(self):
        try:
            self._sock = socket.create_connection((self.host, self.port), timeout=10)
            self._sock.settimeout(10)

            if not self._read_until("Login:"):
                print("[feed] 未收到登录提示", flush=True)
                return
            self._sock.sendall((self.login + "\r\n").encode())

            if not self._read_until("Password:"):
                print("[feed] 未收到密码提示", flush=True)
                return
            self._sock.sendall((self.password + "\r\n").encode())

            if not self._read_until("granted"):
                print("[feed] 认证失败", flush=True)
                return
            # 订阅：可直接用 CTP 合约名
            self._sock.sendall(("> Symbols:" + self.symbol + "\r\n").encode())
            print(f"[feed] 已订阅 {self.symbol} @ {self.host}:{self.port}", flush=True)

            self._sock.settimeout(5)
            buf = b""
            while not self._stop.is_set():
                try:
                    chunk = self._sock.recv(4096)
                except socket.timeout:
                    continue
                except OSError:
                    break
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    raw, buf = buf.split(b"\n", 1)
                    self._on_line(raw.decode("utf-8", "ignore").strip())
        finally:
            self.stop()
            print("[feed] 行情连接结束", flush=True)

    def _on_line(self, line):
        if not line:
            return
        parts = line.split()
        if len(parts) < 4:
            return  # 订阅确认、提示行等
        sym, ts, bid, ask = parts[0], parts[1], parts[2], parts[3]
        if sym != self.symbol:
            return
        try:
            ts_i = int(float(ts))
            mid = (float(bid) + float(ask)) / 2.0
        except ValueError:
            return
        self._push_tick(ts_i, mid)

    def _push_tick(self, ts, price):
        key = ts // self.bar_seconds
        if self._bar_key is None:
            self._bar_key = key
            self._bar_open = self._bar_high = self._bar_low = self._bar_close = price
            self._bar_count = 1
            return
        if key == self._bar_key:
            self._bar_high = max(self._bar_high, price)
            self._bar_low = min(self._bar_low, price)
            self._bar_close = price
            self._bar_count += 1
            return
        # 跨过 K 线边界：先交付上一根，再开新的一根
        self.on_bar({
            "time": self._bar_key * self.bar_seconds,
            "open": self._bar_open,
            "high": self._bar_high,
            "low": self._bar_low,
            "close": self._bar_close,
            "ticks": self._bar_count,
        })
        self._bar_key = key
        self._bar_open = self._bar_high = self._bar_low = self._bar_close = price
        self._bar_count = 1


# ---------------------------------------------------------------- 下单客户端


class OrderClient:
    """连接 4445，按 10 列 CSV 发指令并等待 OK/ERR 应答。"""

    def __init__(self, host, port, login, group, dry_run=False):
        self.host = host
        self.port = port
        self.login = login
        self.group = group
        self.dry_run = dry_run
        self._sock = None

    def connect(self):
        if self.dry_run:
            return
        self._sock = socket.create_connection((self.host, self.port), timeout=5)
        self._sock.settimeout(5)
        print(f"[order] 已连接指令端口 {self.host}:{self.port}", flush=True)

    def close(self):
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    def send(self, side, qty, symbol, price, order_type, offset, tick, comment):
        line = "{},{},{},{:.4f},{},{},{},{},{},{}".format(
            side, qty, symbol, price, self.login, self.group,
            order_type, offset, tick, comment)
        if self.dry_run:
            print(f"[order][dry-run] {line}", flush=True)
            return "DRYRUN"
        return self._request(line)

    def _request(self, line):
        try:
            self._sock.sendall((line + "\r\n").encode())
        except OSError as e:
            print(f"[order] 发送失败: {e}", flush=True)
            return "ERR send"
        # 并行模式下网关腿是同步的，上游卡顿时单笔应答可达 30s，不能按 8s 判超时
        deadline = time.time() + 35
        while time.time() < deadline:
            r, _, _ = select.select([self._sock], [], [], 0.5)
            if not r:
                continue
            try:
                data = self._sock.recv(4096).decode("utf-8", "ignore")
            except OSError:
                return "ERR recv"
            if not data:
                return "ERR closed"
            for raw in data.splitlines():
                text = raw.strip()
                if not text:
                    continue
                if text.startswith("OK") or text.startswith("ERR"):
                    return text
                # 异步成交通知：只记录
                print(f"[order][push] {text}", flush=True)
        return "ERR timeout"


# ---------------------------------------------------------------- 策略主体


class MaCrossStrategy:
    def __init__(self, feed, order, symbol, fast, slow, qty, tick,
                 allow_short=False):
        self.feed = feed
        self.order = order
        self.symbol = symbol
        self.fast = fast
        self.slow = slow
        self.qty = qty
        self.tick = tick
        self.allow_short = allow_short

        self.bars = queue.Queue()
        self.closes = []
        self.prev_fast = None
        self.prev_slow = None
        self.position = 0   # 1 多，-1 空，0 空仓

    def on_bar(self, bar):
        self.bars.put(bar)

    def run(self):
        print(f"[strategy] 双均线 MA{self.fast}/MA{self.slow}，"
              f"合约 {self.symbol}，手数 {self.qty}", flush=True)
        while True:
            try:
                bar = self.bars.get(timeout=1.0)
            except queue.Empty:
                continue

            self.closes.append(bar["close"])
            if len(self.closes) > self.slow + 1:
                self.closes.pop(0)

            fast = self._sma(self.fast)
            slow = self._sma(self.slow)
            stamp = time.strftime("%H:%M", time.localtime(bar["time"]))
            if fast is None or slow is None:
                print(f"[bar] {stamp} close={bar['close']:.4f} "
                      f"预热 {len(self.closes)}/{self.slow}", flush=True)
                continue

            print(f"[bar] {stamp} close={bar['close']:.4f} "
                  f"MA{self.fast}={fast:.4f} MA{self.slow}={slow:.4f} "
                  f"pos={self.position}", flush=True)

            if self.prev_fast is not None and self.prev_slow is not None:
                crossed_up = self.prev_fast <= self.prev_slow and fast > slow
                crossed_down = self.prev_fast >= self.prev_slow and fast < slow
                if crossed_up:
                    self._golden_cross(bar["close"])
                elif crossed_down:
                    self._death_cross(bar["close"])

            self.prev_fast = fast
            self.prev_slow = slow

    def _sma(self, n):
        if len(self.closes) < n:
            return None
        return sum(self.closes[-n:]) / float(n)

    def _golden_cross(self, price):
        print("[signal] 金叉", flush=True)
        comment = f"MA{self.fast}/{self.slow} up"
        if self.position < 0:
            self._place("BUY", price, "CLOSE", comment)
            self.position = 0
            if not self.allow_short:
                return
        if self.position == 0:
            self._place("BUY", price, "OPEN", comment)
            self.position = 1

    def _death_cross(self, price):
        print("[signal] 死叉", flush=True)
        comment = f"MA{self.fast}/{self.slow} down"
        if self.position > 0:
            self._place("SELL", price, "CLOSE", comment)
            self.position = 0
            if not self.allow_short:
                return
        if self.position == 0 and self.allow_short:
            self._place("SELL", price, "OPEN", comment)
            self.position = -1

    def _place(self, side, price, offset, comment):
        # 对齐最小变动价位：行情中间价常出现 0.5 的尾数，直接用会被 CTP
        # 以“价格不是最小变动价位的整数倍”拒单（ErrorID=48）
        if self.tick > 0:
            price = round(price / self.tick) * self.tick
        # M=市价（CTP 侧按 price±20*tick 报单，网关侧 action=open）；
        # tick 传给控台的 BidAskDif 列
        reply = self.order.send(side, self.qty, self.symbol, price, "M", offset,
                                f"{self.tick:g}", comment)
        print(f"[order] {side} {offset} {self.qty}@{price:.4f} -> {reply}", flush=True)


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    default_ini = os.path.join(here, "..", "config.ini")

    ap = argparse.ArgumentParser(description="一分钟双均线策略 demo（对接 SimpleCTPConsole）")
    ap.add_argument("--ini", default=default_ini, help="SimpleCTPConsole 的 config.ini")
    ap.add_argument("--symbol", default="ru2701", help="CTP 合约代码")
    ap.add_argument("--fast", type=int, default=5, help="快线周期（分钟）")
    ap.add_argument("--slow", type=int, default=20, help="慢线周期（分钟）")
    ap.add_argument("--qty", type=int, default=1, help="每次下单手数")
    ap.add_argument("--tick", type=float, default=5.0, help="最小变动价位（BidAskDif），ru 为 5")
    ap.add_argument("--bar-seconds", type=int, default=60, help="K 线周期（秒），默认 60")
    ap.add_argument("--feed-host", default="127.0.0.1")
    ap.add_argument("--order-host", default="127.0.0.1")
    ap.add_argument("--allow-short", action="store_true", help="允许反手做空")
    ap.add_argument("--dry-run", action="store_true", help="只打印指令不发单")
    args = ap.parse_args()

    cfg = load_ini(args.ini)
    feed_port = int(cfg.get("feedport", 4444))
    order_port = int(cfg.get("order_port", 4445))
    feed_login = cfg.get("feed_login", "localhost")
    feed_password = cfg.get("feed_password", "localhost")
    login_nr = cfg.get("userlogin", "0")

    order = OrderClient(args.order_host, order_port, login_nr, "strategy", args.dry_run)
    try:
        order.connect()
    except OSError as e:
        print(f"[order] 连接 4445 失败: {e}", flush=True)
        return 1

    feed = FeedClient(args.feed_host, feed_port, feed_login, feed_password,
                      args.symbol, args.bar_seconds, None)
    strategy = MaCrossStrategy(feed, order, args.symbol, args.fast, args.slow,
                               args.qty, args.tick, args.allow_short)
    feed.on_bar = strategy.on_bar
    feed.start()

    try:
        strategy.run()
    except KeyboardInterrupt:
        print("\n[strategy] 收到中断，退出", flush=True)
    finally:
        feed.stop()
        order.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
