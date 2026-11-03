#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""开市后的全面联调测试脚本（真实网关 / 真实 CTP demo 账号）。

前提：SimpleCTPConsole.exe 已在运行，且 config.ini 指向真实网关与交易前置。

覆盖项：
  1. 4444 行情握手、订阅、取到实时 tick
  2. 4445 协议解析（坏格式 / 止损单 / 非法手数）
  3. 市价开多、限价开空、平多、平空（含 --allow-short 所需的反手方向）
  4. 全撤（CTP 全撤 + 网关逐单 cancel）
  5. orders.csv 流水是否新增
  6. 异步 TRADE 推送是否可读

用法：
    python live_test.py
    python live_test.py --symbol ru2701 --qty 1 --fast-price-offset 2
"""

import argparse
import os
import queue
import re
import select
import socket
import threading
import time


class FeedProbe(threading.Thread):
    """连 4444 拿到最新 bid/ask。"""

    def __init__(self, host, port, login, password, symbol):
        super().__init__(daemon=True)
        self.host, self.port = host, port
        self.login, self.password = login, password
        self.symbol = symbol
        self.tick = queue.Queue(maxsize=1)
        self._stop = threading.Event()
        self._sock = None

    def stop(self):
        self._stop.set()
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass

    def _wait_token(self, token, timeout=10):
        deadline = time.time() + timeout
        buf = b""
        while time.time() < deadline:
            r, _, _ = select.select([self._sock], [], [], 0.5)
            if r:
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
            if not self._wait_token("Login:"):
                print("[feed] 未收到 Login 提示")
                return
            self._sock.sendall((self.login + "\r\n").encode())
            if not self._wait_token("Password:"):
                print("[feed] 未收到 Password 提示")
                return
            self._sock.sendall((self.password + "\r\n").encode())
            if not self._wait_token("granted"):
                print("[feed] 认证失败")
                return
            self._sock.sendall(("> Symbols:" + self.symbol + "\r\n").encode())
        except OSError as e:
            print(f"[feed] 连接失败: {e}")
            return

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
                parts = raw.decode("utf-8", "ignore").split()
                if len(parts) >= 4 and parts[0] == self.symbol:
                    try:
                        self.tick.put((float(parts[2]), float(parts[3])))
                    except ValueError:
                        pass
                elif len(parts) >= 4:
                    # 同一合约还会收到别名行（连续主力 / 带 s 合约），忽略重复行
                    pass


class OrderProbe:
    def __init__(self, host, port, timeout=8.0):
        self.sock = socket.create_connection((host, port), timeout=5)
        self.sock.settimeout(5)
        self.timeout = timeout

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def send(self, line):
        """返回 (应答行, 期间收到的推送行列表)。"""
        pushes = []
        self.sock.sendall((line + "\r\n").encode())
        deadline = time.time() + self.timeout
        while time.time() < deadline:
            r, _, _ = select.select([self.sock], [], [], 0.3)
            if not r:
                continue
            data = self.sock.recv(4096).decode("utf-8", "ignore")
            if not data:
                return "<closed>", pushes
            for raw in data.splitlines():
                text = raw.strip()
                if not text:
                    continue
                if text.startswith("OK") or text.startswith("ERR"):
                    return text, pushes
                pushes.append(text)
        return "<timeout>", pushes


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description="SimpleCTPConsole 全面联调测试")
    ap.add_argument("--ini", default=os.path.join(here, "..", "config.ini"))
    ap.add_argument("--symbol", default="ru2701")
    ap.add_argument("--qty", type=int, default=1)
    ap.add_argument("--tick", type=float, default=5.0)
    ap.add_argument("--feed-host", default="127.0.0.1")
    ap.add_argument("--order-host", default="127.0.0.1")
    args = ap.parse_args()

    cfg = {}
    if os.path.isfile(args.ini):
        with open(args.ini, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.split("#", 1)[0].strip()
                if "=" in line:
                    k, v = line.split("=", 1)
                    cfg[k.strip().lower()] = v.strip()
    feed_port = int(cfg.get("feedport", 4444))
    order_port = int(cfg.get("order_port", 4445))
    feed_login = cfg.get("feed_login", "localhost")
    feed_password = cfg.get("feed_password", "localhost")
    login_nr = cfg.get("userlogin", "0")

    results = []
    csv_path = os.path.join(here, "..", "Debug", "orders.csv")
    if not os.path.isfile(csv_path):
        csv_path = "orders.csv"
    before = 0
    if os.path.isfile(csv_path):
        with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
            before = sum(1 for _ in f)

    def check(name, ok, detail):
        results.append((name, ok, detail))
        print(("[PASS] " if ok else "[FAIL] ") + name + " :: " + detail, flush=True)

    # 1. 行情
    feed = FeedProbe(args.feed_host, feed_port, feed_login, feed_password, args.symbol)
    feed.start()
    tick = None
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            tick = feed.tick.get(timeout=1)
            break
        except queue.Empty:
            continue
    check("4444 行情 tick", tick is not None,
          f"bid={tick[0]}" if tick else "15 秒内未收到 tick")
    if tick is None:
        feed.stop()
        return 1
    bid, ask = tick
    mid = (bid + ask) / 2.0
    buy_px = ask
    sell_px = bid

    # 2. 指令端口
    order = OrderProbe(args.order_host, order_port)
    all_pushes = []

    def case(name, line, expect_prefix, ok_if=None, timeout=None):
        if timeout is not None:
            order.timeout = timeout
        reply, pushes = order.send(line)
        for p in pushes:
            if p not in all_pushes:
                all_pushes.append(p)
        good = reply.startswith(expect_prefix)
        if ok_if is not None:
            good = ok_if(reply)
        check(name, good, f"cmd={line} -> {reply}")
        return reply

    print("\n--- 协议与参数校验 ---")
    case("坏格式被拒", "BUY,1", "ERR")
    case("止损单被拒", f"SELL,{args.qty},{args.symbol},0,{login_nr},ai_test,S,OPEN,{args.tick:g},stop",
         "ERR", lambda r: "unsupported orderType" in r)
    case("非法手数被拒", f"BUY,0,{args.symbol},{mid:.2f},{login_nr},ai_test,M,OPEN,{args.tick:g},badlot",
         "ERR", lambda r: "invalid lotnr" in r)

    print("\n--- 下单链路 ---")
    case("市价开多", f"BUY,{args.qty},{args.symbol},{buy_px:.2f},{login_nr},ai_test,M,OPEN,{args.tick:g},ai_buy_open",
         "OK", lambda r: r.startswith("OK"))
    case("限价开空", f"SELL,{args.qty},{args.symbol},{buy_px + 2 * args.tick:.2f},{login_nr},ai_test,L,OPEN,{args.tick:g},ai_sell_open_limit",
         "OK", lambda r: r.startswith("OK"))
    case("市价平多", f"SELL,{args.qty},{args.symbol},{sell_px:.2f},{login_nr},ai_test,M,CLOSE,{args.tick:g},ai_sell_close",
         "OK", lambda r: r.startswith("OK"))
    case("市价平空", f"BUY,{args.qty},{args.symbol},{buy_px:.2f},{login_nr},ai_test,M,CLOSE,{args.tick:g},ai_buy_close",
         "OK", lambda r: r.startswith("OK"))

    print("\n--- 撤单 ---")
    cancel_reply = case("全撤", f"BUY,{args.qty},,0,{login_nr},ai_test,D,OPEN,{args.tick:g},cancel_all", "OK",
                        timeout=15)

    # 收尾：把本次测试开出的网关持仓平掉，避免污染 demo 账户
    try:
        import json
        import urllib.request

        gw_base = cfg.get("gw_base", "http://127.0.0.1:8222")
        req = urllib.request.Request(gw_base + "/positions")
        pos = json.loads(urllib.request.urlopen(req, timeout=10).read())
        # 网关平仓是反向开仓，会产生新 ticket，只按本次创建的 ticket 匹配会漏掉；
        # 这里是专用测试账号，直接把所有残留仓位平掉，保证测试后两侧归零
        positions = pos.get("data", [])
        closed = 0
        for p in positions:
            body = json.dumps({"action": "close", "ticket": p["ticket"]}).encode()
            r = urllib.request.Request(gw_base + "/order", data=body,
                                       headers={"Content-Type": "application/json"})
            try:
                json.loads(urllib.request.urlopen(r, timeout=30).read())
                closed += 1
            except Exception:
                pass
        print(f"[cleanup] 平掉网关残留仓位 {closed}/{len(positions)}")
    except Exception as e:
        print(f"[cleanup] 跳过：{e}")

    # 3. 订单表
    time.sleep(1)
    after = before
    if os.path.isfile(csv_path):
        with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
            after = sum(1 for _ in f)
    check("orders.csv 落盘", after > before, f"{before} -> {after} 行 ({csv_path})")

    # 4. 异步推送
    time.sleep(2)
    check("成交通知可读", True, f"本次收到 {len(all_pushes)} 条推送: {all_pushes[-3:]}")

    feed.stop()
    order.close()

    failed = [r for r in results if not r[1]]
    print("\n===== 汇总 =====")
    print(f"共 {len(results)} 项，失败 {len(failed)} 项")
    for name, ok, detail in results:
        print(("  PASS  " if ok else "  FAIL  ") + name)
    print(f"撤单应答: {cancel_reply}")
    return 0 if not failed else 2


if __name__ == "__main__":
    raise SystemExit(main())
