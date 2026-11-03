#!/usr/bin/env python3
"""Sync gateway auto-reconnect patches to aqntFuturesCn copies."""
import shutil

SRC = r"e:\UniDDEConnector\tdx64_unifeeder\simpleCtpConsole\gateway"
BUILD = r"E:\aqntFuturesCn\build\aqntFutureCn\app"
MYSELF = r"E:\aqntFuturesCn\forMyself\aqntFutureCn\app"

# --- aqclient.py: 两份直接覆盖 ---
for tgt in (BUILD, MYSELF):
    shutil.copy2(SRC + r"\aqclient.py", tgt + r"\aqclient.py")
print("aqclient.py synced to both targets")

# --- aqgw.py: build 直接覆盖 ---
shutil.copy2(SRC + r"\aqgw.py", BUILD + r"\aqgw.py")
print("aqgw.py synced to build (0 diff expected vs source)")

# --- aqgw.py: forMyself = source + 恢复本地默认值 ---
with open(SRC + r"\aqgw.py", "r", encoding="utf-8") as f:
    content = f.read()

# 源默认值是线上 117.72.80.248:443，改成 forMyself 的本地 127.0.0.1:1500
content = content.replace(
    'ap.add_argument("--srv-host", default="117.72.80.248")',
    'ap.add_argument("--srv-host", default="127.0.0.1")'
)
content = content.replace(
    'ap.add_argument("--srv-port", type=int, default=443)',
    'ap.add_argument("--srv-port", type=int, default=1500)'
)

# 确保末尾有且只有一个换行符
content = content.rstrip("\n") + "\n"

with open(MYSELF + r"\aqgw.py", "w", encoding="utf-8", newline="") as f:
    f.write(content)
print("aqgw.py synced to forMyself (2-line diff expected: srv-host + srv-port defaults)")
print("\n=== DONE ===")
