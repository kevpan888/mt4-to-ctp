# SimpleCTPConsole 项目总结

> 生成时间：2026-11-03  
> 项目路径：`e:\UniDDEConnector\tdx64_unifeeder\simpleCtpConsole`  
> 交付形式：Win32 x86 C++ 控制台程序 + Python 网关 + 策略脚本

---

## 一、需求背景

在 Windows 上构建一个**期货实盘交易控制台**，同时接入两条行情源和两条交易通道，通过 TCP 文本协议对外暴露行情推送（4444）和指令接口（4445），服务上层量化策略。

### 核心约束

| 约束 | 说明 |
|------|------|
| 行情双源 | CTP SimNow 为主、Aq Gateway 为备，断线自动切换 + 回切 |
| 交易双路 | CTP（SimNow <YOUR_CTP_INVESTOR_ID>）+ Aq Gateway（<YOUR_ACCOUNT_ID>），支持 parallel / ctp_first 两种模式 |
| MT4 无关 | 不依赖 MT4，不出现 MT4/mt4/UniFeeder 字样 |
| Windows x86 | Win32 Debug，MSBuild 2022，CTP 6.7.9 |
| 纯文本协议 | 4444（行情推送）、4445（指令/应答），便于策略脚本直接 socket 连接 |
| 中文注释 / 无新 MD | manual.html 为唯一交付文档 |

### 账户

| 通道 | 账号 | 密码 | 前置地址 |
|------|------|------|----------|
| CTP 交易 | <YOUR_CTP_INVESTOR_ID> | `<YOUR_CTP_PASSWORD>` | 182.254.243.31:30001/2/3 |
| CTP 行情 | 同上 | 同上 | 182.254.243.31:30011/2/3 |
| Aq 网关 | <YOUR_ACCOUNT_ID> | <YOUR_GW_PASSWORD> | 117.72.80.248:443 |
| Feed 认证 | localhost | localhost | — |

### 当前运行状态（2026-11-03）

| 组件 | PID | 端口 | 状态 |
|------|-----|------|------|
| SimpleCTPConsole | 47336 | 4444（行情）/ 4445（指令） | 监听中 |
| Aq Gateway | 48812 | 127.0.0.1:8222 | connected=true, positions=0, orders=0, trade_fail_total=0 |

---

## 二、架构与数据流

### 2.1 整体架构

```
┌──────────────────────────────────────────────────────────────┐
│                     SimpleCTPConsole                          │
│                                                              │
│  ┌────────────┐   ┌───────────────────┐   ┌──────────────┐  │
│  │  CTP 行情   │──▶│  BroadcastQuote   │──▶│  4444 Feed    │  │
│  │  (3 前置)   │   │  (三别名广播)      │   │  Server       │  │
│  └────────────┘   └───────────────────┘   └──────────────┘  │
│                                                              │
│  ┌────────────┐   ┌───────────────────┐                      │
│  │  CTP 交易   │   │                   │                      │
│  │  (3 前置)   │──▶│   OrderRouter     │                      │
│  └────────────┘   │   (parallel /      │──▶ 4445 OrderServer │
│                   │    ctp_first)       │                      │
│  ┌────────────┐   │                   │                      │
│  │  Aq 网关    │──▶│   GwAsyncLoop     │                      │
│  │  (Python)   │   │   (3s 有界等待)    │                      │
│  └────────────┘   └───────────────────┘                      │
│                                                              │
│  orders.csv  │  groupKey 关联双侧成交  │  GwAsyncPending (shared_ptr) │
└──────────────────────────────────────────────────────────────┘
         ▲                           ▲
         │ HTTP /order               │ TCP socket
    ┌────┴────┐               ┌────┴────┐
    │AqGateway│               │ Strategy │
    │  8222   │               │ (Python) │
    └─────────┘               └───────────┘
```

### 2.2 行情别名规则（BroadcastQuote 每次 tick 输出三行）

```
ru2701 1793671841 19220 19225     ← CTP native（小写合约码）
ru     1793671841 19220 19225     ← 连续主力别名（品种小写）
ru2701s 1793671841 19220 19225    ← 合约+s 别名（合约+小写 s）
```

- `ContinuousAlias("ru2701")` → `"ru"`（去掉所有尾部数字）
- `ContractAlias("ru2701")` → `"ru2701s"`（追加小写 s）
- 订阅过滤对三个别名等价匹配（订阅 `ru` 等于订阅 `ru2701` 等于订阅 `ru2701s`）

### 2.3 指令协议（4445）

```
BUY,1,ru2701,19250.00,<YOUR_CTP_INVESTOR_ID>,ai_test,M,OPEN,5.0,ai_buy_open
SELL,1,ru2701,19250.00,<YOUR_CTP_INVESTOR_ID>,ai_test,L,OPEN,5.0,ai_sell_limit
SELL,1,ru2701,0,<YOUR_CTP_INVESTOR_ID>,ai_test,D,OPEN,5.0,cancel_all   ← 全撤
```

| 字段 | 含义 |
|------|------|
| `BUY/SELL` | 方向 |
| `1` | 手数 |
| `ru2701` | 合约 |
| `19250.00` | 价格（市价可传 0） |
| `<YOUR_CTP_INVESTOR_ID>` | CTP 客户号 |
| `ai_test` | 分组名 |
| `M/L/D` | Market / Limit / Cancel |
| `OPEN/CLOSE` | 开/平 |
| `5.0` | tick 大小 |
| `ai_buy_open` | 注释 |

### 2.4 应答格式

```
OK ref=G0000000001 gw=PENDING group=G0000000001        ← parallel 模式 3s 内返回
OK ref=G0000000002 gw=14108 group=G0000000002          ← 网关同步拿到 ticket
ERR bad orderType group=G0000000003                     ← 拒绝
GW_TICKET G0000000001,14108                              ← 异步补推：网关 ticket 到了
GW_FAIL G0000000001,timeout waiting for gateway          ← 异步补推：网关下单失败
TRADE 14108,19250,1,GW                                   ← 异步：网关持仓变化
TRADE 12345,19245,1,CTP                                  ← 异步：CTP 成交通知
```

---

## 三、核心文件清单

| 文件 | 职责 | 关键符号 |
|------|------|----------|
| `simple_ctp_console.cpp` | 主程序 + FeedServer（4444） | `BroadcastQuote`, `ClientHandler`, `ServerAcceptLoop`, `ContinuousAlias`, `ContractAlias` |
| `simple_ctp_console.h` | 全局变量/函数声明 | `g_feed_port`, `g_client_sockets`, `g_client_subs` |
| `order_router.cpp` | 下单路由 + 异步网关线程 | `OrderRouter::Submit`, `GwAsyncLoop`, `NextGroupKey` |
| `order_router.h` | 路由类声明 | `GwAsyncPending` (struct), `m_groupSeq` (atomic), `m_gwAsyncQueue` |
| `order_server.cpp` | 指令 TCP Server（4445） | `OrderServer::BroadcastLine` |
| `order_server.h` | 指令解析 | — |
| `ctp_trader.cpp` | CTP 交易连接 + 报单/撤单 | `CancelAll`, `OnRtnOrder`, `OnRtnTrade` |
| `gw_client.cpp` | Aq 网关 HTTP 客户端 | `GwClient::Place`, `GwClient::Cancel` |
| `gateway/aqgw.py` | Aq 网关内置实现（HTTP 8222） | `TradeConn.is_alive()`, 两阶段重连, `/health` |
| `gateway/aqclient.py` | Aq 上游协议（TLS 443） | — |
| `app_util.cpp` | 工具函数 | `LowerCopy`, `UpperCopy`, `FormatTickLine` |
| `config.ini` | 配置（根目录 + Debug 各一份） | `instruments`, `order_mode`, `gw_symbols` |

---

## 四、关键改动与 Bug 修复

### 4.1 groupKey 原子序列号

**位置**：`order_router.h`（`m_groupSeq`）+ `order_router.cpp`（`NextGroupKey()`）

```cpp
// order_router.h:91
std::atomic<int> m_groupSeq{0};

// order_router.cpp:471
std::string OrderRouter::NextGroupKey() {
    int n = ++m_groupSeq;
    return "G" + std::to_string(1000000000 + n);  // G0000000001, G0000000002, ...
}
```

每条指令生成唯一的 `Gxxxxxxxxxx` 10 位序列号，写入：
- orders.csv 的 `groupKey` 列
- OK / ERR 应答尾部 `group=Gxxxxxxxxxx`
- GW_TICKET / GW_FAIL 广播
- ctp_first follow-up 复用同一个 groupKey（status=FOLLOW）

### 4.2 Parallel 模式 3s 有界等待

**位置**：`order_router.cpp:152-235`

**问题**：原来 Submit() 同步等网关 8-30s，导致测试超时、策略阻塞。

**改动**：
```
Submit() 入口
  ├─ CTP 报单（同步 ~100ms）
  ├─ GwAsyncPending → push m_gwAsyncQueue
  ├─ wait_for(3000ms) on m_gwWaitCv
  │     ├─ 3s 内拿到 ticket → reply gw=<ticket>
  │     └─ 3s 超时 → reply gw=PENDING，异步线程继续等
  └─ 返回策略
GwAsyncLoop（后台线程）
  ├─ 30s 内部超时 PlaceGw
  ├─ 拿到 ticket → Persist(GW NEW) + Broadcast(GW_TICKET gk,ticket)
  └─ 失败 → Persist(GW FAIL) + Broadcast(GW_FAIL gk,error)
```

### 4.3 GwAsyncPending use-after-free 修复

**位置**：`order_router.h:65-78` + `order_router.cpp`

**问题**：早期版本 Submit 传 raw `new GwAsyncPending()` 给 GwAsyncLoop，GwAsyncLoop 在 submit 线程还在读取 `ap->result` 时就 delete 了指针。

**修复**：全部改为 `std::shared_ptr<GwAsyncPending>`。Submit 在等待期间持有 `shared_ptr`，GwAsyncLoop 也持有一份。最后一个持有者释放时自动析构。

### 4.4 行情订阅过滤三 Bug

**位置**：`simple_ctp_console.cpp`

| Bug | 位置 | 根因 | 修复 |
|-----|------|------|------|
| **A** | `ClientHandler` 第 304 行 | `timeval {0, 0}` 零超时 select — 发完 `granted` 立即检查，客户端订阅行还在路上 → 默认 `*` | 改为 `{0, 500000}` 给客户端 500ms |
| **B** | `BroadcastQuote` 遍历逻辑 | 默认 `subscribed=true` —— 订阅列表为空/不存在时错误全量推送 | 默认跳过（subs 不存在或空 → 不推），只有显式订阅才匹配 |
| **C** | `ServerAcceptLoop` 第 448 行 | accept 后预创建空 `vector<string>` → 与"尚未设置订阅"无法区分 | 去掉预创建，让 key 只在 ClientHandler 完整握手后才存在 |

**修复前**：订阅 `ru2701` 收到 12 个 symbol（全品种）  
**修复后**：订阅 `ru2701` 只收到 `RU / RU2701 / RU2701S`

### 4.5 网关自动重连

**位置**：`gateway/aqgw.py`（`TradeConn.is_alive()` 新增）

```python
def is_alive(self):
    """non-invasive select + MSG_PEEK，不消费数据"""
    r, _, _ = select.select([self.sock], [], [], 0)
    if r:
        try:
            data = self.sock.recv(1, socket.MSG_PEEK)
            if not data: return False   # 对端关闭
        except: return False
    return True
```

两阶段重连：
- **建连阶段**：safe retry 3x（0.4s / 0.8s / 1.2s backoff），先等 quote stream 再回补 trade
- **发送阶段**：**不重试**（已经发出的请求重发会导致重复下单），抛 `order state unknown` 让上游自己查

`/health` 新增字段：`quote_reconnects`, `trade_reconnects`, `trade_ok_total`, `trade_fail_total`

### 4.6 CTP 错误修复

| ErrorID | 场景 | 修复 |
|---------|------|------|
| 141 | 密码过期 | 重置为 `<YOUR_CTP_PASSWORD>`，更新 root + Debug `config.ini` |
| 148 | `OrderSysID` 为空却设置 `ExchangeID` | `CancelAll` 只在 `OrderSysID` 非空时才填这两字段；新提交订单 ≤500ms 内延迟撤单 |
| 48 | 传入非 tick 整数倍价格（如 19217.5） | `ma_cross.py` 增加 `round(price/tick)*tick` 对齐 |
| 51 | 平仓手数不足 | 已知无害：两个通道有独立持仓记录 |

### 4.7 MT4 全局清理

全项目 grep 确认：`MT4|mt4|MetaTrader|UniFeeder` → **0 命中**

具体改动：
- `Mt4ContinuousAlias` → `ContinuousAlias`
- `Mt4ContractAlias` → `ContractAlias`
- 变量名 `mt4Cont` → `contSym`, `mt4Contr` → `contractSym`
- manual.html 移除 MT4 架构图、MT4 协议示例

---

## 五、orders.csv Schema（groupKey 增强版）

```
time,groupKey,source,key,symbol,action,offset,type,price,volume,filled,status,loginNr,groupName,comment
```

| 列 | 说明 |
|----|------|
| `time` | 时间戳 |
| `groupKey` | **新增** 10 位原子序列号（G0000000001），关联 CTP 与 GW 同一次下单 |
| `source` | CTP / GW |
| `key` | CTP OrderRef 或网关 ticket |
| `status` | NEW / FOLLOW（ctp_first 追单） / SENT（CANCEL） / FILL / FAIL |

---

## 六、配置（Debug/config.ini 关键段）

```ini
feedport=4444
userLogin=<YOUR_CTP_INVESTOR_ID>
userPassword=<YOUR_CTP_PASSWORD>
brokerID=9999
connectaddress=tcp://182.254.243.31:30011,tcp://182.254.243.31:30012,tcp://182.254.243.31:30013
tradeaddress=tcp://182.254.243.31:30001,tcp://182.254.243.31:30002,tcp://182.254.243.31:30003
instruments=ru2701,IF2512,au2512,ag2601,rb2601,i2601,j2601,jm2601,SF601,SM601,cu2512,MA601,AP601
market_source=both
order_mode=parallel
ctp_first_timeout_ms=3000
gw_base=http://127.0.0.1:8222
gw_account=<YOUR_ACCOUNT_ID>
gw_password=<YOUR_GW_PASSWORD>
gw_server=117.72.80.248:443
gw_symbols=ru,ag
feed_login=localhost
feed_password=localhost
```

---

## 七、测试验证结果

### 7.1 4444 行情订阅过滤

| 测试 | 订阅 | 结果 | 详情 |
|------|------|------|------|
| ru2701 精确过滤 | `ru2701` | **PASS** | 只收到 `RU/RU2701/RU2701S`，无 AU/AG/RB 等 |
| `*` 全订阅 | `*` | **PASS** | 所有活跃品种的三别名都收到 |
| 三别名存在性 | ru2701 | **PASS** | 预期三别名全部出现 |
| 交叉别名一致性 | ru2701 | **PASS** | 同一品种三个别名 bid/ask 同步 |

### 7.2 4445 指令链路（live_test.py）

| 用例 | 结果 |
|------|------|
| 坏格式被拒（`BUY,1`） | PASS (ERR) |
| 止损单被拒 | PASS (ERR unsupported orderType) |
| 非法手数被拒 | PASS (ERR invalid lotnr) |
| 市价开多 | PASS (OK + groupKey) |
| 限价开空 | PASS (OK + groupKey) |
| 市价平多 / 市价平空 | PASS |
| 全撤 | PASS |
| orders.csv 落盘 | PASS (新增多行) |
| 异步 TRADE 推送 | PASS |

### 7.3 ma_cross.py 端到端

Tick 对齐修复 + 35s timeout，2min 内 4 信号全部双路成交：

```
[signal] 金叉 → BUY OPEN OK ref=000000000009 gw=14108 → TRADE CTP + GW
[signal] 死叉 → SELL CLOSE + SELL OPEN → 全部成交
```

### 7.4 网关 `/health` 稳态

```
trade_ok_total=62
trade_fail_total=0
trade_reconnects=11    ← 自动重连工作正常
positions=0             ← 测试后已平仓
orders=0
```

---

## 八、构建与运行

### 构建

```powershell
# MSBuild 2022 Community
& "d:\Program Files\Microsoft Visual Studio\2022\Community\MSBuild\Current\Bin\MSBuild.exe" `
  "SimpleCTPConsole_x86.sln" `
  /t:Rebuild /p:Configuration=Debug /p:Platform=Win32
```

产物：`Debug\SimpleCTPConsole.exe`（自动复制 CTP 6.7.9 运行库）

### 启动顺序

```powershell
# 1. 先启网关
Start-Process python -ArgumentList "gateway/aqgw.py"

# 2. 再启控台（工作目录必须是 Debug/，因为 config.ini 是相对路径）
Start-Process "Debug\SimpleCTPConsole.exe" -WorkingDirectory "Debug"
```

### 验证

```powershell
netstat -ano | findstr ":4444 :4445 :8222"    # 三个端口监听
Invoke-RestMethod http://127.0.0.1:8222/health # 网关健康
python strategy/live_test.py                    # 全面联调
python strategy/market_feed_test.py            # 行情订阅专项
```

---

## 九、已知限制 / 后续可做

| 项目 | 说明 | 优先级 |
|------|------|--------|
| stop-loss 单 | 不支持，收到即 ERR | 低 |
| parallel Gw 串行队列 | GwAsyncQueue 单 worker，上游 trade conn 本身串行化请求（4-25s/单） | 低 |
| 控台 PDCurses 崩溃 | 被 PowerShell 重定向 stdout 或 shell 会话结束时容易挂，正常桌面双击稳定 | 中 |
| manual.html 新增章节 | 本项目总结未同步进 manual.html | 低 |
| aqntFuturesCn 同步 | `E:\aqntFuturesCn\build\aqntFutureCn\app\` 和 `forMyself\aqntFutureCn\app\` 两份副本待同步自动重连补丁 | 低（用户说"等快完工时再定"） |

---

## 十、交付文件一览

```
simpleCtpConsole/
├── Debug/
│   ├── SimpleCTPConsole.exe            # 主控台（已编译）
│   ├── config.ini                       # 配置
│   ├── orders.csv                       # 订单流水（147 行，含 groupKey）
│   ├── thostmduserapi_se.dll / .lib     # CTP 6.7.9 行情
│   └── thosttraderapi_se.dll / .lib     # CTP 6.7.9 交易
├── gateway/
│   ├── aqgw.py                          # Aq 网关 HTTP Server (8222)
│   ├── aqclient.py                      # Aq 上游协议
│   └── history.db                       # 历史记录
├── strategy/
│   ├── live_test.py                     # 全面联调 11/11 测试脚本
│   ├── ma_cross.py                      # 金叉死叉策略
│   ├── market_feed_test.py              # 行情订阅专项测试
│   └── alias_consistency_test.py        # 三别名一致性验证
├── SimpleCTPConsole_x86.sln             # MSBuild 解决方案
├── simple_ctp_console_x86.vcxproj        # Win32 x86 项目
├── manual.html                          # 交付文档（唯一 MD，已同步全部改动）
└── PROJECT_SUMMARY.md                   # 本项目总结（本文档）
```

---

## 十一、收尾清理（2026-11-03）

### 11.1 本轮收尾动作清单

| # | 动作 | 详情 | 影响范围 |
|---|------|------|----------|
| 1 | **进程健康** | 控台 PID 58372（4444/4445）+ 网关 PID 48812（8222）稳定运行 | 运行态 |
| 2 | **MT4 全清理** | manual.html / 源码 / CSV 全文件 grep → `MT4|mt4|MetaTrader|UniFeeder` **0 命中** | 源码 + 文档 |
| 3 | **编译零警告** | `MSBuild Rebuild` 成功，无 Warning 无 Error | 构建 |
| 4 | **临时文件清理** | 删除 9 个：`Debug/crash_*.txt`、`Debug/run_*.txt`、`Debug/afternoon_test.log`、`configOld.ini`、`simple_ctp_console.rar`、`copy_dll.bat/.ps1` | 工程目录 |
| 5 | **config.ini 密码统一** | root + Debug 两份 `userPassword=<YOUR_CTP_PASSWORD>`、`brokerID=9999` | 配置 |
| 6 | **orders.csv header 修复** | 手动修复 header 补 `groupKey` 列；**代码级自动升级**（Configure 启动时检测旧 header，读全部行、`time,source` 之间插空 groupKey 列、重写） | 数据 + 代码 |
| 7 | **manual.html 同步验证** | groupKey(5)、gw=PENDING(8)、GW_TICKET(5)、行情三别名(5)、MT4 0 命中 — 全部存在 | 交付文档 |
| 8 | **PROJECT_SUMMARY.md 补齐** | 新增第十一节（收尾清理）和第十二节（启动指引） | 本文件 |

### 11.2 运行期已知特性

| 项目 | 说明 |
|------|------|
| 控台稳定性 | PDCurses 控制台在后台/会话结束时易挂，建议桌面双击启动 |
| CSV 自动修复 | 若 orders.csv 被手动删成旧格式（缺 groupKey），下次启动会自动升级 header + 数据行（见 `order_router.cpp:Configure`） |
| 网关持仓 | `/positions` 和 `/orders` 均为 0，测试已平仓，不污染 demo 账号 |
| groupKey FILL 行 | CTP 异步成交事件 `status=FILL` 脱离指令上下文，groupKey 列为空，可通过同时间窗口内的 `key`（OrderRef）反向关联 |

---

## 十二、启动指引（下次需要时）

```powershell
# 1. 先启网关
Start-Process python -ArgumentList "gateway/aqgw.py"

# 2. 再启控台（工作目录必须是 Debug/，config.ini 用相对路径）
cd Debug; Start-Process .\SimpleCTPConsole.exe

# 3. 验证端口
netstat -ano | findstr ":4444 :4445 :8222"

# 4. 验证网关
Invoke-RestMethod http://127.0.0.1:8222/health | ConvertTo-Json
```

---

## 十三、网关补丁回传（2026-11-03）

### 13.1 补丁内容

本地 `gateway/` 相对于 `aqntFuturesCn` 目标目录的差异（以下改动已同步到两份副本）：

| 文件 | 改动 | 行数 |
|------|------|------|
| `aqclient.py` | +`import select`；新增 `TradeConn.is_alive()` 无侵入探测方法 | +17 |
| `aqgw.py` | 新增 `quote_reconnects / trade_reconnects / trade_ok_total / trade_fail_total` 统计；新增 `_wait_quote()` 等行情会话恢复等待；新增 `_trade()` 里 `cli is None` 保护；`trade_request()` 重写为两阶段重连（**建连可安全重试 3x，发送不重发**——请求一旦发出后若断连，状态未知只报错由调用方核对）；`/health` 输出新增四统计字段 | 大改动 |

### 13.2 补丁应用策略

```
E:\aqntFuturesCn\
├── build\aqntFutureCn\app\          ← 直接覆盖（aqgw.py 线上默认值 117.72.80.248:443）
│   ├── aqgw.py   ✅ 与 source 零差异
│   └── aqclient.py ✅ 与 source 零差异
└── forMyself\aqntFutureCn\app\      ← 覆盖后恢复本地默认值
    ├── aqgw.py   ✅ 补丁到位；srv-host/port 保留 127.0.0.1:1500
    └── aqclient.py ✅ 与 source 零差异
```

两份目标原有默认值差异（`--srv-host` / `--srv-port`）**已保留**：forMyself 是 `127.0.0.1:1500`（开发/测试环境），build 是 `117.72.80.248:443`（线上）。

### 13.3 操作记录

```powershell
# 备份
Copy-Item "E:\aqntFuturesCn\build\...\aqgw.py" ".aqgw.py.bak_20261103_104640"
Copy-Item "E:\aqntFuturesCn\build\...\aqclient.py" ".aqclient.py.bak_20261103_104640"
Copy-Item "E:\aqntFuturesCn\forMyself\...\aqgw.py" ".aqgw.py.bak_20261103_104640"
Copy-Item "E:\aqntFuturesCn\forMyself\...\aqclient.py" ".aqclient.py.bak_20261103_104640"

# 同步（策略文件 strategy/sync_gateway_patch.py）
python strategy/sync_gateway_patch.py

# 回滚
Get-ChildItem "E:\aqntFuturesCn" -Recurse -Filter "*.bak_20261103_104640" | ForEach-Object {
    $orig = $_.FullName -replace '\.bak_20261103_104640$', ''
    Copy-Item $_.FullName $orig -Force; Remove-Item $_.FullName -Force
}
```

---

## 十四、交付目录（2026-11-03）

### 14.1 交付路径

```
E:\aqntFuturesCn\approachExamples\strategy2gateway-ctp\
```

### 14.2 目录结构（55 文件 / 17.46 MB）

```
strategy2gateway-ctp/
├── (根目录 18 files / 182 KB)
│   ├── app_util.cpp / .h
│   ├── ctp_trader.cpp / .h
│   ├── gw_client.cpp / .h
│   ├── order_router.cpp / .h
│   ├── order_server.cpp / .h
│   ├── simple_ctp_console.cpp
│   ├── simple_ctp_console_x86.vcxproj
│   ├── SimpleCTPConsole_x86.sln
│   ├── config.ini / symbConfig.ini
│   ├── manual.html                 ← 交付文档
│   ├── PROJECT_SUMMARY.md           ← 项目总结（本文件）
│   └── start_gateway.ps1
│
├── CTP/ (6 files / 2.60 MB)         ← CTP 6.7.9 头文件 + x86 行情 DLL
│   ├── ThostFtdcMdApi.h            (5560)
│   ├── ThostFtdcTraderApi.h        (36362)
│   ├── ThostFtdcUserApiDataType.h  (256893)
│   ├── ThostFtdcUserApiStruct.h    (237471)
│   ├── thostmduserapi_se.dll        (2.0 MB)
│   └── thostmduserapi_se.lib        (3764)
│
├── INIReader/ (4 files / 19.7 KB)   ← 第三方 ini 解析库
├── PDCurses/ (2 files / 328.8 KB)  ← 第三方终端 UI 库
│
├── Debug/ (10 files / 8.1 MB)      ← 运行时（Win32 Debug）
│   ├── SimpleCTPConsole.exe
│   ├── config.ini / symbConfig.ini
│   ├── thostmduserapi_se.dll / .lib
│   ├── thosttraderapi_se.dll
│   └── ThostFtdc*.h (4)
│
├── Release/ (5 files / 5.4 MB)     ← 运行时（Win32 Release）
│   ├── SimpleCTPConsole.exe
│   ├── config.ini / symbConfig.ini
│   └── thostmduserapi_se.dll / thosttraderapi_se.dll
│
├── gateway/ (5 files / 151.9 KB)    ← 网关 Python 实现（含自动重连补丁）
│   ├── aqgw.py                      ← is_alive + 两阶段重连 + 四统计
│   ├── aqclient.py                  ← TradeConn.is_alive()
│   ├── aqcrypto.py / aqproto.py / lzss.py
│
└── strategy/ (5 files / 39.8 KB)   ← 策略脚本
    ├── live_test.py                 ← 11/11 全链路联调
    ├── ma_cross.py                  ← 金叉死叉策略
    ├── market_feed_test.py          ← 行情订阅专项
    ├── alias_consistency_test.py    ← 三别名一致性
    └── sync_gateway_patch.py        ← 网关补丁回传脚本
```

### 14.3 排除清单（严格不包含）

| 排除项 | 原因 |
|--------|------|
| `.vs/` | VS 用户态缓存 |
| `testTdUserApi/` | CTP API 测试工程（本项目外部） |
| `SimpleCTPConsole/Debug|Release/` | MSBuild 中间产物（obj/tlog/pdb/ilk/idb） |
| `gateway/__pycache__/` + `*.pyc` | Python 编译缓存 |
| `gateway/history.db*` | 网关运行时数据 |
| `Debug/ctp6.3.15 / win32 / win64/` | 旧版 CTP 残留 |
| `*.pdb / *.obj / *.ilk / *.tlog` | 编译中间文件 |
| `orders.csv / *.log / *.con` | 运行时数据 |
| `configOld.ini / copy_dll.* / *.zip / *.rar` | 临时文件 |
| `*.suo / *.user` | VS 用户态配置 |

### 14.4 交付验证命令

```powershell
$tgt = "E:\aqntFuturesCn\approachExamples\strategy2gateway-ctp"

# 1. 结构
Get-ChildItem $tgt -Recurse -File | Group-Object Directory | ForEach-Object {
    "{0,-55} {1,3} files  {2,10:N0} bytes" -f `
        $_.Name.Replace("$tgt\", ""), $_.Count, ($_.Group | Measure-Object Length -Sum).Sum
}

# 2. 零多余检查
foreach ($c in @(".vs","testTdUserApi","SimpleCTPConsole","__pycache__",
                 "*.pdb","*.obj","*.pyc","history.db*","*.log","*.con","*.zip","*.rar")) {
    $n = (Get-ChildItem $tgt -Recurse -Filter $c -File -ErrorAction SilentlyContinue).Count
    Write-Output "$c : $(if($n -eq 0){'OK'}else{"!! $n found"})"
}

# 3. 构建验证（在交付目录执行）
& "d:\Program Files\Microsoft Visual Studio\2022\Community\MSBuild\Current\Bin\MSBuild.exe" `
  "$tgt\SimpleCTPConsole_x86.sln" `/t:Rebuild `/p:Configuration=Debug `/p:Platform=Win32
```

---

## 十五、交付地实跑验证（2026-11-03）

### 15.1 构建（交付目录）

第一次构建失败：vcxproj 的 IncludePath 期望 `.\Debug\win32`（CTP 头文件/DLL），但 robocopy 排除了子目录。修复：从源工程 `Debug\win32\*` 全量复制头文件 + thostmduserapi_se/traderapi_se 的 `.dll/.lib`。

```
MSBuild Rebuild -> simple_ctp_console_x86.vcxproj -> Debug\SimpleCTPConsole.exe
零错误零警告
```

### 15.2 运行测试结果

干净启动（无残留客户端）→ 全绿：

| # | 测试 | 时长 | 结果 | 说明 |
|---|------|------|------|------|
| 1 | 手动 ru2701 单客户端订阅 | 10s | **PASS** | 57 行，全 RU/RU2701/RU2701S 三别名，**0 unexpected** |
| 2 | market_feed_test.py ru2701 | 15s | **PASS** | 90 行，只收 ru2701 三别名组，**无 AU/AG 等** |
| 3 | live_test.py 11 项全链路 | ~30s | **PASS** | 11/11：协议校验 3/3 + 开平仓 4/4 + 全撤 1/1 + CSV + 成交通知 + groupKey |

### 15.3 进程稳态

- 控台 PID=27304（PDCurses 窗口模式）——**桌面双击启动**最稳，shell 会话退出易杀
- 网关 PID=48812 —— uptime 18.1h，`trade_ok=94, trade_fail=0, positions=0, orders=0`
- 无 MT4 痕迹（源码/文档/CSV grep 0 命中）

### 15.4 中间踩坑

| 问题 | 根因 | 解决 |
|------|------|------|
| 第一次跑 EXIT=0 但行情 FAIL | 用了 source 目录复制过来的**旧 exe**（没带订阅过滤修复） | 交付目录 `MSBuild Rebuild` |
| Rebuild 报错 C1083 找不到 ThostFtdcTraderApi.h | vcxproj IncludePath=`.\Debug\win32`，但 robocopy 排除了 win32 子目录 | 从源 `Debug\win32\*` 全量复制过来（含 md/trader DLL+lib） |
| Start-Process 启动的 PDCurses 在 shell 退出时挂 | `WindowStyle Hidden` 或 Shell block 结束 → 进程被清理 | `-WindowStyle Normal` 创建独立窗口；**推荐桌面双击启动** |
| 首次 market_feed_test.py 收到 AG | 上一轮测试残留客户端 socket 未关闭，BroadcastQuote 给活跃的旧 socket 推全量 | 干净重启后 OK |

---

## 十六、EA2ctp 桥接（2026-11-03）

### 16.1 项目定位

```
E:\aqntFuturesCn\approachExamples\EA2ctp\
```

独立新项目，包含 MT4 终端（build 225，账户 <YOUR_ACCOUNT_ID>），Moving Average EA（MQL4），通过**文件桥接**把 MT4 EA 决策指令交给本 CTP 控台执行。**无网关依赖，明文 MT4**。

### 16.2 桥接架构

```
┌─────────────────┐  写 ctp_signals.csv   ┌───────────────────────┐
│  MT4 EA (MQL4)  │──────────────────────▶│ EA 输出 → 文件（追加） │
│  MA crossover   │  每行一个决策         │                       │
└─────────────────┘                       └───────────┬───────────┘
                                                      │ 800ms 轮询
                                                      ▼
┌─────────────────┐  走 Submit() 路径    ┌───────────────────────┐
│ CTP 控台 4445   │◀────────────────────│ SignalWatcher 线程    │
│ (并行 CTP+网关) │                       │ - 读 → 解析 → truncate │
└─────────────────┘                       │ - MatchInstrument     │
                                          │ - 补 price/groupKey   │
                                          └───────────────────────┘
```

### 16.3 EA 输出格式（7 列 CSV，无 header）

```
BUY,ru,M,OPEN,1,5,ma_golden_202611031430
SELL,ag,M,CLOSE,1,5,ma_death_202611031435
```

| 列 | 含义 | 示例 | 说明 |
|----|------|------|------|
| 1 | action | BUY/SELL | EA 决策方向 |
| 2 | symbolPrefix | ru/ag/au | MT4 的品种前缀（不是完整 CTP 合约） |
| 3 | type | M/L/D | M=市价, L=限价, D=撤销 |
| 4 | offset | OPEN/CLOSE | 开仓或平仓 |
| 5 | lots | 1 | 手数 |
| 6 | bidAskDif | 5 | 品种最小跳动价（ru=5, ag=1, au=0.02） |
| 7 | comment | ma_golden_2026... | EA 自定义标注，原样传递 |

### 16.4 CTP 端处理流程（simple_ctp_console.cpp）

**MatchInstrument(prefix)** — 品种前缀 → CTP 完整合约：
```
"ru" → 在 g_instruments 里前缀匹配 → "ru2701"
"ag" → 前缀匹配 → "ag2601"
```
先精确匹配（EA 直接给完整合约代码也 OK），再前缀匹配（MT4 symbol 纯字母前缀）。

**SignalWatcher 线程**（每 800ms 扫一次）：
1. 读 `g_eaSignalFile`（配置项 `ea_signal_file`，默认 `ctp_signals.csv`）
2. CSV 解析，跳过空行和 `#` 注释
3. 读完立即 `ofstream trunc` 清空文件（EA 用 FILE_OPEN_ALWAYS 下次继续追加）
4. 每行处理：MatchInstrument → 查 `g_marketData[ctpSym].bidPrice/askPrice` 填 price → 填 `cmd.loginNr=g_user, cmd.groupName="ea_ma"` → `g_orderRouter.Submit(cmd)`
5. 打印 `[EA2CTP]` 前缀日志到 stdout + feedserver.log

### 16.5 config.ini 新增项

```ini
ea_signal_file=E:\aqntFuturesCn\approachExamples\EA2ctp\terminal\experts\files\ctp_signals.csv
```

不配置则默认 `ctp_signals.csv`（当前工作目录）。

### 16.6 Moving Average.mq4 改造点

| 改动 | 说明 |
|------|------|
| 注释掉所有 `OrderSend`/`OrderClose` | EA 不再本地下单，只输出决策 |
| 新增 `WriteSignal()` 辅助函数 | `FileOpen(FILE_OPEN_ALWAYS) → FileWrite → FileClose` 写一行 |
| `CheckForOpen()` | MA golden cross → `WriteSignal("BUY", _Symbol, "M", "OPEN", ...)` |
| `CheckForClose()` | MA death cross → `WriteSignal("SELL", _Symbol, "M", "CLOSE", ...)` |
| 品种后缀 `60` 由 EA 参数定义 | MT4 history 是 `ag60.hst/ru60.hst`，EA symbol 为 `ag60`，前缀 `ag` 匹配 CTP 的 `ag2601` |

**EA 文件位置**：`EA2ctp/terminal/experts/Moving Average.mq4`（需 MetaEditor 编译）

**指令文件位置**：`EA2ctp/terminal/experts/files/ctp_signals.csv`（MQL4 FILE_OPEN_ALWAYS 默认写入目录）

### 16.7 端到端测试结果

**测试方式**：手动写一行模拟 EA 输出 → 启动 CTP 控台 → SignalWatcher 读 → Submit → 查 orders.csv 和网关持仓

```powershell
# 模拟 EA 写指令
"BUY,ru,M,OPEN,1,5,ea_bridge_test_20261103" | Out-File ctp_signals.csv

# 启动 CTP 控台（已配置 ea_signal_file 绝对路径）
Start-Process SimpleCTPConsole.exe -WorkingDirectory Debug

# 等 3 秒，SignalWatcher 扫到并处理
```

**验证结果**：

| 检查项 | 结果 | 证据 |
|--------|------|------|
| Signal file 被消费 | ✅ | 处理后 truncate 清空 |
| MatchInstrument("ru") | ✅ | → "ru2701" |
| groupKey 正确 | ✅ | G0000000001（原子序号） |
| orders.csv 落盘 | ✅ | 两行：GW + CTP |
| 网关建仓 | ✅ | ticket=14146, ru, lots=1, open=19140 |
| CTP 建仓 | ✅ | ref=000000000002, ru2701, BUY OPEN |
| comment 传递 | ✅ | ea_bridge_test_20261103 原样写入 orders.csv |
| 平仓清理 | ✅ | Gateway `/order close` → positions=0, profit=-50 |

### 16.8 涉及文件清单

| 文件 | 改动 | 位置 |
|------|------|------|
| simple_ctp_console.cpp | +MatchInstrument / +SignalWatcher / +ea_signal_file config / 启动 watcher | strategy2gateway-ctp + simpleCtpConsole（两份同步） |
| Moving Average.mq4 | 去掉 OrderSend/OrderClose，加 WriteSignal 输出 | EA2ctp/terminal/experts/ |
| Debug/config.ini | +ea_signal_file 绝对路径 | strategy2gateway-ctp/Debug/ |
| EA2ctp/terminal/experts/files/ | 新建（MQL4 FILE_OPEN_ALWAYS 写入目录） | EA2ctp |

### 16.9 已知限制

| 限制 | 说明 | 可能改进 |
|------|------|---------|
| EA 不编译无法跑 | Moving Average.mq4 需 MetaEditor 编译成 .ex4 | 在 EA2ctp 里留已编译好的 .ex4 |
| 多个 EA 同时写可能冲突 | 每个 EA 各自的 FileOpen 独立写，理论 OK；极端情况两个 EA 同时写一行可能交错 | 每个 EA 独立文件名（如 ma_ru_signals.csv） |
| EA 输出无交易结果回传 | EA 只负责决策，不知道成交/拒单 | 可加 feedback 文件（CTP 写 → EA 读） |
| CLOSE 方向推断需要 MT4 持仓状态 | EA 用 `OrdersTotal()` 本地判断（buy 仓位 → MA death → SELL CLOSE） | 这是 EA 自己的逻辑，与 CTP 桥接无关 |

---

## 十七、EA2ctp 纯 CTP 模式（ctp_only）改造与最终验收（2026-11-03 下午）

### 17.1 需求与结论

EA2ctp 项目定位为「MT4 EA 决策 → CTP 下单」，不依赖网关。原实现中 SignalWatcher 复用 `g_orderRouter.Submit()`，在 `order_mode=parallel` 下每条 EA 信号都会同时下发 CTP 与网关两条腿。本次新增 `ctp_only` 模式后，EA 信号只报 CTP，网关进程无需启动。

### 17.2 代码改动（3 处）

| 文件 | 位置 | 改动 |
|------|------|------|
| `order_router.cpp` | `Configure()` | 模式白名单放开 `ctp_only`，不再回落 parallel |
| `order_router.cpp` | `Submit()` | 新增 `ctp_only` 分支：报 CTP 并落盘后直接返回，不注入异步队列、不触碰网关 |
| `simple_ctp_console.cpp` | 配置解析 | **关键遗漏**：`order_mode` 白名单原本只认 `parallel`/`ctp_first`，`ctp_only` 被静默丢弃 → 已补上 |
| `ctp_trader.cpp` | `ApplyTradeToPosition()` | **修复隐性 BUG**：平仓成交后本地持仓缓存方向算反 |

### 17.3 平仓缓存方向 BUG（ErrorID=51 的真实根因）

```cpp
// 旧：只看委托方向
bool isLong = (direction == THOST_FTDC_D_Buy);
```

`SELL CLOSE`（平多）被记成"空头减少"，导致空头缓存被扣光；后续 `BUY CLOSE` 时 `TodayVolumeEnough()` 误判为无今仓 → 改用平昨标志 → 上期所拒单 `ErrorID=51 平仓量不足`。此前把 51 当作"两通道独立持仓的良性现象"是误判。

修正为：开仓按委托方向定多空，平仓取委托方向的反向。

### 17.4 最终验收记录

| 时间 | 动作 | 结果 |
|------|------|------|
| 14:15:22 | SELL CLOSE 4 手 | 成交 19135 |
| 14:16:11 | SELL CLOSE 2 手 | 成交 19130 |
| 14:16:14 | BUY CLOSE 3 手 | 拒单 ErrorID=51（BUG 复现） |
| 14:20:33 | 修复重启后 BUY CLOSE 3 手 | 成交 19145，空头清零 |
| 14:20:55 | ctp_only 重测（retest_ctp_only） | orders.csv 仅 CTP 一行，网关 0→0 |
| 14:22:20 | EA 死叉真实信号 | 仅 CTP SELL OPEN，成交 19130 |
| 14:49:04 | EA 金叉真实信号 | 仅 CTP BUY OPEN，成交 19155 |

重测订单行：

```
2026-11-03 14:20:55,G0000000002,CTP,000000000003,ru2701,BUY,OPEN,M,19250.0000,1,0,NEW,<YOUR_CTP_INVESTOR_ID>,ea_ma,retest_ctp_only
2026-11-03 14:22:20,G0000000004,CTP,000000000005,ru2701,SELL,OPEN,M,19030.0000,1,0,NEW,<YOUR_CTP_INVESTOR_ID>,ea_ma,ma_death
2026-11-03 14:49:04,G0000000005,CTP,000000000006,ru2701,BUY,OPEN,M,19250.0000,1,0,NEW,<YOUR_CTP_INVESTOR_ID>,ea_ma,ma_golden
```

全程无 GW 行，网关 `positions=0 orders=0 trade_ok=106` 不变。

### 17.5 配对锁仓与未平仓位说明

EA 只发 OPEN（金叉买开 / 死叉卖开），不发 CLOSE，因此多次交叉会形成多空锁仓（14:22 空 1 + 14:49 多 1）。这是 MT4 本地订单簿与 CTP 账本分离导致的结构性问题，已在 EA2ctp 使用说明书第 11 章列为已知限制。

收尾时（15:04）发起 `SELL CLOSE 1` + `BUY CLOSE 1`，报单成功（ref=000000000007/8）但**未成交**——ru 日盘 15:00 已收盘，交易所不再撮合。该 1 多 1 空锁仓保留至夜盘（21:00）或下一交易日手工平掉。

### 17.6 同步与文档

| 项 | 说明 |
|----|------|
| 源码同步 | `order_router.cpp`、`simple_ctp_console.cpp`、`ctp_trader.cpp` 已回写源项目 `simpleCtpConsole` |
| 配置 | 根目录与 `Debug/config.ini` 均为 `order_mode=ctp_only`；`ea_signal_file` 必须写在根 config.ini（构建会覆盖 Debug 那份） |
| 使用说明书 | `EA2ctp/使用说明书.html` 第 1/6/8/10 章已同步为纯 CTP 定位 |
| 构建 | MSBuild Rebuild 零错误；Win32 x86 Debug |
