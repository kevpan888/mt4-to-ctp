# Mt4 to CTP

[![量化平台 aqnt.top](https://img.shields.io/badge/量化平台-aqnt.top-00b96b?logo=web)](https://aqnt.top) [![GitHub](https://img.shields.io/badge/GitHub-kevpan888%2Fmt4--to--ctp-181717?logo=github)](https://github.com/kevpan888/mt4-to-ctp) [![GitHub stars](https://img.shields.io/github/stars/kevpan888/mt4-to-ctp?style=social)](https://github.com/kevpan888/mt4-to-ctp/stargazers)

> 本项目是 [**aqnt.top — 人与 AI Agent 共同的量化空间**](https://aqnt.top) 开源生态的一部分：让 MT4 EA 与自研策略无缝接入国内期货 CTP 交易。

把 **MetaTrader 4 (MT4) 的 EA 决策**与**自研 Python 策略**桥接到 **CTP（综合交易平台）** 期货实盘/仿真交易的一整套开源示例。

仓库包含两个可独立运行的子项目，共享同一套 C++ CTP 控制台内核（`SimpleCTPConsole`）：

```
mt4-to-ctp/
├── EA2ctp/                # MT4 EA 决策 → 文件桥接 → CTP 下单（纯 CTP，不依赖网关）
└── strategy2gateway-ctp/  # Python 策略/网关 → TCP 文本协议 → CTP 控制台（双路：CTP + Aq 网关）
```

---

## 一、它能做什么

| 能力 | 说明 |
|------|------|
| 行情接入 | CTP（SimNow 为主）+ Aq Gateway（备），断线自动切换 / 回切 |
| 交易通道 | CTP 单路（`ctp_only`）或 CTP + 网关双路（`parallel` / `ctp_first`） |
| 对外协议 | 纯文本 TCP：`4444` 行情推送、`4445` 指令/应答，策略脚本可直接 socket 连接 |
| EA 桥接 | MT4 的 MQL4 EA 只输出决策到 `ctp_signals.csv`，C++ 端轮询消费并报单 CTP |
| 订单关联 | 每条指令生成原子 `groupKey`（`G0000000001`…），关联 CTP 与网关双侧成交 |
| 运行形态 | Win32 x86 C++ 控制台（MSBuild 2022 + CTP 6.7.9）+ Python 网关/策略 |

---

## 二、子项目

### 1. `EA2ctp/` —— MT4 EA → CTP

- `terminal/`：MT4 终端（含已改造的 `experts/Moving Average.mq4`，金叉/死叉写决策文件，不再本地下单）
- `ctp_console/`：C++ CTP 控制台，`SignalWatcher` 线程每 800ms 扫描 `ctp_signals.csv`，解析后 `Submit()` 到 CTP
- `使用说明书.html`：完整使用文档
- 配置：`order_mode=ctp_only`，EA 信号只报 CTP，网关无需启动

### 2. `strategy2gateway-ctp/` —— 策略/网关 → CTP

- C++ 控制台内核（`simple_ctp_console.cpp` / `order_router.cpp` / `ctp_trader.cpp` …）
- `gateway/`：Python Aq 网关（HTTP 8222，含自动重连、`/health` 统计）
- `strategy/`：Python 策略与联调脚本（`ma_cross.py` 金叉死叉、`live_test.py` 全链路、`market_feed_test.py` 行情订阅）
- `manual.html`：完整交付文档

---

## 三、快速开始

> ⚠️ 所有账号/密码均已在仓库中替换为占位符（`<YOUR_CTP_PASSWORD>` 等）。运行前请先填写自己的配置。
> 默认地址为 SimNow 仿真前置，建议先在仿真环境充分验证，切勿直接用于实盘。

### 3.1 填写配置

编辑各子项目下的 `config.ini`（根目录一份，`Debug/` 一份，构建时根目录会覆盖到 `Debug/`）：

```ini
userLogin=<YOUR_CTP_INVESTOR_ID>
userPassword=<YOUR_CTP_PASSWORD>
brokerID=9999
connectaddress=tcp://182.254.243.31:30011,tcp://182.254.243.31:30012,tcp://182.254.243.31:30013
tradeaddress=tcp://182.254.243.31:30001,tcp://182.254.243.31:30002,tcp://182.254.243.31:30003
instruments=ru2701,au2512,ag2601,...
order_mode=ctp_only        ; 或 parallel / ctp_first
```

`EA2ctp` 还需在 `config.ini` 中指定 EA 决策文件路径：

```ini
ea_signal_file=<...>\EA2ctp\terminal\experts\files\ctp_signals.csv
```

### 3.2 构建（可选，已附预编译产物）

```powershell
& "d:\Program Files\Microsoft Visual Studio\2022\Community\MSBuild\Current\Bin\MSBuild.exe" `
  "SimpleCTPConsole_x86.sln" /t:Rebuild /p:Configuration=Debug /p:Platform=Win32
```

`Debug/` 与 `Release/` 目录已附带预编译的 `SimpleCTPConsole.exe` 和 CTP 6.7.9 运行库，可不构建直接运行。

### 3.3 启动

**strategy2gateway-ctp：**
```powershell
Start-Process python -ArgumentList "gateway/aqgw.py"          # 1) 网关
Start-Process "Debug\SimpleCTPConsole.exe" -WorkingDirectory "Debug"  # 2) 控台
```

**EA2ctp：**
```powershell
# 1) 启动 MT4，加载 Moving Average EA（已编译 .ex4 或用 MetaEditor 编译 .mq4）
# 2) 启动 CTP 控台，工作目录为 ctp_console\Debug
Start-Process "Debug\SimpleCTPConsole.exe" -WorkingDirectory "Debug"
# 3) EA 产生金叉/死叉 → 写 ctp_signals.csv → 控台 SignalWatcher 消费 → CTP 报单
```

### 3.4 验证

```powershell
netstat -ano | findstr ":4444 :4445 :8222"
Invoke-RestMethod http://127.0.0.1:8222/health | ConvertTo-Json
python strategy/live_test.py
python strategy/market_feed_test.py
```

---

## 四、协议速览

**行情（4444）** —— 每个 tick 推三行别名：
```
ru2701 1793671841 19220 19225     # CTP 原生合约
ru     1793671841 19220 19225     # 连续主力别名
ru2701s 1793671841 19220 19225    # 合约+s 别名
```

**指令（4445）**：
```
BUY,1,ru2701,19250.00,<YOUR_CTP_INVESTOR_ID>,ai_test,M,OPEN,5.0,comment
```

**应答**：
```
OK ref=G0000000001 gw=PENDING group=G0000000001
GW_TICKET G0000000001,14108
TRADE 12345,19245,1,CTP
```

详见各子项目的 `manual.html` / `使用说明书.html`。

---

## 五、目录说明与已排除内容

仓库已剔除构建中间产物与运行时数据（`.vs/`、`SimpleCTPConsole/` 中间目录、`*.obj/*.tlog/*.pdb`、`__pycache__/`、`*.log/*.con`、`orders.csv`、`history.db*` 等），仅保留源码、配置模板、文档与预编译运行时产物。完整排除规则见 `.gitignore`。

---

## 六、关于 aqnt.top

[**aqnt.top（壹股量化空间）**](https://aqnt.top) 是一个让 **人与 AI Agent 协作**的量化投研与交易平台，覆盖中国期货与 A 股，兼容 MT4 接口，并对外提供 HTTP API（`/strategy` 执行交易、`/market` 推送行情、`/login`、`/health` 等）。

本仓库 `strategy2gateway-ctp/` 中的 Aq 网关（`gateway/aqgw.py`）即为 aqnt.top 平台交易网关的本地适配实现。如果你想把 MT4 / 自研策略 / AI Agent 一键接入国内期货与股票市场，欢迎访问 👉 **[aqnt.top](https://aqnt.top)**。

---

## 七、致谢 / Acknowledgements

- **[openctp](https://github.com/openctp/openctp)**：本仓库各子项目 `PDCurses/` 目录中的 `curses.h` 与 `pdcurses.lib` 来源于开源项目 openctp，感谢 openctp 社区的工作。
- **PDCurses**（Public Domain Curses）：公有领域软件。
- **CTP API**：版权归上海期货信息技术有限公司所有。
- **MetaTrader 4**：MetaQuotes 专有软件，本仓库仅作示例打包。

---

## 八、免责声明

- 本项目仅供学习与技术研究，**不构成任何投资建议**。
- 期货交易有风险，使用前请在 CTP 仿真环境（SimNow）充分验证。
- `EA2ctp/terminal` 内含的 MT4 终端为 MetaQuotes 专有软件，仅作示例打包，请遵守其许可协议；如需正式使用请从官方渠道获取。
- CTP API 版权归上海期货信息技术有限公司所有。
- 作者不对因使用本代码造成的任何盈亏或损失负责。

## 九、路线图 / Roadmap

- [x] MT4 EA → CTP 单路下单（`EA2ctp`，文件桥接）
- [x] Python 策略/网关 → CTP 控制台（双路：CTP + Aq 网关，断线自动切换 / 回切）
- [x] 纯文本 TCP 协议（4444 行情 / 4445 指令），策略脚本可直接 socket 接入
- [x] 预编译运行时产物，开箱即用
- [ ] A 股股票通道（对接 openctp / 各券商柜台的 CTPAPI 兼容接口）
- [ ] 更多内置策略示例（网格、套利、AI Agent 信号）
- [ ] 一键部署脚本与 Docker 镜像
- [ ] 可视化订单 / 持仓监控面板

完整变更记录见 [CHANGELOG.md](CHANGELOG.md)。

---

## ⭐ 支持本项目

如果这个项目对你有帮助，欢迎在 GitHub 上点个 **Star**，也欢迎提 Issue / PR 反馈。你的支持是持续维护的最大动力 👉 [**去 Star**](https://github.com/kevpan888/mt4-to-ctp/stargazers)

更多项目与平台能力见 👉 [**aqnt.top — 壹股量化空间**](https://aqnt.top)

## License

[MIT](LICENSE)
