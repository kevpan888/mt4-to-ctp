# Changelog

本项目的重要变更记录。格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)。

## [Unreleased]

- A 股股票通道（对接 openctp / 各券商柜台的 CTPAPI 兼容接口）
- 更多内置策略示例（网格、套利、AI Agent 信号）
- 一键部署脚本与 Docker 镜像
- 可视化订单 / 持仓监控面板

## [1.0.0] - 2026-11-03

首次开源发布。

### Added
- `EA2ctp`：MT4 EA 决策 → 文件桥接 → CTP 下单（纯 CTP 单路，不依赖网关）
- `strategy2gateway-ctp`：Python 策略/网关 → TCP 文本协议 → CTP 控制台
- 双交易通道：CTP 单路（`ctp_only`）/ CTP + Aq 网关双路（`parallel`、`ctp_first`），行情断线自动切换与回切
- 纯文本 TCP 协议：4444 行情推送、4445 指令/应答，策略脚本可直接 socket 接入
- 原子 `groupKey` 订单关联机制，关联 CTP 与网关双侧成交
- 预编译运行时产物（Win32 x86，MSBuild 2022 + CTP 6.7.9），开箱即用
- 完整文档：`README.md`、各子项目 `manual.html` / `使用说明书.html`
- MIT License；致谢 openctp（PDCurses 来源）、CTP API、PDCurses、MetaTrader 4

### Notes
- 所有账号 / 密码在开源版本中均已替换为占位符（`<YOUR_CTP_PASSWORD>` 等），使用前请在 `config.ini` 中填入自己的配置
- 默认地址为 SimNow 仿真前置，建议先在仿真环境充分验证后再用于实盘
- 本项目为 [aqnt.top](https://aqnt.top) 量化平台开源生态的一部分
