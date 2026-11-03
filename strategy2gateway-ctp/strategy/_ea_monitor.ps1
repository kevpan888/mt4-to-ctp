# EA2ctp 下午持续监控 — 每 3 分钟抓一次全链路状态
# 写到 ctp_ea_monitor.log，同时 stdout 输出

$ea = "E:\aqntFuturesCn\approachExamples\EA2ctp"
$ctp = "E:\aqntFuturesCn\approachExamples\strategy2gateway-ctp"
$log = "$ctp\ctp_ea_monitor.log"
$sig = "$ea\terminal\experts\files\ctp_signals.csv"
$ordersCsv = "$ctp\Debug\orders.csv"

"=== EA2ctp MONITOR STARTED $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ===" | Tee-Object $log | Out-Null

while ($true) {
    $now = Get-Date -Format 'HH:mm:ss'
    "--- $now ---" | Tee-Object -Append $log | Out-Null

    # 1. 进程
    $mt4 = (Get-Process mt4 -ErrorAction SilentlyContinue).Id
    $con = (Get-Process SimpleCTPConsole -ErrorAction SilentlyContinue).Id
    "mt4=$mt4 console=$con" | Tee-Object -Append $log | Out-Null

    # 2. 信号文件
    if (Test-Path $sig) {
        $lines = Get-Content $sig -ErrorAction SilentlyContinue
        if ($lines.Count -gt 0) {
            "!! SIGNAL FILE HAS $($lines.Count) LINE(S):" | Tee-Object -Append $log | Out-Null
            $lines | Tee-Object -Append $log | Out-Null
        } else {
            "signal file empty (watcher consumed)" | Tee-Object -Append $log | Out-Null
        }
    } else {
        "signal file not created yet (EA no trigger)" | Tee-Object -Append $log | Out-Null
    }

    # 3. orders.csv 增量（只看 ea_ma 组）
    if (Test-Path $ordersCsv) {
        $all = Get-Content $ordersCsv
        $eaRows = $all | Select-String ",ea_ma," | Select-Object -Last 3
        $total = ($all | Select-String ",ea_ma,").Count
        "ea_ma rows total=$total" | Tee-Object -Append $log | Out-Null
        foreach ($r in $eaRows) { "  $($r.Line)" | Tee-Object -Append $log | Out-Null }
    }

    # 4. 网关
    try {
        $gw = (Invoke-RestMethod "http://127.0.0.1:8222/health" -UseBasicParsing).data
        "gw: connected=$($gw.connected) ok=$($gw.trade_ok_total) fail=$($gw.trade_fail_total) pos=$($gw.positions) ord=$($gw.orders)" | Tee-Object -Append $log | Out-Null
        $pos = (Invoke-RestMethod "http://127.0.0.1:8222/positions" -UseBasicParsing).data
        if ($pos.Count -gt 0) {
            "!! OPEN POSITIONS ($($pos.Count)):" | Tee-Object -Append $log | Out-Null
            foreach ($p in $pos) { "  ticket=$($p.ticket) sym=$($p.symbol) lots=$($p.lots)" | Tee-Object -Append $log | Out-Null }
        }
    } catch { "gw unreachable" | Tee-Object -Append $log | Out-Null }

    # 5. EA 日志尾部
    $eaLog = Get-ChildItem "$ea\terminal\experts\logs\*.log" -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if ($eaLog) {
        $tail = Get-Content $eaLog.FullName -Tail 5
        foreach ($t in $tail) { "ea: $t" | Tee-Object -Append $log | Out-Null }
    }

    "sleep 180s..." | Tee-Object -Append $log | Out-Null
    Start-Sleep -Seconds 180
}
