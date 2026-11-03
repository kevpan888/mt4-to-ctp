# SimpleCTPConsole watchdog — 每 60 秒检查进程 + 端口，挂了重启
# 后台常驻，直到手动 kill
$ctp = "E:\aqntFuturesCn\approachExamples\strategy2gateway-ctp"

Write-Output "[$(Get-Date -Format 'HH:mm:ss')] WATCHDOG STARTED"
$lastPid = $null

while ($true) {
    $alive = Get-Process SimpleCTPConsole -ErrorAction SilentlyContinue
    $hasPort = (netstat -ano 2>$null | Select-String ":4444.*LISTENING" -Quiet)

    if ($alive -and $hasPort) {
        if ($lastPid -ne $alive.Id) {
            Write-Output "[$(Get-Date -Format 'HH:mm:ss')] CONSOLE UP PID=$($alive.Id)"
            $lastPid = $alive.Id
        }
    } else {
        Write-Output "[$(Get-Date -Format 'HH:mm:ss')] CONSOLE DOWN (alive=$($alive -ne $null) port=$hasPort) — RESTART"
        if ($alive) { Stop-Process -Id $alive.Id -Force -ErrorAction SilentlyContinue }
        Start-Sleep -Seconds 2
        $new = Start-Process -FilePath "$ctp\Debug\SimpleCTPConsole.exe" -WorkingDirectory "$ctp\Debug" -WindowStyle Normal -PassThru
        Write-Output "[$(Get-Date -Format 'HH:mm:ss')] CONSOLE RESTARTED PID=$($new.Id)"
        $lastPid = $new.Id
        Start-Sleep -Seconds 8  # 给启动点时间
    }
    Start-Sleep -Seconds 60
}
