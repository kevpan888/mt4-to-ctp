# 启动项目内置的 aq 网关（gateway/aqgw.py），账号/服务器从 config.ini 读取。
# 用法：.\start_gateway.ps1
#       .\start_gateway.ps1 -Symbols ru,ag -Restart

param(
    [string]$Symbols = '',
    [switch]$Restart
)

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$ini = Join-Path $root 'config.ini'
$gwDir = Join-Path $root 'gateway'
$gwPy = Join-Path $gwDir 'aqgw.py'

if (-not (Test-Path $gwPy)) {
    Write-Error "找不到网关程序：$gwPy"
    exit 1
}

# --- 读取 config.ini ---
$cfg = @{}
if (Test-Path $ini) {
    foreach ($line in Get-Content $ini -Encoding UTF8) {
        $t = $line.Split('#', 2)[0].Trim()
        if ($t -and $t.Contains('=')) {
            $kv = $t.Split('=', 2)
            $cfg[$kv[0].Trim().ToLower()] = $kv[1].Trim()
        }
    }
}

$port    = if ($cfg['gateway_port']) { [int]$cfg['gateway_port'] } else { 8222 }
$account = if ($cfg['gw_account']) { $cfg['gw_account'] } else { '<YOUR_ACCOUNT_ID>' }
$pass    = if ($cfg['gw_password']) { $cfg['gw_password'] } else { '<YOUR_GW_PASSWORD>' }
$server  = if ($cfg['gw_server']) { $cfg['gw_server'] } else { '117.72.80.248:443' }
$srvHost, $srvPort = $server.Split(':', 2)
if (-not $srvPort) { $srvPort = '443' }
if (-not $Symbols) { $Symbols = if ($cfg['gw_symbols']) { $cfg['gw_symbols'] } else { 'ru,ag' } }

# --- 重启时先停掉占用端口的旧进程 ---
if ($Restart) {
    Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" |
        Where-Object { $_.CommandLine -like '*aqgw.py*' } |
        ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
    Start-Sleep -Seconds 1
}

try {
    $h = Invoke-RestMethod -Uri ("http://127.0.0.1:{0}/health" -f $port) -TimeoutSec 3
    Write-Output ("网关已在运行（端口 {0}），健康: {1}" -f $port, ($h | ConvertTo-Json -Compress))
    exit 0
} catch { }

$args = @(
    'aqgw.py',
    '--symbols', $Symbols,
    '--login', $account,
    '--password', $pass,
    '--srv-host', $srvHost,
    '--srv-port', $srvPort,
    '--port', $port
)

$proc = Start-Process -FilePath 'python' -ArgumentList $args -WorkingDirectory $gwDir -PassThru
Write-Output ("网关启动中，PID={0}，上游 {1}，账号 {2}，端口 {3}" -f $proc.Id, $server, $account, $port)

for ($i = 0; $i -lt 15; $i++) {
    Start-Sleep -Seconds 1
    try {
        $h = Invoke-RestMethod -Uri ("http://127.0.0.1:{0}/health" -f $port) -TimeoutSec 3
        Write-Output ("健康检查通过: " + ($h | ConvertTo-Json -Compress))
        exit 0
    } catch { }
}
Write-Warning "网关启动超时，请查看窗口输出"
exit 1
