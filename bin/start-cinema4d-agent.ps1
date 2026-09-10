[CmdletBinding()]
param(
    [ValidateSet("Start", "Status", "Stop", "Restart")]
    [string]$Action = "Start"
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$RuntimeDir = Join-Path $env:LOCALAPPDATA "Cinema 4D Agent\runtime"
$McpPort = 8790
$TunnelPort = 8787
$C4DPort = 5555
$McpUrl = "http://127.0.0.1:$McpPort/mcp"
$TunnelBaseUrl = "http://127.0.0.1:$TunnelPort"
$TunnelExe = Join-Path $ProjectRoot "bin\tunnel-client.exe"
$TunnelProfile = Join-Path $env:APPDATA "tunnel-client\cinema4d-local.yaml"
$Cinema4DExe = "C:\Program Files\Maxon Cinema 4D 2026\Cinema 4D.exe"

function Write-Step([string]$Message) {
    Write-Host "[Cinema 4D Agent] $Message" -ForegroundColor Cyan
}

function Write-Ok([string]$Message) {
    Write-Host "[OK] $Message" -ForegroundColor Green
}

function Write-Warn([string]$Message) {
    Write-Host "[WARN] $Message" -ForegroundColor Yellow
}

function Test-TcpPort([int]$Port) {
    $client = [System.Net.Sockets.TcpClient]::new()
    try {
        $connection = $client.BeginConnect("127.0.0.1", $Port, $null, $null)
        if (-not $connection.AsyncWaitHandle.WaitOne(750)) {
            return $false
        }
        $client.EndConnect($connection)
        return $true
    }
    catch {
        return $false
    }
    finally {
        $client.Dispose()
    }
}

function Wait-TcpPort([int]$Port, [int]$TimeoutSeconds) {
    $timer = [System.Diagnostics.Stopwatch]::StartNew()
    while ($timer.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        if (Test-TcpPort $Port) {
            return $true
        }
        Start-Sleep -Milliseconds 300
    }
    return $false
}

function Test-McpEndpoint {
    if (-not (Test-TcpPort $McpPort)) {
        return $false
    }

    $headers = @{
        Accept = "application/json, text/event-stream"
        "MCP-Protocol-Version" = "2025-03-26"
    }
    $body = @{
        jsonrpc = "2.0"
        id = 1
        method = "initialize"
        params = @{
            protocolVersion = "2025-03-26"
            capabilities = @{}
            clientInfo = @{ name = "cinema4d-agent-launcher"; version = "1.0" }
        }
    } | ConvertTo-Json -Depth 8 -Compress

    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri $McpUrl -Method Post `
            -Headers $headers -ContentType "application/json" -Body $body -TimeoutSec 5
        return $response.StatusCode -eq 200 -and $response.Content -match '"name":"Cinema4D"'
    }
    catch {
        return $false
    }
}

function Test-TunnelReady {
    if (-not (Test-TcpPort $TunnelPort)) {
        return $false
    }
    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri "$TunnelBaseUrl/readyz" -TimeoutSec 5
        return $response.StatusCode -eq 200 -and $response.Content.Trim() -eq "ready"
    }
    catch {
        return $false
    }
}

function Get-PortOwner([int]$Port) {
    Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue |
        Select-Object -First 1
}

function Stop-OwnedService([int]$Port, [string]$ExpectedProcess, [string]$Label) {
    $connection = Get-PortOwner $Port
    if (-not $connection) {
        Write-Warn "$Label 未运行。"
        return
    }

    $process = Get-Process -Id $connection.OwningProcess -ErrorAction SilentlyContinue
    if (-not $process -or $process.ProcessName -notmatch $ExpectedProcess) {
        throw "端口 $Port 被非预期进程占用，未自动停止。"
    }

    # A generic interpreter name does not identify the service it hosts.
    $details = Get-CimInstance Win32_Process -Filter "ProcessId = $($process.Id)"
    $commandLine = $details.CommandLine
    if ($Port -eq $McpPort) {
        $owned = $commandLine -match '(?i)(?:^|\s)-m\s+cinema4d_mcp(?:\s|$)' -and
            $commandLine -match '(?i)--transport\s+streamable-http(?:\s|$)' -and
            $commandLine -match "--port\s+$McpPort(?:\s|$)"
    }
    elseif ($Port -eq $TunnelPort) {
        $owned = $details.ExecutablePath -eq $TunnelExe -and
            $commandLine -match '(?i)--profile\s+cinema4d-local(?:\s|$)'
    }
    else { $owned = $false }
    if (-not $owned) {
        throw "端口 $Port 的进程身份无法确认，未自动停止。"
    }
    $process.Kill()
    Write-Ok "$Label 已停止。"
}

function Show-Status {
    $c4d = Test-TcpPort $C4DPort
    $mcp = Test-McpEndpoint
    $tunnel = Test-TunnelReady

    Write-Host ""
    Write-Host "连接状态" -ForegroundColor White
    Write-Host ("  C4D 插件   127.0.0.1:{0}  {1}" -f $C4DPort, $(if ($c4d) { "在线" } else { "离线" }))
    Write-Host ("  标准 MCP   127.0.0.1:{0}  {1}" -f $McpPort, $(if ($mcp) { "在线" } else { "离线" }))
    Write-Host ("  OpenAI Tunnel 127.0.0.1:{0}  {1}" -f $TunnelPort, $(if ($tunnel) { "就绪" } else { "未就绪" }))
    Write-Host ""

    return $c4d -and $mcp -and $tunnel
}

function Start-All {
    New-Item -ItemType Directory -Path $RuntimeDir -Force | Out-Null

    if (-not (Test-Path -LiteralPath $Cinema4DExe)) {
        throw "未找到 Cinema 4D：$Cinema4DExe"
    }
    if (-not (Get-Process "Cinema 4D" -ErrorAction SilentlyContinue)) {
        Write-Step "正在启动 Cinema 4D..."
        $env:C4D_AGENT_FORCE_START = "1"
        Start-Process -FilePath $Cinema4DExe | Out-Null
        Remove-Item Env:C4D_AGENT_FORCE_START -ErrorAction SilentlyContinue
    }
    if (Wait-TcpPort $C4DPort 45) {
        Write-Ok "C4D 插件服务已在线（$C4DPort）。"
    }
    else {
        Write-Warn "C4D 已打开，但插件服务 $C4DPort 尚未在线。"
        Write-Warn "请在 C4D 的“扩展 → Cinema 4D Agent”中点击 Start Server，并勾选随 C4D 启动。"
    }

    if (Test-TcpPort $McpPort) {
        if (-not (Test-McpEndpoint)) {
            throw "端口 $McpPort 已被占用，但响应的不是 Cinema 4D MCP。"
        }
        Write-Ok "标准 MCP 已在线（$McpPort）。"
    }
    else {
        $pythonCommand = (Get-Command python -ErrorAction Stop).Source
        $PythonExe = (& $pythonCommand -c "import sys; print(sys.executable)" | Select-Object -Last 1).Trim()
        & $PythonExe -c "import cinema4d_mcp" 2>$null
        if ($LASTEXITCODE -ne 0) {
            throw "Python 环境中未安装 cinema4d_mcp。请先在项目目录运行：python -m pip install -e ."
        }

        Write-Step "正在启动标准 MCP（$McpPort）..."
        Start-Process -FilePath $PythonExe `
            -ArgumentList @("-m", "cinema4d_mcp", "--transport", "streamable-http", "--host", "127.0.0.1", "--port", "$McpPort", "--path", "/mcp") `
            -WorkingDirectory $ProjectRoot -WindowStyle Hidden `
            -RedirectStandardOutput (Join-Path $RuntimeDir "mcp.stdout.log") `
            -RedirectStandardError (Join-Path $RuntimeDir "mcp.stderr.log") | Out-Null

        if (-not (Wait-TcpPort $McpPort 15) -or -not (Test-McpEndpoint)) {
            throw "标准 MCP 启动失败。请检查：$(Join-Path $RuntimeDir 'mcp.stderr.log')"
        }
        Write-Ok "标准 MCP 已在线（$McpPort）。"
    }

    if (-not (Test-Path -LiteralPath $TunnelExe)) {
        throw "未找到 Tunnel 客户端：$TunnelExe"
    }
    if (-not (Test-Path -LiteralPath $TunnelProfile)) {
        throw "未找到 Tunnel 配置：$TunnelProfile"
    }
    if ([string]::IsNullOrWhiteSpace($env:CONTROL_PLANE_API_KEY)) {
        $savedKey = [Environment]::GetEnvironmentVariable("CONTROL_PLANE_API_KEY", "User")
        if (-not [string]::IsNullOrWhiteSpace($savedKey)) {
            $env:CONTROL_PLANE_API_KEY = $savedKey
        }
    }
    if ([string]::IsNullOrWhiteSpace($env:CONTROL_PLANE_API_KEY)) {
        throw "未找到 CONTROL_PLANE_API_KEY 用户环境变量。"
    }

    if (Test-TcpPort $TunnelPort) {
        if (Test-TunnelReady) {
            Write-Ok "OpenAI Tunnel 已就绪（$TunnelPort）。"
        }
        else {
            Write-Step "Tunnel 状态异常，正在重新启动..."
            Stop-OwnedService $TunnelPort "tunnel-client" "OpenAI Tunnel"
        }
    }

    if (-not (Test-TcpPort $TunnelPort)) {
        Write-Step "正在启动 OpenAI Tunnel（$TunnelPort）..."
        Start-Process -FilePath $TunnelExe -ArgumentList @("run", "--profile", "cinema4d-local") `
            -WorkingDirectory $ProjectRoot -WindowStyle Hidden `
            -RedirectStandardOutput (Join-Path $RuntimeDir "tunnel.stdout.log") `
            -RedirectStandardError (Join-Path $RuntimeDir "tunnel.stderr.log") | Out-Null

        $timer = [System.Diagnostics.Stopwatch]::StartNew()
        while ($timer.Elapsed.TotalSeconds -lt 20 -and -not (Test-TunnelReady)) {
            Start-Sleep -Milliseconds 500
        }
        if (-not (Test-TunnelReady)) {
            throw "OpenAI Tunnel 启动后未就绪。请检查：$(Join-Path $RuntimeDir 'tunnel.stdout.log')"
        }
        Write-Ok "OpenAI Tunnel 已就绪（$TunnelPort）。"
    }

    if (-not (Show-Status)) {
        Write-Warn "MCP 与 Tunnel 已启动，但 C4D 插件仍需手动启动。"
        return $false
    }
    return $true
}

function Stop-All {
    Stop-OwnedService $TunnelPort "tunnel-client" "OpenAI Tunnel"
    Stop-OwnedService $McpPort "python" "标准 MCP"
    Write-Host "C4D 与其插件服务保持运行。" -ForegroundColor DarkGray
}

try {
    $success = $true
    switch ($Action) {
        "Start"   { $success = Start-All }
        "Status"  { $success = Show-Status }
        "Stop"    { Stop-All }
        "Restart" { Stop-All; $success = Start-All }
    }
    if (-not $success) { exit 2 }
}
catch {
    Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
