[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$SourcePlugin = Join-Path $ProjectRoot "c4d_plugin\Cinema 4D Agent\mcp_server_plugin.pyp"
$SourceIcon = Join-Path $ProjectRoot "c4d_plugin\Cinema 4D Agent\res\icon.png"
$Destination = "C:\Program Files\Maxon Cinema 4D 2026\plugins\Cinema 4D Agent"
$DestinationIconDir = Join-Path $Destination "res"

if (-not (Test-Path -LiteralPath $SourcePlugin)) {
    throw "Plugin source not found: $SourcePlugin"
}

New-Item -ItemType Directory -Path $DestinationIconDir -Force | Out-Null
Copy-Item -LiteralPath $SourcePlugin -Destination (Join-Path $Destination "mcp_server_plugin.pyp") -Force
if (Test-Path -LiteralPath $SourceIcon) {
    Copy-Item -LiteralPath $SourceIcon -Destination (Join-Path $DestinationIconDir "icon.png") -Force
}

$sourceHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $SourcePlugin).Hash
$installedHash = (Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $Destination "mcp_server_plugin.pyp")).Hash
if ($sourceHash -ne $installedHash) {
    throw "Installed plugin verification failed."
}

Write-Host "Cinema 4D Agent plugin installed successfully." -ForegroundColor Green
