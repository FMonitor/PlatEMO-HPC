[CmdletBinding()]
param(
    [string]$ConfigPath = (Join-Path $PSScriptRoot 'config.json'),
    [string]$HostAddress = '0.0.0.0',
    [int]$Port = 6001,
    [switch]$SkipInstall
)

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$workerVersion = 'dynamic-seed-session-2026.08.14'
Write-Host "[PlatEMO-HPC Worker] version=$workerVersion path=$scriptDir mode=starting" -ForegroundColor Cyan
$originalLocation = Get-Location
Set-Location -LiteralPath $scriptDir
$python = Join-Path $scriptDir '.venv\Scripts\python.exe'
$requirements = Join-Path $scriptDir 'requirements.txt'
$installMarker = Join-Path $scriptDir '.venv\.requirements-installed'

if (-not (Test-Path -LiteralPath $ConfigPath)) {
    Copy-Item -LiteralPath (Join-Path $scriptDir 'config.example.json') -Destination $ConfigPath
    throw "Created $ConfigPath. Edit it with this node's PlatEMO and Master settings, then run this script again."
}

if (-not (Test-Path -LiteralPath $python)) {
    py -m venv (Join-Path $scriptDir '.venv')
}

if (-not $SkipInstall -and -not (Test-Path -LiteralPath $installMarker)) {
    & $python -m pip install --disable-pip-version-check -r $requirements
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    New-Item -ItemType File -Force -Path $installMarker | Out-Null
}

& $python -m app --config $ConfigPath --host $HostAddress --port $Port
Set-Location -LiteralPath $originalLocation
