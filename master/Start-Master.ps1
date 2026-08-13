[CmdletBinding()]
param(
    [string]$DataDir = (Join-Path $PSScriptRoot 'Data'),
    [string]$HostAddress = '0.0.0.0',
    [int]$Port = 6000,
    [switch]$SkipInstall
)

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$python = Join-Path $scriptDir '.venv\Scripts\python.exe'
$requirements = Join-Path $scriptDir 'requirements.txt'
$installMarker = Join-Path $scriptDir '.venv\.requirements-installed'

if (-not (Test-Path -LiteralPath $python)) {
    py -m venv (Join-Path $scriptDir '.venv')
}

if (-not $SkipInstall -and -not (Test-Path -LiteralPath $installMarker)) {
    & $python -m pip install --disable-pip-version-check -r $requirements
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    New-Item -ItemType File -Force -Path $installMarker | Out-Null
}

New-Item -ItemType Directory -Force -Path $DataDir | Out-Null
& $python -m app --data-dir $DataDir --host $HostAddress --port $Port
