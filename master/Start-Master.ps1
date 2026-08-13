[CmdletBinding()]
param(
    [string]$DataDir = (Join-Path (Get-Location) 'Data'),
    [string]$HostAddress = '0.0.0.0',
    [int]$Port = 6000,
    [switch]$SkipInstall
)

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$python = Join-Path $scriptDir '.venv\Scripts\python.exe'

if (-not (Test-Path -LiteralPath $python)) {
    py -m venv (Join-Path $scriptDir '.venv')
}

if (-not $SkipInstall) {
    & $python -m pip install --disable-pip-version-check -r (Join-Path $scriptDir 'requirements.txt')
}

New-Item -ItemType Directory -Force -Path $DataDir | Out-Null
& $python -m app --data-dir $DataDir --host $HostAddress --port $Port

