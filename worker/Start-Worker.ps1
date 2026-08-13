[CmdletBinding()]
param(
    [string]$ConfigPath = (Join-Path $PSScriptRoot 'config.json'),
    [string]$HostAddress = '0.0.0.0',
    [int]$Port = 6001,
    [switch]$SkipInstall
)

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$python = Join-Path $scriptDir '.venv\Scripts\python.exe'

if (-not (Test-Path -LiteralPath $ConfigPath)) {
    Copy-Item -LiteralPath (Join-Path $scriptDir 'config.example.json') -Destination $ConfigPath
    throw "Created $ConfigPath. Edit it with this node's PlatEMO and Master settings, then run this script again."
}

if (-not (Test-Path -LiteralPath $python)) {
    py -m venv (Join-Path $scriptDir '.venv')
}

if (-not $SkipInstall) {
    & $python -m pip install --disable-pip-version-check -r (Join-Path $scriptDir 'requirements.txt')
}

& $python -m app --config $ConfigPath --host $HostAddress --port $Port

