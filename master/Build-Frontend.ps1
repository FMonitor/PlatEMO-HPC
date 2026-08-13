[CmdletBinding()]
param(
    [switch]$SkipInstall
)

$ErrorActionPreference = 'Stop'
$frontendDir = Join-Path $PSScriptRoot 'frontend'
$nodeDir = Join-Path ${env:ProgramFiles} 'nodejs'
if (Test-Path -LiteralPath $nodeDir) {
    $env:Path = "$nodeDir;$env:Path"
}

if (-not (Get-Command npm -ErrorAction SilentlyContinue)) {
    throw 'Node.js/npm was not found. Install Node.js LTS, then run this script again.'
}

Push-Location -LiteralPath $frontendDir
try {
    if (-not $SkipInstall) {
        npm install
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }
    npm run build
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
finally {
    Pop-Location
}
