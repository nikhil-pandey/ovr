# Installs ovr and ovrd from the latest release into %LOCALAPPDATA%\ovr\bin and adds it to the user PATH.
#
#   irm https://raw.githubusercontent.com/nikhil-pandey/ovr/main/install.ps1 | iex
$ErrorActionPreference = 'Stop'

$target = if ($env:PROCESSOR_ARCHITECTURE -eq 'ARM64') { 'aarch64-pc-windows-gnullvm' } else { 'x86_64-pc-windows-gnu' }
$url = "https://github.com/nikhil-pandey/ovr/releases/latest/download/ovr-$target.zip"
$dir = Join-Path $env:LOCALAPPDATA 'ovr\bin'
$zip = Join-Path ([IO.Path]::GetTempPath()) "ovr-$([guid]::NewGuid()).zip"

Invoke-WebRequest $url -OutFile $zip
Expand-Archive $zip -DestinationPath $dir -Force
Remove-Item $zip

$path = [Environment]::GetEnvironmentVariable('Path', 'User')
if (($path -split ';') -notcontains $dir) {
    [Environment]::SetEnvironmentVariable('Path', "$path;$dir", 'User')
}
Write-Host "installed ovr and ovrd to $dir"
