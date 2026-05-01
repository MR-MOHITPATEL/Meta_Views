# PowerShell Script to Setup Windows Task Scheduler for Meta Pipeline (Hourly Smart Sync)
# This script will automatically request Administrator privileges if needed.

if (-not ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "Requesting Administrator privileges..." -ForegroundColor Yellow
    Start-Process powershell.exe "-ExecutionPolicy Bypass -NoProfile -File `"$PSCommandPath`"" -Verb RunAs
    exit
}

$ProjectRoot = "c:\Users\Kashyap Vadapalli\Downloads\Meta Camp Daily Data\Meta_Views"
$WorkingDir  = "$ProjectRoot\meta_ads_raw_dump"
$RunScript   = "$WorkingDir\smart_sync.py"
$VenvPython  = "$ProjectRoot\.venv\Scripts\python.exe"

if (-not (Test-Path $VenvPython)) {
    Write-Host "Error: Virtual environment python not found at $VenvPython" -ForegroundColor Red
    exit
}

if (-not (Test-Path $RunScript)) {
    Write-Host "Error: smart_sync.py not found at $RunScript" -ForegroundColor Red
    exit
}

$TaskName = "Meta_Ads_Hourly_Smart_Sync"

Write-Host "Creating task: $TaskName"

$Action = New-ScheduledTaskAction -Execute $VenvPython -Argument "`"$RunScript`"" -WorkingDirectory $WorkingDir

# Create multiple daily triggers (every hour from 9 AM to 11 PM)
$Triggers = @()
for ($h = 9; $h -le 23; $h++) {
    $TimeStr = "$($h.ToString('D2')):00:00"
    $Triggers += New-ScheduledTaskTrigger -Daily -At $TimeStr
}

# StartWhenAvailable ensures it runs if the PC was off at 9 AM
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable

if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Triggers -Settings $Settings -Description "Automated Meta Ads Pipeline - runs every hour from 9 AM to midnight. Smart Sync logic handles Full vs Fast syncs."

Write-Host ""
Write-Host "Done! Task '$TaskName' registered successfully." -ForegroundColor Green
Write-Host "It will run hourly from 9 AM to midnight."
Write-Host "If the PC is turned off at 9 AM, it will automatically run as soon as it boots up."
Write-Host "Verify in Task Scheduler (taskschd.msc) under Task Scheduler Library."
