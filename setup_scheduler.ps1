# PowerShell Script to Setup Windows Task Scheduler for Meta Pipeline
# Run this script as Administrator.
# Schedules the Meta Ads pipeline at 9 AM, 2 PM, and 6 PM daily.

$ProjectRoot = "c:\Users\mahar\Documents\Projects\ZenJeevani\Master Data"
$WorkingDir  = "$ProjectRoot\meta_ads_raw_dump"
$RunScript   = "$WorkingDir\run_all.py"
$VenvPython  = "$ProjectRoot\.venv\Scripts\python.exe"

if (-not (Test-Path $VenvPython)) {
    Write-Host "Error: Virtual environment python not found at $VenvPython" -ForegroundColor Red
    Write-Host "Run: python -m venv .venv && .venv\Scripts\pip install -r requirements.txt"
    exit
}

if (-not (Test-Path $RunScript)) {
    Write-Host "Error: run_all.py not found at $RunScript" -ForegroundColor Red
    exit
}

# 9 AM, 2 PM, 6 PM IST
$Times = @("09:00:00", "14:00:00", "18:00:00")
$TaskBaseName = "Meta_Ads_Pipeline"

foreach ($Time in $Times) {
    $TaskName = "$TaskBaseName`_$($Time.Replace(':', ''))"
    Write-Host "Creating task: $TaskName at $Time"

    $Action   = New-ScheduledTaskAction -Execute $VenvPython -Argument "`"$RunScript`"" -WorkingDirectory $WorkingDir
    $Trigger  = New-ScheduledTaskTrigger -Daily -At $Time
    $Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable

    if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    }

    Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings `
        -Description "Automated Meta Ads Pipeline — runs at $Time"
}

Write-Host ""
Write-Host "Done! Tasks registered: 9 AM, 2 PM, 6 PM." -ForegroundColor Green
Write-Host "Verify in Task Scheduler (taskschd.msc) under Task Scheduler Library."
