for ($i = 0; $i -lt 20; $i++) {
    Start-Sleep 3
    $r = Test-NetConnection -ComputerName localhost -Port 8000 -InformationLevel Quiet -WarningAction SilentlyContinue
    if ($r) { Write-Host "Port 8000 UP after $($i*3)s"; break }
    else { Write-Host "Waiting... $($i*3)s elapsed" }
}
