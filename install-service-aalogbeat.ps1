# Delete and stop the service if it already exists.
if (Get-Service aalogbeat -ErrorAction SilentlyContinue) {
  $service = Get-WmiObject -Class Win32_Service -Filter "name='aalogbeat'"
  $service.StopService()
  Start-Sleep -s 1
  $service.delete()
}

$workdir = Split-Path $MyInvocation.MyCommand.Path

# Create the new service.
New-Service -name aalogbeat `
  -displayName aalogbeat `
  -binaryPathName "`"$workdir\aalogbeat.exe`" -c `"$workdir\aalogbeat.yml`" -path.home `"$workdir`" -path.data `"C:\ProgramData\aalogbeat`" -path.logs `"C:\ProgramData\aalogbeat\logs`" -E logging.files.redirect_stderr=true"

# Attempt to set the service to delayed start using sc config.
Try {
  Start-Process -FilePath sc.exe -ArgumentList 'config aalogbeat start= delayed-auto'
}
Catch { Write-Host -f red "An error occured setting the service to delayed start." }
