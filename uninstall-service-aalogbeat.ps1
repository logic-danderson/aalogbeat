# Delete and stop the service if it already exists.
if (Get-Service aalogbeat -ErrorAction SilentlyContinue) {
  $service = Get-WmiObject -Class Win32_Service -Filter "name='aalogbeat'"
  $service.StopService()
  Start-Sleep -s 1
  $service.delete()
}
