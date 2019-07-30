$SourceStorageAccount = "abinavtestsa3"
$SourceStorageKey = "f/98U0nFBTK6+oLULvJvPrmiE8kW+8ATeQrEH5i9WF/iubyFc0E9dB9uK5/43QsKRFncdlnDjqhoBIzZjkyxDA=="
$SourceStorageContext = New-AzureStorageContext –StorageAccountName $SourceStorageAccount -StorageAccountKey $SourceStorageKey
$Prefix = "backup-"
$Containers = Get-AzureStorageContainer -Context $SourceStorageContext -Prefix $Prefix

#Delete all containers with prefix name "backup"
foreach ($Container in $Containers)
{
   Remove-AzureStorageContainer -Name $Container.Name -Context $SourceStorageContext -Force
}