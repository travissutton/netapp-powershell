# Overview

## Connect
```
Import-Module DataONTAP
$acme = Connect-NcController 10.10.120.15
```


## There's a lot of excess data out there
```
Get-NcVol -Controller $acme
$DS01 = Get-NcVol -Controller $acme -Name DS01
$DS01

$DS01 | fl
```


## How can we use this to answer a question? What Volumes have more than 10 snapshots?
```
Get-NcVol -Controller $acme | ? {$_.VolumeSnapshotAttributes.SnapshotCount -gt 10}
```


## Let's do something better
```
$date = (Get-Date).AddDays(-5)
$snapshots = Get-NcSnapshot -Controller $acme | ? {$_.Created -lt $date}
Get-NcSnapshot -Controller $acme | ? {$_.Created -lt $date} | Export-Csv -Path ./snapshots_raw.csv
```


## Too much excess data, we need to filter it.
```
$snapshotFilter = @(
    "Name","Volume","Vserver",
    @{n="Created";e={$_.AccessTimeDT.ToShortDateString()}},
    @{n="Total";e={[MATH]::Round(($_.Total / 1024 / 1024),2)}}
)

$snapshots | Select-Object $snapshotFilter | Export-Csv -Path ./snapshots.csv
```


## Polish up this a bit, send out an email?
```
Send-StorageReport -To "tsutton@ayetier.com" -Subject "Acme Snapshot Report" -Message "A 200GB snapshot from April 2015!!" -Attachment "snapshots.csv"
```
