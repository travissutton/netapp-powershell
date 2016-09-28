[CmdletBinding()]
    PARAM (

        [parameter(Mandatory=$true,ValueFromPipelineByPropertyName=$true)]
        [object]$Controller,
        
        [parameter(Mandatory=$true,ValueFromPipelineByPropertyName=$true)]
        [string]$Vserver,

        [parameter(Mandatory=$true,ValueFromPipelineByPropertyName=$true)]
        [string]$Path,

        [parameter(Mandatory=$false,ValueFromPipelineByPropertyName=$true)]
        [int]$MaxThreads=20

    )


BEGIN {

	if((Get-Module 'DataONTAP') -eq $null){
		try {
	        import-module 'DataONTAP' -ErrorAction Stop -DisableNameChecking
	    } catch {
	        Write-Error "Unable to import module DataONTAP. Please confirm this module is installed and accessible on this system."
	    }
	}


	$ISS = [system.management.automation.runspaces.initialsessionstate]::CreateDefault()
	$ISS.ImportPSModule("DataONTAP");
	$RunspacePool = [runspacefactory]::CreateRunspacePool(1, $MaxThreads, $ISS, $Host)
	$RunspacePool.Open()


	Function Invoke-PsRunSpaces {
	    
		    [cmdletbinding()]
		    PARAM (
	            [parameter(Mandatory=$true,ValueFromPipeline=$true,ValueFromPipelineByPropertyName=$true)]
		        [object]$RunspacePool,   
		        [parameter(Mandatory=$true,ValueFromPipeline=$true,ValueFromPipelineByPropertyName=$true)]
		        [object]$ScriptBlock,     
		        [parameter(Mandatory=$true,ValueFromPipeline=$true,ValueFromPipelineByPropertyName=$true)]
		        [object]$ObjectList,
		        [parameter(Mandatory=$false,ValueFromPipeline=$true,ValueFromPipelineByPropertyName=$true)]
		        [int]$MaxThreads = 20,
		        $InputParam = $Null,
		        [int]$SleepTimer = 100,
		        [int]$MaxResultTime = 120,
		        [HashTable]$AddParam = @{},
		        [Array]$AddSwitch = @(),
		        $addArgument = $null
		    )

		    BEGIN {
	            $ReturnVals = [hashtable]::Synchronized(@{})
	            $ReturnVals.Results = New-Object System.Collections.ArrayList
		        $Jobs = @()
		    }

		    PROCESS {
		        Write-Progress -Activity "Preloading threads" -Status "Starting Job $($jobs.count)"
		        ForEach ($Object in $ObjectList){
		            $PowershellThread = [powershell]::Create().AddScript($ScriptBlock)
	                $PowershellThread.AddArgument($ReturnVals) | out-null
		            If ($InputParam -ne $Null){ $PowershellThread.AddParameter($InputParam, $Object) | out-null }
		            Else{ $PowershellThread.AddArgument($Object) | out-null }
		            ForEach($Key in $AddParam.Keys){ $PowershellThread.AddParameter($Key, $AddParam.$key) | out-null }
		            ForEach($Switch in $AddSwitch){
		                $Switch
		                $PowershellThread.AddParameter($Switch) | out-null
		            }
		            if ($addArgument -ne $null){ $PowershellThread.AddArgument($addArgument) | out-null }
		            $PowershellThread.RunspacePool = $RunspacePool
		            $Handle = $PowershellThread.BeginInvoke()
		            $Job = "" | Select-Object Handle, Thread, object
		            $Job.Handle = $Handle
		            $Job.Thread = $PowershellThread
		            $Job.Object = $Object.ToString()
		            $Jobs += $Job
		        }
		    }

		    END {
		        $ResultTimer = Get-Date
		        While (@($Jobs | Where-Object {$_.Handle -ne $Null}).count -gt 0)  {
		            $Remaining = "$($($Jobs | Where-Object {$_.Handle.IsCompleted -eq $False}).object)"
		            If ($Remaining.Length -gt 60){ $Remaining = $Remaining.Substring(0,60) + "..." }
		            Write-Progress `
		                -Activity "Waiting for Jobs - $($MaxThreads - $($RunspacePool.GetAvailableRunspaces())) of $MaxThreads threads running" `
		                -PercentComplete (($Jobs.count - $($($Jobs | Where-Object {$_.Handle.IsCompleted -eq $False}).count)) / $Jobs.Count * 100) `
		                -Status "$(@($($Jobs | Where-Object {$_.Handle.IsCompleted -eq $False})).count) remaining - $remaining"
		 
		            ForEach ($Job in $($Jobs | Where-Object {$_.Handle.IsCompleted -eq $True})){
		                $Job.Thread.EndInvoke($Job.Handle)
		                $Job.Thread.Dispose()
		                $Job.Thread = $Null
		                $Job.Handle = $Null
		                $ResultTimer = Get-Date
		            }
		            If (($(Get-Date) - $ResultTimer).totalseconds -gt $MaxResultTime){
		                Write-Error "Child script appears to be frozen, try increasing MaxResultTime"
		                break;
		                
		            }
		            Start-Sleep -Milliseconds $SleepTimer
		        } 
		        
		        return $ReturnVals.Results
		    }
		}




	Function Get-NcVolDirectoryStatistics {
	[CmdletBinding()]
	    param (
	        [parameter(Mandatory=$true,ValueFromPipeline=$true,ValueFromPipelineByPropertyName=$true)]
		    [object]$RunspacePool,
	        [parameter(Mandatory=$true,ValueFromPipeline=$true,ValueFromPipelineByPropertyName=$true)]
	        [object]$Controller,
	        [parameter(Mandatory=$true,ValueFromPipeline=$true,ValueFromPipelineByPropertyName=$true)]
	        [string]$Vserver,
	        [parameter(Mandatory=$false,ValueFromPipeline=$true,ValueFromPipelineByPropertyName=$true)]
	        [string]$Path=$null,
	        [Parameter(Mandatory=$false,ValueFromPipeline=$true,ValueFromPipelineByPropertyName=$true)]
	        [array]$Directories
	    )

	    $DirListBlock = {
	        param($ReturnVals, $item, $argList)
		    $controller = $argList[0]
	        $svm = $argList[1]


	        $dirFilter = @(
	            "Name",
	            "Type",
	            "Size",
	            "Path",
	            "ModifiedTimestamp",
	            "AccessedTimestamp"
	        )

	        $files = Read-NcDirectory -Controller $controller -VserverContext $svm -Path $item.Path | Select-Object $dirFilter | ? {$_.Name -notlike ".*"}


	        $null = $ReturnVals.Results.AddRange($files)


	    }

	    $argList = @(
	        $Controller,
	        $Vserver
	    )
	    $Results = New-Object System.Collections.ArrayList

	    if ($Path) {
	    
	        $dirFilter = @(
	            "Name",
	            "Type",
	            "Size",
	            "Path",
	            "ModifiedTimestamp",
	            "AccessedTimestamp"
	        )

	        $start = Read-NcDirectory -Controller $Controller -VserverContext $Vserver -Path $Path | Select-Object $dirFilter | ? {$_.Name -notlike ".*"}
	        $null = $Results.AddRange($start)
	        $dirsToProcess = $start | ? { $_.Type -eq "Directory"}
	        if ($dirsToProcess.Count -gt 0) {
	            $rec= Get-NcVolDirectoryStatistics -RunspacePool $RunspacePool -Controller $controller -Vserver $Vserver -Directories $dirsToProcess
	            $null = $Results.AddRange($rec)
	        }
	    }

	    else {
	        if ($Directories.Count -gt 0) {
	            $dirlist = Invoke-PsRunSpaces -RunspacePool $RunspacePool -ScriptBlock $DirListBlock -ObjectList $Directories -addArgument $argList -MaxThreads 10
                if ($dirList -ne $null) {
	                $null = $Results.AddRange($dirlist)
	                $dirsToProcess = $dirlist | ? { $_.Type -eq "Directory" }
	                if ($dirsToProcess.Count -gt 0) {
	                    $rec= Get-NcVolDirectoryStatistics -RunspacePool $RunspacePool -Controller $controller -Vserver $Vserver -Directories $dirsToProcess
	                    if ($rec -ne $null) {
                            $null = $Results.AddRange($rec)
                        }
	                }
                }
	        }
	    }

	    
	    return $Results

	}

}

PROCESS {
	Measure-Command {
		Write-Host "Starting initial volume process." -ForegroundColor Cyan
	    $List = Get-NcVolDirectoryStatistics -RunspacePool $RunspacePool -Controller $controller -Vserver $Vserver -Path $Path
	}
}

END {
	Write-Host "Volume scan completed. Exporting results to CSV." -ForegroundColor Green
	$List | Export-Csv -Path "VolumeDirectoryInfo.csv" -NoTypeInformation
	return $List
}




