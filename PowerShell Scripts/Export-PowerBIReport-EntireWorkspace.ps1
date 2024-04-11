# Connect to Power BI
Connect-PowerBIServiceAccount

$workspaceName = "BI WORKSPACE"
$workspace = Get-PowerBIWorkspace -Name $workspaceName

if($workspace) {
    Write-Host "The workspace named $workspaceName already exists."
    
    # Get all reports in the workspace
    $reports = Get-PowerBIReport -WorkspaceId $workspace.Id
    
    foreach ($report in $reports) {
        # Specify the full path where you want to store the exported PBIX file
        $fileName = $report.Name + ".pbix"
        $filePath = "C:\Users\Dkambo\DAVIS and SHIRTLIFF LTD\ICT - Power BI Backups\$fileName"
        
        # Check if the file already exists in the specified path
        if (Test-Path $filePath) {
            Write-Host "File $fileName already exists. Creating a unique file name..."
            $fileNameWithoutExtension = [System.IO.Path]::GetFileNameWithoutExtension($fileName)
            $fileExtension = [System.IO.Path]::GetExtension($fileName)
            $uniqueFileName = $fileNameWithoutExtension + "_" + (Get-Date -Format "yyyyMMdd_HHmmss") + $fileExtension
            $filePath = [System.IO.Path]::Combine([System.IO.Path]::GetDirectoryName($filePath), $uniqueFileName)
            Write-Host "Unique file name created: $uniqueFileName"
        }
        
        # Export the Power BI report and save it to the specified location
        Export-PowerBIReport -Id $report.Id -WorkspaceId $workspace.Id -OutFile $filePath -Verbose
        
        # If a unique file name was created, move or replace the original file with the unique one
        if ($uniqueFileName) {
            Move-Item -Path $filePath -Destination "C:\Users\Dkambo\DAVIS and SHIRTLIFF LTD\ICT - Power BI Backups\$fileName" -Force
            Write-Host "Unique file moved or replaced the original file."
        }
    }
}
else {
    Write-Host "The workspace named $workspaceName does not exist."
    return
}
