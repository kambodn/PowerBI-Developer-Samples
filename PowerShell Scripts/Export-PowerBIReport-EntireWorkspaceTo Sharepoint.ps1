Connect-PowerBIServiceAccount

$workspaceName = "BI WORKSPACE"
$workspace = Get-PowerBIWorkspace -Name $workspaceName

# SharePoint Variables
$SharePointSite = "https://your-sharepoint-site-url.com"
$SharePointFolder = "/sites/your-site/your-folder/"
$SharePointUsername = "your-username@your-domain.com"
$SharePointPassword = "your-password"

if($workspace) {
    Write-Host "The workspace named $workspaceName already exists."
    
    $reports = Get-PowerBIReport -WorkspaceId $workspace.Id
    
    # Connect to SharePoint
    Connect-PnPOnline -Url $SharePointSite -UseWebLogin
    $credentials = New-Object PSCredential ($SharePointUsername, (ConvertTo-SecureString $SharePointPassword -AsPlainText -Force))
    Connect-PnPOnline -Url $SharePointSite -Credentials $credentials
    
    foreach ($report in $reports) {
        $pbixFileName = "$($report.Name).pbix"
        $localFilePath = ".\$pbixFileName"
        
        Export-PowerBIReport -Id $report.Id -WorkspaceId $workspace.Id -OutFile $localFilePath -Verbose
        Write-Host "Report $($report.Name) downloaded."
        
        # Upload PBIX file to SharePoint
        $destinationUrl = "$SharePointSite$SharePointFolder$pbixFileName"
        Add-PnPFile -Path $localFilePath -Folder $SharePointFolder -FileName $pbixFileName -Verbose
        Write-Host "Report $($report.Name) uploaded to SharePoint at $destinationUrl."
        
        # Delete local PBIX file
        Remove-Item -Path $localFilePath -Force
        Write-Host "Local copy of $($report.Name) deleted."
    }
}
else {
    Write-Host "The workspace named $workspaceName does not exist."
    return
}
