# Get environment variables
Get-Content .env | foreach {
    $name, $value = $_.split('=')
    if ([string]::IsNullOrWhiteSpace($name) -or $name.Contains('#')) {
      # skip empty or comment line in ENV file
      return
    }
    Set-Content env:\$name $value
  }

# Parameters 
$targetDirectory = $env:targetDirectory
$workspaceId = $env:workspaceId

# Get all folders that end with .Report
$reportFolders = Get-ChildItem -Path $targetDirectory -Directory | Where-Object { $_.Name -like "*.Report" }

# Remove .Report from report names
$reportFolders = $reportFolders | ForEach-Object { $_.Name -replace ".{7}$"}

# Download modules and install
New-Item -ItemType Directory -Path ".\modules" -ErrorAction SilentlyContinue | Out-Null
@("https://raw.githubusercontent.com/microsoft/Analysis-Services/master/pbidevmode/fabricps-pbip/FabricPS-PBIP.psm1"
, "https://raw.githubusercontent.com/microsoft/Analysis-Services/master/pbidevmode/fabricps-pbip/FabricPS-PBIP.psd1") |% {
    Invoke-WebRequest -Uri $_ -OutFile ".\modules\$(Split-Path $_ -Leaf)"
}
if(-not (Get-Module Az.Accounts -ListAvailable)) { 
    Install-Module Az.Accounts -Scope CurrentUser -Force
}
Import-Module ".\modules\FabricPS-PBIP" -Force


# Authenticate
$tenantId = $env:tenantId
$appId = $env:appId
$appSecret = $env:appSecret
Connect-PowerBIServiceAccount -ServicePrincipal -TenantId $tenantId -ClientId $appId -Credential (New-Object System.Management.Automation.PSCredential($appId, (ConvertTo-SecureString $appSecret -AsPlainText -Force)))

# Import reports
foreach ($report in $reportFolders) {
    $pbipSemanticModelPath = $targetDirectory + $report + ".SemanticModel"
    $pbipReportPath = $targetDirectory + $report + ".Report"
    
    # Import the semantic model and save the item id
    $semanticModelImport = Import-FabricItem -workspaceId $workspaceId -path $pbipSemanticModelPath

    # Import the report and ensure its binded to the previous imported report
    $reportImport = Import-FabricItem -workspaceId $workspaceId -path $pbipReportPath -itemProperties @{"semanticModelId" = $semanticModelImport.Id}
    }