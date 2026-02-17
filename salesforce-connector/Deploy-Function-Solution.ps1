<#
.SYNOPSIS
    Automates multi-environment Azure Function deployment for Salesforce Service Cloud integration with Microsoft Sentinel using Data Collection Rules and managed identities.

.DESCRIPTION
    Deploy-Function-Solution.ps1 orchestrates the deployment of Azure Functions with Flex Consumption plans that ingest Salesforce EventLogFile data into Azure Sentinel. 
    
    Supports four deployment modes:
    - InfraOnly: Deploys Azure infrastructure (Function App, Key Vault, DCE, DCRs, Storage) without credentials
    - CredsOnly: Securely stores Salesforce OAuth credentials in Key Vault for specified environments
    - CodeOnly: Deploys/updates Function App Python code using Azure CLI with remote build
    - AddEnv: Dynamically adds new Salesforce environments to existing deployment with automatic backup/rollback
    
    The script provisions environment-specific Data Collection Rules, assigns managed identity RBAC permissions (Key Vault Secrets User, Monitoring Metrics Publisher, Storage roles), and configures multi-environment support with centralized credential management.

.EXAMPLE
    # Deploy infrastructure for two environments
    $envs = @(
        @{name="prod"; salesforceDomain="https://yourorg.my.salesforce.com"},
        @{name="preprod"; salesforceDomain="https://yourorg--preprod.my.salesforce.com"}
    )
    .\Deploy-Function-Solution.ps1 -Mode InfraOnly -ResourceGroupName "rg-sentinel" -WorkspaceName "law-sentinel" -Environments $envs

.EXAMPLE
    # Store credentials after infrastructure deployment
    .\Deploy-Function-Solution.ps1 -Mode CredsOnly -ResourceGroupName "rg-sentinel" -WorkspaceName "law-sentinel" -Environments $envs

.EXAMPLE
    # Deploy Function App code
    .\Deploy-Function-Solution.ps1 -Mode CodeOnly -ResourceGroupName "rg-sentinel" -WorkspaceName "law-sentinel"

.EXAMPLE
    # Add a new environment to existing deployment
    $newEnv = @(@{name="sandbox"; salesforceDomain="https://yourorg--sandbox.my.salesforce.com"})
    .\Deploy-Function-Solution.ps1 -Mode AddEnv -ResourceGroupName "rg-sentinel" -WorkspaceName "law-sentinel" -Environments $newEnv -KeyVaultName "kv-sf-abc123" -FunctionAppName "func-sf-abc123"

.NOTES
    Version: 1.0
    Requires: Azure PowerShell (Az module), Azure CLI (for CodeOnly mode)
    Permissions Required: Owner/Contributor on Resource Group, Key Vault Secrets Officer for credential operations
    
    Dependencies:
    - ARM template: salesforce-function-multienv.json
    - Function code: function_app.py, requirements.txt, host.json
#>


param(
    [Parameter(Mandatory = $true)]
    [string]$ResourceGroupName,

    [Parameter(Mandatory = $true)]
    [string]$WorkspaceName,

    [Parameter(Mandatory = $false)]
    [hashtable[]]$Environments,

    [Parameter(Mandatory = $false)]
    [string]$KeyVaultName,

    [Parameter(Mandatory = $false)]
    [string]$FunctionAppName,

    [Parameter(Mandatory = $false)]
    [bool]$CreateSharedResources = $true,

    [Parameter(Mandatory = $false)]
    [string]$Location = "northeurope",

    [Parameter(Mandatory = $true)]
    [ValidateSet("InfraOnly", "CredsOnly", "CodeOnly", "AddEnv")]
    [string]$Mode,

    [Parameter(Mandatory = $false)]
    [string]$FunctionCodePath = "."
)

function Sync-AzureContexts {
    $azContext = Get-AzContext
    $currentCliSub = az account show --query id -o tsv 2>$null
    
    if ($currentCliSub -ne $azContext.Subscription.Id) {
        Write-Host "⚠️  Synchronizing Azure CLI with PowerShell context..." -ForegroundColor Yellow
        az account set --subscription $azContext.Subscription.Id
        Write-Host "✅ Contexts synchronized" -ForegroundColor Green
    }
}

$ErrorActionPreference = "Stop"

# Validate subscription context consistency
$azContext = Get-AzContext -ErrorAction Stop
if (-not $azContext -or -not $azContext.Subscription) {
    Write-Host "❌ No Azure context found. Run Connect-AzAccount first." -ForegroundColor Red
    exit 1
}

$targetSubId = $azContext.Subscription.Id
$targetSubName = $azContext.Subscription.Name

Write-Host "Target Subscription: $targetSubName ($targetSubId)" -ForegroundColor Cyan

# Verify resource group exists in this subscription
$rgCheck = Get-AzResourceGroup -Name $ResourceGroupName -ErrorAction SilentlyContinue
if (-not $rgCheck) {
    Write-Host "❌ Resource group '$ResourceGroupName' not found in subscription '$targetSubName'" -ForegroundColor Red
    Write-Host "`n📋 To switch to a different subscription, run:" -ForegroundColor Yellow
    Write-Host "   Get-AzSubscription | Select-Object Name, Id, TenantId" -ForegroundColor White
    Write-Host "   Set-AzContext -SubscriptionId '<subscription-id>'" -ForegroundColor Cyan
    Write-Host "`nThen re-run this script.`n" -ForegroundColor Gray
    exit 1
}


# ================================================
# MODE: ADD ENVIRONMENT (DCR + ENV JSON + CREDS)
# ================================================
if ($Mode -eq "AddEnv") {
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host "ADD ENVIRONMENT MODE" -ForegroundColor Cyan
    Write-Host "========================================`n" -ForegroundColor Cyan

    # Validation
    if (-not $Environments -or $Environments.Count -eq 0) {
        Write-Host "❌ -Environments required" -ForegroundColor Red
        exit 1
    }
    if (-not $KeyVaultName -or -not $FunctionAppName) {
        Write-Host "❌ -KeyVaultName and -FunctionAppName required" -ForegroundColor Red
        exit 1
    }

    Sync-AzureContexts

    $subscriptionId = $targetSubId

    # [1/4] READ existing Env + DCE_ENDPOINT
    Write-Host "`n[1/4] Reading current ENVIRONMENTS_JSON + DCE_ENDPOINT..." -ForegroundColor Yellow
    
    $cliOutput = az functionapp config appsettings list `
        --resource-group $ResourceGroupName --name $FunctionAppName `
        --query "[?name=='ENVIRONMENTS_JSON'].value" --output tsv 2>$null
    
    if ($LASTEXITCODE -eq 0 -and $cliOutput -and $cliOutput.Trim()) {
        $existingJson = $cliOutput.Trim()
        Write-Host "  📖 Found ENVIRONMENTS_JSON ($($existingJson.Length) chars): $($existingJson.Substring(0,[Math]::Min(80,$existingJson.Length)))..."
    }
    else {
        $existingJson = $null
        Write-Host "  📭 No existing ENVIRONMENTS_JSON found"
    }

    $dceCliOutput = az functionapp config appsettings list `
        --resource-group $ResourceGroupName --name $FunctionAppName `
        --query "[?name=='DCE_ENDPOINT'].value" --output tsv 2>$null
    
    if ($LASTEXITCODE -eq 0 -and $dceCliOutput -and $dceCliOutput.Trim()) {
        $dceEndpoint = $dceCliOutput.Trim()
        Write-Host "  🔗 Found DCE_ENDPOINT: $($dceEndpoint.Substring(0,[Math]::Min(40,$dceEndpoint.Length)))..."
    }
    else {
        $dceEndpoint = $null
        Write-Host "  ⚠️ No DCE_ENDPOINT found"
    }

    # Parse safely
    if ([string]::IsNullOrWhiteSpace($existingJson)) {
        $existingEnvs = @()
    }
    else {
        try {
            $existingEnvs = $existingJson | ConvertFrom-Json
            if ($existingEnvs -isnot [System.Collections.IEnumerable]) { $existingEnvs = @($existingEnvs) }
        }
        catch {
            Write-Host "  ⚠️ Invalid JSON – starting fresh" -ForegroundColor Yellow
            $existingEnvs = @()
        }
    }

    $existingNames = $existingEnvs | ForEach-Object { $_.name }
    Write-Host "  📋 Existing: $($existingNames -join ', ')"

    # BACKUP before ARM (safety!)
    $backupsCreated = @()
    if ($existingJson) {
        az functionapp config appsettings set --resource-group $ResourceGroupName --name $FunctionAppName `
            --settings "ENVIRONMENTS_JSON_BACKUP=$existingJson" 2>$null | Out-Null
        Write-Host "  💾 Backup created: ENVIRONMENTS_JSON_BACKUP" -ForegroundColor Gray
        $backupsCreated += "ENVIRONMENTS_JSON_BACKUP"
    }
    if ($dceEndpoint) {
        az functionapp config appsettings set --resource-group $ResourceGroupName --name $FunctionAppName `
            --settings "DCE_ENDPOINT_BACKUP=$dceEndpoint" 2>$null | Out-Null
        Write-Host "  💾 Backup created: DCE_ENDPOINT_BACKUP" -ForegroundColor Gray
        $backupsCreated += "DCE_ENDPOINT_BACKUP"
    }

    $updatedEnvs = @($existingEnvs)  # Copy old ones
    $newlyAdded = @()

    # [2/4] Deploy NEW DCRs
    Write-Host "`n[2/4] Deploying DCRs for new environments..." -ForegroundColor Yellow

    $deploymentParams = @{
        ResourceGroupName     = $ResourceGroupName
        TemplateFile          = ".\salesforce-function-multienv.json"
        workspaceName         = $WorkspaceName
        location              = $Location
        environments          = $Environments
        createSharedResources = $false
        Verbose               = $true
    }
    $deploymentParams.keyVaultName = $KeyVaultName
    $deploymentParams.functionAppName = $FunctionAppName

    try {
        $deployment = New-AzResourceGroupDeployment @deploymentParams
        if (-not $?) {
            throw "ARM deployment failed"
        }
        Write-Host "✅ DCRs deployed successfully" -ForegroundColor Green
    }
    catch {
        Write-Host "❌ ARM deployment failed: $($_.Exception.Message)" -ForegroundColor Red
        
        # RESTORE from backup
        if ($existingJson) {
            Write-Host "🔄 Restoring ENVIRONMENTS_JSON from backup..." -ForegroundColor Yellow
            $restoreCmd = az functionapp config appsettings set `
                --resource-group $ResourceGroupName --name $FunctionAppName `
                --settings "ENVIRONMENTS_JSON=$existingJson" 2>&1
            
            if ($LASTEXITCODE -eq 0) {
                Write-Host "✅ ENVIRONMENTS_JSON restored from backup" -ForegroundColor Green
            }
            else {
                Write-Host "⚠️ ENVIRONMENTS_JSON restore failed: $restoreCmd" -ForegroundColor Yellow
            }
        }
        
        # NEW: Restore DCE_ENDPOINT
        $dceRestoreCli = az functionapp config appsettings list `
            --resource-group $ResourceGroupName --name $FunctionAppName `
            --query "[?name=='DCE_ENDPOINT_BACKUP'].value" --output tsv 2>$null
        
        if ($LASTEXITCODE -eq 0 -and $dceRestoreCli -and $dceRestoreCli.Trim()) {
            $dceRestoreValue = $dceRestoreCli.Trim()
            $dceRestoreCmd = az functionapp config appsettings set `
                --resource-group $ResourceGroupName --name $FunctionAppName `
                --settings "DCE_ENDPOINT=$dceRestoreValue" 2>&1
            
            if ($LASTEXITCODE -eq 0) {
                Write-Host "✅ DCE_ENDPOINT restored from backup" -ForegroundColor Green
            }
            else {
                Write-Host "⚠️ DCE_ENDPOINT restore failed: $dceRestoreCmd" -ForegroundColor Yellow
            }
        }
        elseif ($dceEndpoint) {
            Write-Host "⚠️ DCE_ENDPOINT_BACKUP not found for restore" -ForegroundColor Yellow
        }
        
        # Clean up backups
        if ($backupsCreated.Count -gt 0) {
            az functionapp config appsettings delete `
                --resource-group $ResourceGroupName --name $FunctionAppName `
                --setting-names ($backupsCreated -join ",") 2>$null | Out-Null
            Write-Host "🧹 Backup settings deleted: $($backupsCreated -join ', ')" -ForegroundColor Gray
        }
        
        Write-Host "💥 Script aborted - environment preserved (ENV_JSON + DCE)" -ForegroundColor Red
        exit 1
    }

    # [3/4] Get new DCR IDs + MERGE
    Write-Host "`n[3/4] Adding new DCRs to configuration..." -ForegroundColor Yellow

    foreach ($env in $Environments) {
        $envName = $env.name
        if ($existingNames -contains $envName) {
            Write-Host "  ⚠️ '$envName' exists – skipping" -ForegroundColor Yellow
            continue
        }

        $dcrName = "SalesforceServiceCloud-$envName-DCR"
        try {
            $dcr = Get-AzDataCollectionRule -ResourceGroupName $ResourceGroupName -Name $dcrName -ErrorAction Stop
            $dcrImmutableId = $dcr.ImmutableId

            $updatedEnvs += [PSCustomObject]@{
                name             = $envName
                salesforceDomain = $env.salesforceDomain
                dcrImmutableId   = $dcrImmutableId
            }
            $newlyAdded += $envName
            Write-Host "  ✅ +$envName (DCR: $($dcrImmutableId.Substring(0,16))...)" -ForegroundColor Green
        }
        catch {
            Write-Host "  ❌ DCR $dcrName : $($_.Exception.Message)" -ForegroundColor Red
            exit 1
        }
    }

    if ($newlyAdded.Count -gt 0) {
        $envJson = ($updatedEnvs | ConvertTo-Json -Depth 10 -Compress)
        Write-Host "  📝 Writing $($updatedEnvs.Count) total envs..."

        $setResult = az functionapp config appsettings set `
            --resource-group $ResourceGroupName --name $FunctionAppName `
            --settings "ENVIRONMENTS_JSON=$envJson" 2>&1

        if ($LASTEXITCODE -eq 0) {
            Write-Host "✅ ENVIRONMENTS_JSON → $($updatedEnvs.Count) envs (old+$($newlyAdded.Count))" -ForegroundColor Green
        }
        else {
            Write-Host "❌ Write failed: $setResult" -ForegroundColor Red
            exit 1
        }
    }
    else {
        Write-Host "ℹ️ No new envs added" -ForegroundColor Cyan
    }

    # [4/4] Credentials (new only)  
    Write-Host "`n[4/4] Credentials for new envs..." -ForegroundColor Yellow
    foreach ($env in $Environments) {
        $envName = $env.name
        if ($existingNames -contains $envName) {
            Write-Host "  ℹ️ '$envName' creds exist – skip" -ForegroundColor Cyan
            continue
        }

        Write-Host "`n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
        Write-Host "$envName → $($env.salesforceDomain)" -ForegroundColor Cyan
        Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray

        $clientId = Read-Host "  Consumer Key" -AsSecureString
        $clientSecret = Read-Host "  Consumer Secret" -AsSecureString

        $secretPrefix = "sf-$envName"
        try {
            Set-AzKeyVaultSecret -VaultName $KeyVaultName -Name "$secretPrefix-clientid" -SecretValue $clientId -ErrorAction Stop | Out-Null
            Set-AzKeyVaultSecret -VaultName $KeyVaultName -Name "$secretPrefix-clientsecret" -SecretValue $clientSecret -ErrorAction Stop | Out-Null
            Write-Host "  ✅ $envName creds stored" -ForegroundColor Green
        }
        catch {
            Write-Host "  ❌ $($_.Exception.Message)" -ForegroundColor Red
            exit 1
        }
    }

    Write-Host "`n✅ COMPLETE | New: $($newlyAdded -join ', ') | Total: $($updatedEnvs.Count)" -ForegroundColor Green
    Write-Host "Next Function timer auto-processes everything!" -ForegroundColor Cyan
    exit 0
}


# ================================================
# MODE: CREDENTIALS ONLY
# ================================================

if ($Mode -eq "CredsOnly") {
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host "CREDENTIALS STORAGE MODE" -ForegroundColor Cyan
    Write-Host "========================================`n" -ForegroundColor Cyan

    Write-Host "Resource Group: $ResourceGroupName" -ForegroundColor Gray
    Write-Host "Workspace: $WorkspaceName`n" -ForegroundColor Gray

    Sync-AzureContexts

    # Find Key Vault
    Write-Host "Looking for Key Vault..." -ForegroundColor Yellow

    if ($KeyVaultName) {
        $keyVaultName = $KeyVaultName
        Write-Host "Using specified Key Vault: $keyVaultName" -ForegroundColor Cyan
    }
    else {
        $keyVaults = Get-AzKeyVault -ResourceGroupName $ResourceGroupName | Where-Object { $_.VaultName -like "kv-sf-*" }

        if ($keyVaults.Count -eq 0) {
            Write-Host "❌ No Key Vault found in resource group $ResourceGroupName" -ForegroundColor Red
            exit 1
        }

        $keyVaultName = $keyVaults[0].VaultName
        Write-Host "✅ Found Key Vault: $keyVaultName" -ForegroundColor Green
    }

    # Test access
    Write-Host "`nTesting Key Vault access..." -ForegroundColor Yellow
    try {
        Get-AzKeyVault -VaultName $keyVaultName -ErrorAction Stop | Out-Null
        Write-Host "✅ You have access to Key Vault!" -ForegroundColor Green
    }
    catch {
        Write-Host "❌ You don't have access to Key Vault yet" -ForegroundColor Red
        Write-Host "`n📋 Grant yourself permissions in Portal:" -ForegroundColor Yellow
        Write-Host "1. Azure Portal > Key Vaults > $keyVaultName" -ForegroundColor White
        Write-Host "2. Access control (IAM) > Add role assignment" -ForegroundColor White
        Write-Host "3. Role: Key Vault Secrets Officer" -ForegroundColor White
        Write-Host "4. Members: Select your account" -ForegroundColor White
        Write-Host "5. Review + assign" -ForegroundColor White
        Write-Host "6. Wait 2-3 minutes, then run this script again`n" -ForegroundColor White
        exit 1
    }

    # Get environments from parameter or user input
    if (-not $Environments) {
        Write-Host "`nPlease specify environments using -Environments parameter" -ForegroundColor Yellow
        Write-Host "Example:" -ForegroundColor Cyan
        Write-Host @"
`$environments = @(
    @{
        name = "prod"
        salesforceDomain = "https://yourorg.my.salesforce.com"
    },
    @{
        name = "preProd"
        salesforceDomain = "https://yourorg--preprod.my.salesforce.com"
    }
)

.\Deploy-Function-Solution.ps1 ``
    -Mode CredsOnly ``
    -ResourceGroupName "$ResourceGroupName" ``
    -WorkspaceName "$WorkspaceName" ``
    -KeyVaultName "$keyVaultName" ``
    -Environments `$environments
"@ -ForegroundColor White
        Write-Host ""
        exit 1
    }

    Write-Host "`nEnvironments to configure:" -ForegroundColor Cyan
    foreach ($env in $Environments) {
        Write-Host "  - $($env.name): $($env.salesforceDomain)" -ForegroundColor White
    }
    Write-Host ""

    # Store credentials for each environment
    Write-Host "Please enter Salesforce OAuth credentials for each environment`n" -ForegroundColor Yellow

    $successCount = 0
    $failedEnvs = @()

    foreach ($env in $Environments) {
        $envName = $env.name

        Write-Host "`n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
        Write-Host "Environment: $envName" -ForegroundColor Cyan
        Write-Host "Domain: $($env.salesforceDomain)" -ForegroundColor Gray
        Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray

        $clientId = Read-Host "`n  Consumer Key (Client ID)" -AsSecureString
        $clientSecret = Read-Host "  Consumer Secret" -AsSecureString

        $secretPrefix = "sf-$envName"

        Write-Host "`n  Storing secrets..." -ForegroundColor Gray

        try {
            Set-AzKeyVaultSecret `
                -VaultName $keyVaultName `
                -Name "$secretPrefix-clientid" `
                -SecretValue $clientId `
                -ErrorAction Stop | Out-Null

            Set-AzKeyVaultSecret `
                -VaultName $keyVaultName `
                -Name "$secretPrefix-clientsecret" `
                -SecretValue $clientSecret `
                -ErrorAction Stop | Out-Null

            Write-Host "  ✅ Credentials stored successfully!" -ForegroundColor Green
            $successCount++

        }
        catch {
            Write-Host "  ❌ Failed to store credentials" -ForegroundColor Red
            Write-Host "  Error: $($_.Exception.Message)" -ForegroundColor Red
            $failedEnvs += $envName
        }
    }

    # Summary
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host "CREDENTIALS STORAGE COMPLETE" -ForegroundColor Cyan
    Write-Host "========================================`n" -ForegroundColor Cyan

    if ($successCount -eq $Environments.Count) {
        Write-Host "✅ All credentials stored successfully! ($successCount/$($Environments.Count))" -ForegroundColor Green
    }
    else {
        Write-Host "⚠️  Stored $successCount/$($Environments.Count) credentials" -ForegroundColor Yellow
        if ($failedEnvs.Count -gt 0) {
            Write-Host "❌ Failed for: $($failedEnvs -join ', ')" -ForegroundColor Red
        }
    }

    exit 0
}

# ================================================
# MODE: CODE ONLY (Deploy/Update Function Code)
# ================================================
 
if ($Mode -eq "CodeOnly") {
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host "FUNCTION CODE DEPLOYMENT MODE" -ForegroundColor Cyan
    Write-Host "========================================`n" -ForegroundColor Cyan
 
    # Find Function App
    Write-Host "[1/4] Looking for Function App..." -ForegroundColor Yellow
 
    if ($FunctionAppName) {
        $functionAppName = $FunctionAppName
    }
    else {
        $functionApps = Get-AzFunctionApp -ResourceGroupName $ResourceGroupName | Where-Object { $_.Name -like "func-sf-*" }
 
        if ($functionApps.Count -eq 0) {
            Write-Host "❌ No Function App found in resource group $ResourceGroupName" -ForegroundColor Red
            exit 1
        }
 
        $functionAppName = $functionApps[0].Name
    }
 
    Write-Host "✅ Found Function App: $functionAppName" -ForegroundColor Green
 
    # Verify Azure CLI is available (REQUIRED for Flex Consumption remote build)
    Write-Host "`n[2/4] Verifying Azure CLI..." -ForegroundColor Yellow
 
    try {
        $azVersion = az version --output json 2>$null | ConvertFrom-Json
        Write-Host "✅ Azure CLI version: $($azVersion.'azure-cli')" -ForegroundColor Green
    }
    catch {
        Write-Host "❌ Azure CLI is required for Flex Consumption deployment!" -ForegroundColor Red
        Write-Host "`nFlex Consumption requires remote build with --build-remote flag." -ForegroundColor Yellow
        Write-Host "This flag is only available in Azure CLI, not PowerShell cmdlets.`n" -ForegroundColor Yellow
        Write-Host "Install Azure CLI:" -ForegroundColor Cyan
        Write-Host "  Windows: https://aka.ms/installazurecli" -ForegroundColor White
        Write-Host "  Or run: winget install Microsoft.AzureCLI`n" -ForegroundColor White
        Write-Host "After installation, run: az login`n" -ForegroundColor White
        exit 1
    }
 
    # Ensure logged into Azure CLI
    $accountInfo = az account show 2>$null
    if (-not $accountInfo) {
        Write-Host "Logging into Azure CLI..." -ForegroundColor Yellow
        az login
 
        if ($LASTEXITCODE -ne 0) {
            Write-Host "❌ Azure CLI login failed" -ForegroundColor Red
            exit 1
        }
    }
 
    Write-Host "✅ Logged in to Azure" -ForegroundColor Green
 
    Sync-AzureContexts
 
    # Verify function code path
    Write-Host "`n[3/4] Verifying function code..." -ForegroundColor Yellow
 
    if (-not (Test-Path $FunctionCodePath)) {
        Write-Host "❌ Function code path not found: $FunctionCodePath" -ForegroundColor Red
        exit 1
    }
 
    $requiredFiles = @(
        "function_app.py",
        "requirements.txt",
        "host.json"
    )
 
    foreach ($file in $requiredFiles) {
        $filePath = Join-Path $FunctionCodePath $file
        if (Test-Path $filePath) {
            Write-Host "  ✅ $file" -ForegroundColor Green
        }
        else {
            Write-Host "  ❌ Missing: $file" -ForegroundColor Red
            exit 1
        }
    }
 
    # Create deployment package and deploy
    Write-Host "`n[4/4] Creating package and deploying..." -ForegroundColor Yellow
 
    $timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $zipFile = "function-package-$timestamp.zip"
 
    Push-Location $FunctionCodePath
 
    try {
        # Remove old zip files
        Get-ChildItem -Filter "function-package-*.zip" | Remove-Item -Force -ErrorAction SilentlyContinue
 
        # Create zip package
        Write-Host "  Creating deployment package..." -ForegroundColor Gray
        $filesToZip = Get-ChildItem -Exclude "*.zip", "local.settings.json", ".venv", "__pycache__", "*.pyc", ".git", ".gitignore", ".vscode"
        Compress-Archive -Path $filesToZip -DestinationPath $zipFile -Force
 
        $zipPath = Resolve-Path $zipFile
        $zipSizeMB = [math]::Round((Get-Item $zipPath).Length / 1MB, 2)
 
        Write-Host "  ✅ Package: $zipFile ($zipSizeMB MB)" -ForegroundColor Green
 
        # Verify deployment target before proceeding
        Write-Host "`n  Verifying deployment target..." -ForegroundColor Cyan
        $currentSubName = az account show --query name -o tsv
        $rgCheck = az group show --name $ResourceGroupName --query id -o tsv 2>$null
 
        if (-not $rgCheck) {
            Write-Host "  ❌ Resource group '$ResourceGroupName' not found in current subscription" -ForegroundColor Red
            Write-Host "     Current subscription: $currentSubName" -ForegroundColor Gray
            Pop-Location
            exit 1
        }
 
        Write-Host "  ✅ Subscription: $currentSubName" -ForegroundColor Green
        Write-Host "  ✅ Resource Group: $ResourceGroupName" -ForegroundColor Green
 
        # Deploy with Azure CLI and remote build
        Write-Host "`n  Deploying with remote build..." -ForegroundColor Cyan
        Write-Host "  This will install packages from requirements.txt in Azure" -ForegroundColor Gray
        Write-Host "  (Build process takes 5-10 minutes...)`n" -ForegroundColor Gray
 
        az functionapp deployment source config-zip `
            --resource-group $ResourceGroupName `
            --name $functionAppName `
            --src $zipPath `
            --build-remote true `
            --timeout 900
 
        if ($LASTEXITCODE -eq 0) {
            Write-Host "`n✅ Deployment completed successfully!" -ForegroundColor Green
        }
        else {
            throw "Deployment failed with exit code $LASTEXITCODE"
        }
 
        Write-Host "`n========================================" -ForegroundColor Cyan
        Write-Host "CODE DEPLOYMENT COMPLETE" -ForegroundColor Cyan
        Write-Host "========================================`n" -ForegroundColor Cyan
 
        Write-Host "📋 Deployment Details:" -ForegroundColor Cyan
        Write-Host "  Function App: $functionAppName" -ForegroundColor White
        Write-Host "  Package: $zipFile ($zipSizeMB MB)" -ForegroundColor White
        Write-Host "  Method: Azure CLI with remote build" -ForegroundColor White
 
        Write-Host "`n⏱️  Remote build is running in Azure..." -ForegroundColor Yellow
        Write-Host "  - Installing packages from requirements.txt" -ForegroundColor Gray
        Write-Host "  - This takes 5-10 minutes to complete" -ForegroundColor Gray
        Write-Host "  - Functions will appear after build finishes" -ForegroundColor Gray
 
        Write-Host "`n📋 After build completes (in ~5-10 minutes):" -ForegroundColor Cyan
 
        Write-Host "`n  1. Check functions appear:" -ForegroundColor Yellow
        Write-Host "     Portal > Function App > $functionAppName > Functions" -ForegroundColor White
        Write-Host "     Should see: SalesforceToSentinel" -ForegroundColor Gray
 
        Write-Host "`n  2. Monitor execution:" -ForegroundColor Yellow
        Write-Host "     Portal > Function App > $functionAppName > Monitor > Invocations" -ForegroundColor White
 
        Write-Host "`n  3. Check Application Insights:" -ForegroundColor Yellow
        Write-Host "     traces | where message contains 'Salesforce' | order by timestamp desc" -ForegroundColor White
 
        Write-Host "`n  4. Verify data ingestion:" -ForegroundColor Yellow
        Write-Host "     SalesforceServiceCloudV2_CL | where TimeGenerated > ago(1h)" -ForegroundColor White
 
        Write-Host ""
 
    }
    catch {
        Write-Host "`n❌ Deployment failed: $($_.Exception.Message)" -ForegroundColor Red
 
        Write-Host "`n⚠️  Troubleshooting:" -ForegroundColor Yellow
        Write-Host "1. Check deployment logs:" -ForegroundColor White
        Write-Host "   Portal > Function App > $functionAppName > Deployment Center > Logs" -ForegroundColor Gray
 
        Write-Host "`n2. Verify permissions:" -ForegroundColor White
        Write-Host "   Portal > Function App > $functionAppName > Access Control (IAM)" -ForegroundColor Gray
        Write-Host "   Required: Contributor or Website Contributor role" -ForegroundColor Gray
 
        Write-Host "`n3. Check Function App status:" -ForegroundColor White
        Write-Host "   Portal > Function App > $functionAppName > Overview" -ForegroundColor Gray
        Write-Host "   Status should be: Running" -ForegroundColor Gray
 
        Write-Host "`n4. Verify Azure CLI authentication:" -ForegroundColor White
        Write-Host "   Run: az account show" -ForegroundColor Gray
        Write-Host "   Ensure you're logged into the correct subscription" -ForegroundColor Gray
 
        Pop-Location
        exit 1
    }
    finally {
        Pop-Location
    }
 
    exit 0
}

# ================================================
# MODE: INFRASTRUCTURE ONLY
# ================================================

Write-Host "`n========================================" -ForegroundColor Cyan
if ($Mode -eq "InfraOnly") {
    Write-Host "INFRASTRUCTURE DEPLOYMENT (NO CREDENTIALS)" -ForegroundColor Cyan
}
Write-Host "========================================`n" -ForegroundColor Cyan

Sync-AzureContexts

Write-Host "Resource Group: $ResourceGroupName" -ForegroundColor Gray
Write-Host "Workspace: $WorkspaceName" -ForegroundColor Gray
if ($KeyVaultName) {
    Write-Host "Key Vault: $KeyVaultName (custom)" -ForegroundColor Gray
}
else {
    Write-Host "Key Vault: Auto-generated (kv-sf-...)" -ForegroundColor Gray
}
if ($FunctionAppName) {
    Write-Host "Function App: $FunctionAppName (custom)" -ForegroundColor Gray
}
else {
    Write-Host "Function App: Auto-generated (func-sf-...)" -ForegroundColor Gray
}
Write-Host "Environments: $($Environments.Count)" -ForegroundColor Gray
foreach ($env in $Environments) {
    Write-Host "  - $($env.name): $($env.salesforceDomain)" -ForegroundColor White
}
Write-Host ""

# ================================================
# STEP 1: DEPLOY INFRASTRUCTURE
# ================================================

Write-Host "`n[1/5] Deploying infrastructure for $($Environments.Count) environments..." -ForegroundColor Yellow

$deploymentParams = @{
    ResourceGroupName     = $ResourceGroupName
    TemplateFile          = ".\salesforce-function-multienv.json"
    workspaceName         = $WorkspaceName
    location              = $Location
    environments          = $Environments
    createSharedResources = $CreateSharedResources
    Verbose               = $true
}

# Add custom names if specified
if ($KeyVaultName) {
    $deploymentParams.Add("keyVaultName", $KeyVaultName)
    Write-Host "Using custom Key Vault name: $KeyVaultName" -ForegroundColor Cyan
}

if ($FunctionAppName) {
    $deploymentParams.Add("functionAppName", $FunctionAppName)
    Write-Host "Using custom Function App name: $FunctionAppName" -ForegroundColor Cyan
}

$deployment = New-AzResourceGroupDeployment @deploymentParams

if (-not $?) {
    Write-Host "`n❌ DEPLOYMENT FAILED" -ForegroundColor Red
    exit 1
}

# Extract deployment outputs
$keyVaultName = $deployment.Outputs.keyVaultName.Value
$dceName = $deployment.Outputs.dceName.Value
$dceEndpoint = $deployment.Outputs.dceEndpoint.Value
$dcrDetails = $deployment.Outputs.dcrDetails.Value
$functionAppName = $deployment.Outputs.functionAppName.Value
$functionAppPrincipalId = $deployment.Outputs.functionAppPrincipalId.Value

Write-Host "✅ Infrastructure deployed successfully!" -ForegroundColor Green
Write-Host "   Key Vault: $keyVaultName" -ForegroundColor White
Write-Host "   DCE: $dceName" -ForegroundColor White
Write-Host "   Function App: $functionAppName" -ForegroundColor White
Write-Host "   DCRs created: $($dcrDetails.Count)" -ForegroundColor White

# ================================================
# STEP 2: UPDATE FUNCTION APP WITH DCR IDs
# ================================================

Write-Host "`n[2/5] Updating Function App configuration with DCR immutable IDs..." -ForegroundColor Yellow

# Get DCR Immutable IDs directly from Azure
Write-Host "  Retrieving DCR immutable IDs from Azure..." -ForegroundColor Gray

$updatedEnvironments = @()
foreach ($env in $Environments) {
    $envName = $env.name
    $dcrName = "SalesforceServiceCloud-$envName-DCR"

    Write-Host "  Getting DCR for $envName..." -ForegroundColor Gray

    try {
        $dcr = Get-AzDataCollectionRule -ResourceGroupName $ResourceGroupName -Name $dcrName -ErrorAction Stop
        $dcrImmutableId = $dcr.ImmutableId

        Write-Host "    ✅ $dcrName - $dcrImmutableId" -ForegroundColor Green

        $updatedEnvironments += [PSCustomObject]@{
            name             = $envName
            salesforceDomain = $env.salesforceDomain
            dcrImmutableId   = $dcrImmutableId
        }
    }
    catch {
        Write-Host "    ❌ Failed to get DCR: $dcrName" -ForegroundColor Red
        Write-Host "       Error: $($_.Exception.Message)" -ForegroundColor Red
        exit 1
    }
}

# Save configuration to file
$configFile = "FunctionApp-Environment-Config.json"
$updatedEnvironments | ConvertTo-Json -Depth 10 | Set-Content $configFile

Write-Host ""
Write-Host "✅ DCR IDs retrieved successfully!" -ForegroundColor Green
Write-Host "  Configuration saved to: $configFile" -ForegroundColor Gray

# Update Function App settings with environment configuration
Write-Host "`n  Updating Function App settings..." -ForegroundColor Gray

$environmentsJson = ($updatedEnvironments | ConvertTo-Json -Depth 10 -Compress)

try {
    Update-AzFunctionAppSetting `
        -ResourceGroupName $ResourceGroupName `
        -Name $functionAppName `
        -AppSetting @{
        "ENVIRONMENTS_JSON" = $environmentsJson
    } `
        -Force | Out-Null

    Write-Host "✅ Function App settings updated!" -ForegroundColor Green
}
catch {
    Write-Host "⚠️  Failed to update Function App settings: $($_.Exception.Message)" -ForegroundColor Yellow
}

# Display current configuration
Write-Host "`nEnvironment Configuration:" -ForegroundColor Cyan
foreach ($env in $updatedEnvironments) {
    Write-Host "  $($env.name):" -ForegroundColor Yellow
    Write-Host "    Domain: $($env.salesforceDomain)" -ForegroundColor White
    Write-Host "    DCR ID: $($env.dcrImmutableId)" -ForegroundColor White
}

# ================================================
# STEP 3: ASSIGN PERMISSIONS TO FUNCTION APP
# ================================================

Write-Host "`n[3/5] Assigning KeyVault and Storage permissions to Function App..." -ForegroundColor Yellow

$subscriptionId = $targetSubId

# 3A: Key Vault permissions
Write-Host "`n  [3A] Key Vault permissions..." -ForegroundColor Cyan
$kvScope = "/subscriptions/$subscriptionId/resourcegroups/$ResourceGroupName/providers/microsoft.keyvault/vaults/$keyVaultName"

try {
    New-AzRoleAssignment `
        -ObjectId $functionAppPrincipalId `
        -RoleDefinitionName "Key Vault Secrets User" `
        -Scope $kvScope `
        -ErrorAction SilentlyContinue | Out-Null

    Write-Host "  ✅ Key Vault Secrets User role assigned" -ForegroundColor Green
}
catch {
    if ($_.Exception.Message -notlike "*already exists*") {
        Write-Host "  ⚠️  Warning: $($_.Exception.Message)" -ForegroundColor Yellow
    }
    else {
        Write-Host "  ✅ Key Vault Secrets User role already assigned" -ForegroundColor Green
    }
}

# 3B: Storage Account permissions
Write-Host "`n  [3B] Storage Account permissions..." -ForegroundColor Cyan

$storageAccount = Get-AzStorageAccount -ResourceGroupName $ResourceGroupName | Where-Object { $_.StorageAccountName -like "stgsf*" }
$storageAccountName = $storageAccount.StorageAccountName
$storageScope = "/subscriptions/$subscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.Storage/storageAccounts/$storageAccountName"

Write-Host "  Storage Account: $storageAccountName" -ForegroundColor Gray

$storageRoles = @(
    "Storage Blob Data Owner",
    "Storage Queue Data Contributor",
    "Storage Table Data Contributor",
    "Storage File Data Privileged Contributor",
    "Storage Account Contributor"
)

foreach ($roleName in $storageRoles) {
    try {
        New-AzRoleAssignment `
            -ObjectId $functionAppPrincipalId `
            -RoleDefinitionName $roleName `
            -Scope $storageScope `
            -ErrorAction SilentlyContinue | Out-Null

        Write-Host "  ✅ $roleName" -ForegroundColor Green
    }
    catch {
        if ($_.Exception.Message -notlike "*already exists*") {
            Write-Host "  ⚠️  $roleName warning: $($_.Exception.Message)" -ForegroundColor Yellow
        }
        else {
            Write-Host "  ✅ $roleName (already assigned)" -ForegroundColor Green
        }
    }
}

# ================================================
# STEP 4: ASSIGN DCE PERMISSIONS
# ================================================

Write-Host "`n[4/5] Assigning Monitoring permissions to Function App..." -ForegroundColor Yellow

$rgScope = "/subscriptions/$subscriptionId/resourceGroups/$ResourceGroupName"

try {
    New-AzRoleAssignment `
        -ObjectId $functionAppPrincipalId `
        -RoleDefinitionName "Monitoring Metrics Publisher" `
        -Scope $rgScope `
        -ErrorAction SilentlyContinue | Out-Null

    Write-Host "✅ Monitoring Metrics Publisher role assigned" -ForegroundColor Green
}
catch {
    if ($_.Exception.Message -notlike "*already exists*") {
        Write-Host "⚠️  Monitoring permission assignment warning: $($_.Exception.Message)" -ForegroundColor Yellow
    }
    else {
        Write-Host "✅ Monitoring Metrics Publisher role already assigned" -ForegroundColor Green
    }
}

# ================================================
# STEP 5: WAIT FOR RBAC PROPAGATION
# ================================================

Write-Host "`n[5/5] Waiting for permissions to propagate..." -ForegroundColor Yellow
Write-Host "This takes 2-3 minutes..." -ForegroundColor Gray

for ($i = 120; $i -gt 0; $i -= 10) {
    Write-Progress -Activity "Waiting for RBAC propagation" -Status "Time remaining: $i seconds" -PercentComplete ((120 - $i) / 120 * 100)
    Start-Sleep -Seconds 10
}
Write-Progress -Activity "Waiting for RBAC propagation" -Completed

Write-Host "✅ Permission propagation complete" -ForegroundColor Green

# ================================================
# DEPLOYMENT SUMMARY
# ================================================

Write-Host "`n========================================" -ForegroundColor Cyan
if ($Mode -eq "InfraOnly") {
    Write-Host "INFRASTRUCTURE DEPLOYMENT COMPLETE" -ForegroundColor Green
}
else {
    Write-Host "DEPLOYMENT COMPLETE" -ForegroundColor Green
}
Write-Host "========================================`n" -ForegroundColor Cyan

Write-Host "📋 Deployment Summary:" -ForegroundColor Yellow
Write-Host ""
Write-Host "Shared Resources:" -ForegroundColor Cyan
Write-Host "  Key Vault:            $keyVaultName" -ForegroundColor White
Write-Host "  DCE:                  $dceName" -ForegroundColor White
Write-Host "  DCE Endpoint:         $dceEndpoint" -ForegroundColor White
Write-Host "  Function App:         $functionAppName" -ForegroundColor White
Write-Host "  Function Principal:   $functionAppPrincipalId" -ForegroundColor White
Write-Host ""
Write-Host "Environments ($($updatedEnvironments.Count)):" -ForegroundColor Cyan

foreach ($env in $updatedEnvironments) {
    $dcrName = "SalesforceServiceCloud-$($env.name)-DCR"

    Write-Host ""
    Write-Host "  [$($env.name)]" -ForegroundColor Yellow
    Write-Host "    Salesforce Domain:   $($env.salesforceDomain)" -ForegroundColor White
    Write-Host "    DCR Name:            $dcrName" -ForegroundColor White
    Write-Host "    DCR Immutable ID:    $($env.dcrImmutableId)" -ForegroundColor White
    Write-Host "    Secret Prefix:       sf-$($env.name)" -ForegroundColor White
}
Write-Host ""
Write-Host "Sentinel Data Connector:" -ForegroundColor Cyan
Write-Host "  Status:              Check in Sentinel > Data connectors" -ForegroundColor White
Write-Host "  Connector Name:      Salesforce Service Cloud (Multi-Environment)" -ForegroundColor White
Write-Host "  Connectivity Check:  Auto-updates based on data ingestion (last 3 days)" -ForegroundColor White
Write-Host "  Table:               SalesforceServiceCloudV2_CL" -ForegroundColor White
Write-Host ""

# Save deployment info
$infoFile = "Deployment-Info-Function-$(Get-Date -Format 'yyyyMMdd-HHmmss').txt"
$infoContent = @"
========================================
AZURE FUNCTION DEPLOYMENT INFO
========================================

Resource Group:        $ResourceGroupName
Workspace:             $WorkspaceName
Location:              $Location
Deployment Mode:       $Mode

SHARED RESOURCES
================
Key Vault:             $keyVaultName
DCE Name:              $dceName
DCE Endpoint:          $dceEndpoint
Function App:          $functionAppName
Function Principal:    $functionAppPrincipalId

ENVIRONMENTS
============
"@

foreach ($env in $updatedEnvironments) {
    $dcrName = "SalesforceServiceCloud-$($env.name)-DCR"
    $infoContent += @"

[$($env.name)]
  Salesforce Domain:   $($env.salesforceDomain)
  DCR Name:            $dcrName
  DCR Immutable ID:    $($env.dcrImmutableId)
  Secret Prefix:       sf-$($env.name)
  Secrets:
    - $keyVaultName/secrets/sf-$($env.name)-clientid
    - $keyVaultName/secrets/sf-$($env.name)-clientsecret
"@
}

$infoContent += @"


SENTINEL DATA CONNECTOR
========================
Connector Name:        Salesforce Service Cloud (Multi-Environment)
Connector Type:        GenericUI (Azure Function-based)
Table:                 SalesforceServiceCloudV2_CL
Status Check:          Auto-updates based on data received in last 3 days

Location in Portal:
  Microsoft Sentinel > Data connectors > 
  Search: "Salesforce Service Cloud (Multi-Environment)"

Connectivity Criteria:
  Connected = Data received in SalesforceServiceCloudV2_CL within last 3 days
  Disconnected = No data for 3+ days

"@


$infoContent += @"


RBAC ASSIGNMENTS
================
Function App Managed Identity: $functionAppPrincipalId
  - Key Vault Secrets User on $keyVaultName
  - Monitoring Metrics Publisher on $ResourceGroupName

QUERY EXAMPLES
==============
# View all data
SalesforceServiceCloudV2_CL
| take 100

# Filter by environment
SalesforceServiceCloudV2_CL
| where EnvironmentName == "$($updatedEnvironments[0].name)"
| take 10

# Count by environment
SalesforceServiceCloudV2_CL
| summarize Count=count() by EnvironmentName

# View parsed JSON data
SalesforceServiceCloudV2_CL
| where EnvironmentName == "$($updatedEnvironments[0].name)"
| extend ParsedData = parse_json(JsonData)
| project TimeGenerated, EventType, UserName, ParsedData

Deployed:              $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")
"@

$infoContent | Out-File $infoFile

Write-Host "💾 Deployment info saved to: $infoFile`n" -ForegroundColor Green

# ================================================
# NEXT STEPS
# ================================================

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "NEXT STEPS" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

if ($Mode -eq "InfraOnly") {
    Write-Host "1. Grant yourself Key Vault permissions:" -ForegroundColor Yellow
    Write-Host "   Portal > Key Vaults > $keyVaultName > Access control (IAM)" -ForegroundColor White
    Write-Host "   Add role assignment > Key Vault Secrets Officer > Your account" -ForegroundColor White
    Write-Host ""
    Write-Host "2. Store credentials:" -ForegroundColor Yellow
    Write-Host "   .\Deploy-Function-Solution.ps1 -Mode CredsOnly -ResourceGroupName '$ResourceGroupName' -WorkspaceName '$WorkspaceName' -Environments `$environments" -ForegroundColor White
    Write-Host ""
    Write-Host "3. Deploy Function App code:" -ForegroundColor Yellow
    Write-Host "   .\Deploy-Function-Solution.ps1 -Mode CodeOnly -ResourceGroupName '$ResourceGroupName' -WorkspaceName '$WorkspaceName'" -ForegroundColor White
    Write-Host ""
}
else {
    Write-Host "1. Verify Function App is running:" -ForegroundColor Yellow
    Write-Host "   Portal > Function App > $functionAppName > Functions > SalesforceToSentinel" -ForegroundColor White
    Write-Host ""
    Write-Host "2. Monitor Function execution:" -ForegroundColor Yellow
    Write-Host "   Portal > Function App > $functionAppName > Monitor" -ForegroundColor White
    Write-Host ""
    Write-Host "3. Check Application Insights logs:" -ForegroundColor Yellow
    Write-Host "   Portal > Application Insights > Logs" -ForegroundColor White
    Write-Host "   Query: traces | where message contains 'Salesforce'" -ForegroundColor White
    Write-Host ""
    Write-Host "4. Query data in Log Analytics:" -ForegroundColor Yellow
    Write-Host "   SalesforceServiceCloudV2_CL" -ForegroundColor White
    Write-Host "   | where EnvironmentName == '$($updatedEnvironments[0].name)'" -ForegroundColor White
    Write-Host "   | take 10`n" -ForegroundColor White
}

Write-Host "========================================`n" -ForegroundColor Cyan
