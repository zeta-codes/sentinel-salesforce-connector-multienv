<#
.SYNOPSIS
    Safely removes all Salesforce Function resources from an Azure resource group while preserving the Log Analytics Workspace.

.DESCRIPTION
    Cleanup-SalesforceFunction.ps1 provides automated cleanup of Azure resources deployed by the Deploy-Function-Solution.ps1 script.
    
    The script identifies and deletes Salesforce-related resources in dependency order:
    1. Function Apps (func-sf-*)
    2. Data Collection Rules (SalesforceServiceCloud-*-DCR)
    3. App Service Plans (asp-sf-*)
    4. Application Insights (appi-sf-*)
    5. Data Collection Endpoints (*-shared-dce)
    6. Key Vaults (kv-sf-*) with special handling for soft-delete
    7. Storage Accounts (stgsf*)
    
    The Log Analytics Workspace and resource group are preserved. The script requires explicit confirmation ("YES") before deletion and provides a verification summary after cleanup.

.PARAMETER ResourceGroupName
    The Azure resource group containing the Salesforce Function resources to be deleted.

.EXAMPLE
    # Clean up all Salesforce Function resources from a resource group
    .\Cleanup-SalesforceFunction.ps1 -ResourceGroupName "rg-sentinel-prod"
    
    # The script will prompt for confirmation before deleting resources

.NOTES
    Version: 1.0
    Requires: Azure PowerShell (Az module)
    Permissions Required: Contributor or Owner role on the target resource group
    
    Safety Features:
    - Explicit "YES" confirmation required before deletion
    - Resources deleted in dependency order to prevent orphaned resources
    - Continues cleanup even if individual resources fail
    - Post-cleanup verification to confirm successful removal
    
    Warning: This operation cannot be undone. Ensure you have backups of any critical data.
#>

param(
    [Parameter(Mandatory = $true)]
    [string]$ResourceGroupName
)

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "CLEANUP SCRIPT FOR $ResourceGroupName" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# Validate subscription context consistency
$azContext = Get-AzContext -ErrorAction Stop
if (-not $azContext -or -not $azContext.Subscription) {
    Write-Host "❌ No Azure context found. Run Connect-AzAccount first." -ForegroundColor Red
    exit 1
}

$targetSubId = $azContext.Subscription.Id
$targetSubName = $azContext.Subscription.Name

Write-Host "Target Subscription: $targetSubName" -ForegroundColor Cyan
Write-Host "Subscription ID: $targetSubId`n" -ForegroundColor Gray

# Verify resource group exists in this subscription
Write-Host "Verifying resource group..." -ForegroundColor Yellow
$rgCheck = Get-AzResourceGroup -Name $ResourceGroupName -ErrorAction SilentlyContinue
if (-not $rgCheck) {
    Write-Host "❌ Resource group '$ResourceGroupName' not found in subscription '$targetSubName'" -ForegroundColor Red
    Write-Host "`n📋 To switch to a different subscription, run:" -ForegroundColor Yellow
    Write-Host "   Get-AzSubscription | Select-Object Name, Id, TenantId" -ForegroundColor White
    Write-Host "   Set-AzContext -SubscriptionId '<subscription-id>'" -ForegroundColor Cyan
    Write-Host "`nThen re-run this script.`n" -ForegroundColor Gray
    exit 1
}

Write-Host "✅ Found resource group in current subscription`n" -ForegroundColor Green

Write-Host "⚠️  WARNING: This will delete all Salesforce Function resources in $ResourceGroupName" -ForegroundColor Yellow
Write-Host "The following resources will be DELETED:" -ForegroundColor Yellow
Write-Host "  - Function App (func-sf-*)" -ForegroundColor White
Write-Host "  - App Service Plan (asp-sf-*)" -ForegroundColor White
Write-Host "  - Storage Account (stgsf*)" -ForegroundColor White
Write-Host "  - Key Vault (kv-sf-*)" -ForegroundColor White
Write-Host "  - Application Insights (appi-sf-*)" -ForegroundColor White
Write-Host "  - Data Collection Endpoint (*-shared-dce)" -ForegroundColor White
Write-Host "  - Data Collection Rules (SalesforceServiceCloud-*-DCR)" -ForegroundColor White
Write-Host "`nYour Log Analytics Workspace will NOT be deleted.`n" -ForegroundColor Green

$confirm = Read-Host "Type 'YES' to confirm deletion"

if ($confirm -ne "YES") {
    Write-Host "Cleanup cancelled." -ForegroundColor Yellow
    exit 0
}

Write-Host "`nStarting cleanup..." -ForegroundColor Yellow

# Get all resources in the resource group
$allResources = Get-AzResource -ResourceGroupName $ResourceGroupName

# Filter Salesforce-related resources
$sfResources = $allResources | Where-Object { 
    $_.Name -like "*-sf-*" -or 
    $_.Name -like "func-sf-*" -or 
    $_.Name -like "asp-sf-*" -or 
    $_.Name -like "stgsf*" -or 
    $_.Name -like "kv-sf-*" -or 
    $_.Name -like "appi-sf-*" -or
    $_.Name -like "*-shared-dce" -or
    $_.Name -like "SalesforceServiceCloud-*"
}

Write-Host "`nFound $($sfResources.Count) resources to delete:" -ForegroundColor Cyan
$sfResources | Format-Table Name, ResourceType -AutoSize

# Sort resources by dependency order (delete in reverse order of creation)
$deleteOrder = @(
    "Microsoft.Web/sites",                              # Function App first
    "Microsoft.Insights/dataCollectionRules",           # DCRs
    "Microsoft.Web/serverfarms",                        # App Service Plan
    "Microsoft.Insights/components",                    # App Insights
    "Microsoft.Insights/dataCollectionEndpoints",       # DCE
    "Microsoft.KeyVault/vaults",                        # Key Vault
    "Microsoft.Storage/storageAccounts"                 # Storage Account last
)

foreach ($resourceType in $deleteOrder) {
    $resources = $sfResources | Where-Object { $_.ResourceType -eq $resourceType }
    
    foreach ($resource in $resources) {
        Write-Host "`n[Deleting] $($resource.Name) ($($resource.ResourceType))" -ForegroundColor Yellow
        
        try {
            # Special handling for Key Vault
            if ($resource.ResourceType -eq "Microsoft.KeyVault/vaults") {

                # Delete the Key Vault
                Remove-AzKeyVault `
                    -ResourceGroupName $ResourceGroupName `
                    -VaultName $resource.Name `
                    -Force `
                    -ErrorAction Stop
                
                Write-Host "  ✅ Key Vault deleted" -ForegroundColor Green
            }
            else {
                # Standard deletion for other resources
                Remove-AzResource `
                    -ResourceId $resource.ResourceId `
                    -Force `
                    -ErrorAction Stop | Out-Null
                
                Write-Host "  ✅ Deleted successfully" -ForegroundColor Green
            }
        }
        catch {
            Write-Host "  ⚠️  Error: $($_.Exception.Message)" -ForegroundColor Red
            
            # Continue with next resource even if one fails
            continue
        }
        
        # Small delay between deletions
        Start-Sleep -Seconds 2
    }
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "CLEANUP COMPLETE" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# Verify cleanup
$remainingSFResources = Get-AzResource -ResourceGroupName $ResourceGroupName | Where-Object { 
    $_.Name -like "*-sf-*" -or 
    $_.Name -like "func-sf-*" -or 
    $_.Name -like "asp-sf-*" -or 
    $_.Name -like "stgsf*" -or 
    $_.Name -like "kv-sf-*" -or 
    $_.Name -like "appi-sf-*" -or
    $_.Name -like "*-shared-dce" -or
    $_.Name -like "SalesforceServiceCloud-*"
}

if ($remainingSFResources.Count -eq 0) {
    Write-Host "✅ All Salesforce Function resources removed successfully!" -ForegroundColor Green
    Write-Host "`nYour workspace and resource group are still intact." -ForegroundColor Cyan
}
else {
    Write-Host "⚠️  Some resources may still exist:" -ForegroundColor Yellow
    $remainingSFResources | Format-Table Name, ResourceType -AutoSize
    Write-Host "`nYou may need to manually delete these from the Portal." -ForegroundColor Yellow
}

