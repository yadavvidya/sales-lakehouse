# Fixes PATH issue and deploys databricks bundle

Write-Host "=== Databricks CLI PATH Fix and Deploy ===" -ForegroundColor Cyan

# Step 1: Add Python Scripts to current session PATH
$pythonScripts = "C:\Python312\Scripts"
Write-Host "`n[1/3] Adding Python Scripts to PATH..." -ForegroundColor Yellow
$env:Path += ";$pythonScripts"
Write-Host "Success: Added $pythonScripts to current session" -ForegroundColor Green

# Step 2: Find databricks executable
Write-Host "`n[2/3] Locating Databricks CLI..." -ForegroundColor Yellow

$databricksCmd = $null
$possibleLocations = @(
    "$pythonScripts\databricks.exe",
    "$pythonScripts\databricks",
    "$pythonScripts\databricks.cmd"
)

foreach ($location in $possibleLocations) {
    if (Test-Path $location) {
        $databricksCmd = $location
        Write-Host "Success: Found Databricks CLI at $location" -ForegroundColor Green
        break
    }
}

# If not found as executable, try running via Python module
if (-not $databricksCmd) {
    Write-Host "Executable not found. Trying Python module..." -ForegroundColor Yellow
    try {
        $version = python -m databricks --version 2>&1
        Write-Host "Success: Databricks CLI accessible via Python module" -ForegroundColor Green
        $databricksCmd = "python -m databricks"
    } catch {
        Write-Host "Error: Databricks CLI not found. Please verify installation." -ForegroundColor Red
        Write-Host "Try: pip install databricks-cli" -ForegroundColor Yellow
        exit 1
    }
}

# Step 3: Deploy bundle
Write-Host "`n[3/3] Deploying Databricks bundle to dev..." -ForegroundColor Yellow
try {
    if ($databricksCmd -like "*python*") {
        python -m databricks bundle deploy --target dev
    } else {
        & $databricksCmd bundle deploy --target dev
    }
    Write-Host "`nSuccess: Deployment complete!" -ForegroundColor Green
} catch {
    Write-Host "Error: Deployment failed: $_" -ForegroundColor Red
    exit 1
}

# Optional: Add to PATH permanently
Write-Host "`n--- Optional: Add to PATH permanently? ---" -ForegroundColor Cyan
Write-Host "Run this command to add permanently (requires restart):" -ForegroundColor Yellow
Write-Host '[Environment]::SetEnvironmentVariable("Path", $env:Path + ";C:\Python312\Scripts", "User")' -ForegroundColor White