# Installs the new Databricks CLI for bundle deployments

Write-Host "=== Installing Databricks CLI ===" -ForegroundColor Cyan

# Step 1: Check if running as Administrator
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "Warning: Not running as Administrator. Installation may fail." -ForegroundColor Yellow
}

# Step 2: Download latest Databricks CLI from correct URL
Write-Host "`n[1/3] Downloading Databricks CLI..." -ForegroundColor Yellow
$downloadUrl = "https://github.com/databricks/cli/releases/download/v0.210.0/databricks_cli_0.210.0_windows_amd64.zip"
$zipPath = "$env:TEMP\databricks_cli.zip"
$extractPath = "$env:TEMP\databricks_cli"

try {
    # Get latest release URL dynamically
    $releasesUrl = "https://api.github.com/repos/databricks/cli/releases/latest"
    $release = Invoke-RestMethod -Uri $releasesUrl
    $asset = $release.assets | Where-Object { $_.name -like "*windows_amd64.zip" } | Select-Object -First 1
    
    if ($asset) {
        $downloadUrl = $asset.browser_download_url
        Write-Host "Found latest version: $($release.tag_name)" -ForegroundColor Green
    } else {
        Write-Host "Using fallback URL" -ForegroundColor Yellow
    }
    
    Invoke-WebRequest -Uri $downloadUrl -OutFile $zipPath -UseBasicParsing
    Write-Host "Downloaded to $zipPath" -ForegroundColor Green
} catch {
    Write-Host "Error downloading: $_" -ForegroundColor Red
    Write-Host "`nTrying alternative installation method..." -ForegroundColor Yellow
    
    # Alternative: Install via winget if available
    try {
        winget install Databricks.DatabricksCLI
        Write-Host "Installed via winget" -ForegroundColor Green
        
        # Verify
        $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
        databricks --version
        exit 0
    } catch {
        Write-Host "Winget not available. Please install manually from:" -ForegroundColor Red
        Write-Host "https://github.com/databricks/cli/releases" -ForegroundColor White
        exit 1
    }
}

# Step 3: Extract and install
Write-Host "`n[2/3] Extracting and installing..." -ForegroundColor Yellow
try {
    if (Test-Path $extractPath) {
        Remove-Item $extractPath -Recurse -Force
    }
    
    Expand-Archive -Path $zipPath -DestinationPath $extractPath -Force
    
    # Find databricks.exe in extracted folder
    $exePath = Get-ChildItem -Path $extractPath -Filter "databricks.exe" -Recurse | Select-Object -First 1
    
    if ($exePath) {
        # Move to a directory in PATH
        $targetDir = "C:\Python312\Scripts"
        if (-not (Test-Path $targetDir)) {
            New-Item -ItemType Directory -Path $targetDir -Force | Out-Null
        }
        
        $targetPath = Join-Path $targetDir "databricks.exe"
        Copy-Item $exePath.FullName -Destination $targetPath -Force
        
        Write-Host "Installed to $targetPath" -ForegroundColor Green
    } else {
        Write-Host "Error: databricks.exe not found in archive" -ForegroundColor Red
        exit 1
    }
} catch {
    Write-Host "Error extracting: $_" -ForegroundColor Red
    exit 1
}

# Step 4: Verify installation
Write-Host "`n[3/3] Verifying installation..." -ForegroundColor Yellow
$env:Path += ";C:\Python312\Scripts"
try {
    $version = & "C:\Python312\Scripts\databricks.exe" --version
    Write-Host "Success! Databricks CLI installed: $version" -ForegroundColor Green
    
    Write-Host "`n=== Next Steps ===" -ForegroundColor Cyan
    Write-Host "1. Restart PowerShell or run: `$env:Path += ';C:\Python312\Scripts'" -ForegroundColor White
    Write-Host "2. Configure: databricks configure --token" -ForegroundColor White
    Write-Host "3. Deploy: databricks bundle deploy --target dev" -ForegroundColor White
} catch {
    Write-Host "Error verifying: $_" -ForegroundColor Red
    Write-Host "Try restarting PowerShell and running: databricks --version" -ForegroundColor Yellow
}

# Cleanup
if (Test-Path $zipPath) { Remove-Item $zipPath -Force }
if (Test-Path $extractPath) { Remove-Item $extractPath -Recurse -Force }