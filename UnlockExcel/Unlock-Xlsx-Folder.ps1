<#
.SYNOPSIS
  Batch unlock .xlsx files (remove sheet/workbook protection & read-only recommended),
  then append one space to a configured cell, save & close.

  Reads settings from app.config.json in the same folder as this script.
  Logs results to processed.log.csv.

.NOTES
  - Requires Microsoft Excel installed (Excel COM).
  - Cannot bypass OPEN PASSWORD encryption.
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ---------- Helpers ----------
function Read-JsonFile {
    param([Parameter(Mandatory=$true)][string]$Path)
    if (-not (Test-Path $Path)) { throw "Config not found: $Path" }
    $raw = Get-Content -LiteralPath $Path -Raw -Encoding UTF8
    return $raw | ConvertFrom-Json
}

function Ensure-Dir {
    param([string]$Path)
    if (-not (Test-Path $Path)) { New-Item -ItemType Directory -Path $Path | Out-Null }
}

function Remove-ProtectTagsFromXlsx {
    param(
        [Parameter(Mandatory=$true)][string]$XlsxPath,
        [switch]$NoBackup
    )
    if (-not (Test-Path $XlsxPath)) { throw "File not found: $XlsxPath" }

    if (-not $NoBackup) {
        Copy-Item -LiteralPath $XlsxPath -Destination "$XlsxPath.bak" -Force
    }

    try { Unblock-File -LiteralPath $XlsxPath } catch {}

    $tempDir = Join-Path ([System.IO.Path]::GetTempPath()) ("xlsx_unlock_" + [System.Guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Path $tempDir | Out-Null

    Add-Type -AssemblyName System.IO.Compression.FileSystem
    [System.IO.Compression.ZipFile]::ExtractToDirectory($XlsxPath, $tempDir)

    function Load-Xml($filePath) {
        $xml = New-Object System.Xml.XmlDocument
        $xml.PreserveWhitespace = $true
        $xml.Load($filePath)
        return $xml
    }

    # Remove sheetProtection in each worksheet
    $sheetsPath = Join-Path $tempDir "xl\worksheets"
    if (Test-Path $sheetsPath) {
        Get-ChildItem -LiteralPath $sheetsPath -Filter "*.xml" | ForEach-Object {
            $p = $_.FullName
            $xml = Load-Xml $p
            $nsm = New-Object System.Xml.XmlNamespaceManager($xml.NameTable)
            $nsm.AddNamespace("d", $xml.DocumentElement.NamespaceURI)
            $nodes = $xml.SelectNodes("//d:sheetProtection", $nsm)
            if ($nodes -and $nodes.Count -gt 0) {
                $nodes | ForEach-Object { $_.ParentNode.RemoveChild($_) | Out-Null }
                $xml.Save($p)
            }
        }
    }

    # Remove workbookProtection and fileSharing
    $workbookPath = Join-Path $tempDir "xl\workbook.xml"
    if (Test-Path $workbookPath) {
        $xml = Load-Xml $workbookPath
        $nsm = New-Object System.Xml.XmlNamespaceManager($xml.NameTable)
        $nsm.AddNamespace("d", $xml.DocumentElement.NamespaceURI)

        $wp = $xml.SelectNodes("//d:workbookProtection", $nsm)
        if ($wp) { $wp | ForEach-Object { $_.ParentNode.RemoveChild($_) | Out-Null } }

        $fs = $xml.SelectNodes("//d:fileSharing", $nsm)
        if ($fs) { $fs | ForEach-Object { $_.ParentNode.RemoveChild($_) | Out-Null } }

        $xml.Save($workbookPath)
    }

    # Repack
    $tempZip = Join-Path ([System.IO.Path]::GetTempPath()) ("xlsx_repack_" + [System.Guid]::NewGuid().ToString("N") + ".zip")
    if (Test-Path $tempZip) { Remove-Item $tempZip -Force }
    [System.IO.Compression.ZipFile]::CreateFromDirectory($tempDir, $tempZip, [System.IO.Compression.CompressionLevel]::Optimal, $false)
    Move-Item -LiteralPath $tempZip -Destination $XlsxPath -Force

    Remove-Item -LiteralPath $tempDir -Recurse -Force -ErrorAction SilentlyContinue
}

function Append-Space-WithExcelCom {
    param(
        [Parameter(Mandatory=$true)][string]$XlsxPath,
        [string]$Cell = "A1",
        [Object]$Sheet = 1
    )
    $excel = $null
    $wb = $null
    try {
        $excel = New-Object -ComObject Excel.Application
        $excel.Visible = $false
        $excel.DisplayAlerts = $false

        $wb = $excel.Workbooks.Open($XlsxPath, $null, $false)

        if ($Sheet -is [int]) {
            $ws = $wb.Worksheets.Item([int]$Sheet)
        } else {
            $ws = $wb.Worksheets.Item([string]$Sheet)
        }

        $range = $ws.Range($Cell)
        $val = $range.Value2
        if ($null -eq $val) { $val = "" }
        $range.Value2 = ($val.ToString() + " ")

        $wb.Save()
        $wb.Close($true)
    } catch {
        throw "Excel COM failed on '$([System.IO.Path]::GetFileName($XlsxPath))': $($_.Exception.Message)"
    } finally {
        if ($wb) { try { $wb.Close($false) } catch {} }
        if ($excel) { try { $excel.Quit() } catch {} }
        [System.Runtime.InteropServices.Marshal]::ReleaseComObject($wb) | Out-Null 2>$null
        [System.Runtime.InteropServices.Marshal]::ReleaseComObject($excel) | Out-Null 2>$null
        [GC]::Collect(); [GC]::WaitForPendingFinalizers()
    }
}

# ---------- MAIN ----------
try {
    $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
    $configPath = Join-Path $scriptDir "app.config.json"
    $cfg = Read-JsonFile -Path $configPath

    $root = $cfg.RootFolder
    $recursive = [bool]$cfg.Recursive
    $pattern = if ($cfg.FilePattern) { [string]$cfg.FilePattern } else { "*.xlsx" }
    $cell = if ($cfg.Cell) { [string]$cfg.Cell } else { "A1" }
    $sheet = if ($cfg.Sheet) { $cfg.Sheet } else { 1 }
    $createBackup = [bool]$cfg.CreateBackup
    $stopOnError = [bool]$cfg.StopOnError

    if (-not (Test-Path $root)) { throw "RootFolder not found: $root" }

    $files = Get-ChildItem -LiteralPath $root -Filter $pattern -File -Recurse:$recursive
    if (-not $files -or $files.Count -eq 0) {
        Write-Host "No files matched pattern '$pattern' in '$root' (Recursive=$recursive)." -ForegroundColor Yellow
        return
    }

    $logPath = Join-Path $scriptDir "processed.log.csv"
    if (-not (Test-Path $logPath)) {
        "Timestamp,File,Result,Message" | Out-File -LiteralPath $logPath -Encoding UTF8
    }

    Write-Host "Found $($files.Count) .xlsx file(s). Starting..." -ForegroundColor Cyan

    foreach ($f in $files) {
        $t0 = Get-Date
        try {
            if ([System.IO.Path]::GetExtension($f.FullName).ToLower() -ne ".xlsx") {
                throw "Skip non-.xlsx file."
            }

            Write-Host (" -> Processing: {0}" -f $f.FullName) -ForegroundColor Gray

            Remove-ProtectTagsFromXlsx -XlsxPath $f.FullName -NoBackup:(!$createBackup)
            Append-Space-WithExcelCom -XlsxPath $f.FullName -Cell $cell -Sheet $sheet

            $msg = "OK in {0:n1}s" -f ((Get-Date) - $t0).TotalSeconds
            Add-Content -LiteralPath $logPath -Value ("{0},{1},{2},{3}" -f (Get-Date).ToString("s"), $f.FullName, "Success", $msg)
            Write-Host ("    ✓ {0}" -f $msg) -ForegroundColor Green
        } catch {
            $emsg = $_.Exception.Message.Replace("`n"," ").Replace("`r"," ")
            Add-Content -LiteralPath $logPath -Value ("{0},{1},{2},{3}" -f (Get-Date).ToString("s"), $f.FullName, "Error", '"' + $emsg.Replace('"','""') + '"')
            Write-Host ("    ✗ Error: {0}" -f $emsg) -ForegroundColor Red
            if ($stopOnError) { throw }
        }
    }

    Write-Host "All done. See log: $logPath" -ForegroundColor Cyan
}
catch {
    Write-Error $_.Exception.Message
    Write-Host "Note: This cannot unlock files encrypted with an OPEN PASSWORD." -ForegroundColor Yellow
}
