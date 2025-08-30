<#
.SYNOPSIS
  Import Excel -> SQL Server theo cấu trúc thư mục / cấu hình JSON.
  - Hỗ trợ quét *đệ quy* thư mục, nhiều pattern (*.xlsx, *.xlsm,...)
  - Dừng khi gặp dòng có Keyword trống (break)
  - Bỏ qua file đã xử lý (log _processed.csv), chế độ skip: "seen" | "unchanged"
  - Hỗ trợ bulk/insert/upsert, PreSql/PostSql, TruncateBeforeImport, DateFormat

.REQUIREMENTS
  - PowerShell 5+ (khuyên dùng PowerShell 7+)
  - Module: ImportExcel, SqlServer
#>

# ========== SETTINGS CƠ BẢN ==========
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$DataRoot = Join-Path $Root 'data'

Write-Host ("Root: {0}" -f $Root)
Write-Host ("Data root: {0}" -f $DataRoot)

# ========== ĐẢM BẢO MODULE ==========
function Ensure-Module {
    param([string]$Name)
    if (-not (Get-Module -ListAvailable -Name $Name)) {
        Write-Host ("Installing module {0} ..." -f $Name)
        try {
            Install-Module -Name $Name -Force -Scope CurrentUser -AllowClobber -ErrorAction Stop
        } catch {
            throw ("Không thể cài module {0}. Vui lòng mở PowerShell (Run as Administrator) và chạy: Install-Module {0}" -f $Name)
        }
    }
    Import-Module $Name -ErrorAction Stop
}
Ensure-Module -Name ImportExcel
Ensure-Module -Name SqlServer

# ========== HÀM TRỢ GIÚP ==========

function Get-DbConnectionString {
    param([Parameter(Mandatory)]$cfg)
    $server = $cfg.Server
    if ($cfg.Port -and $cfg.Port -ne 1433) { $server = "{0},{1}" -f $server, $cfg.Port }

    if ($cfg.Auth.IntegratedSecurity -eq $true) {
        return "Server=$server;Database=$($cfg.Database);Integrated Security=SSPI;TrustServerCertificate=True"
    } else {
        return "Server=$server;Database=$($cfg.Database);User Id=$($cfg.Auth.User);Password=$($cfg.Auth.Password);TrustServerCertificate=True"
    }
}

function Invoke-NonQuery {
    param(
        [Parameter(Mandatory)][string]$ConnectionString,
        [Parameter(Mandatory)][string]$Sql
    )
    $conn = New-Object System.Data.SqlClient.SqlConnection $ConnectionString
    try {
        $conn.Open()
        $cmd = $conn.CreateCommand()
        $cmd.CommandTimeout = 0
        $cmd.CommandText = $Sql
        [void]$cmd.ExecuteNonQuery()
    } finally {
        $conn.Close()
        $conn.Dispose()
    }
}

function Resolve-DotNetType {
    param([Parameter(Mandatory)][string]$TypeName)
    $t = $TypeName.Trim().ToLower()

    switch ($t) {
        {$_ -in @('string','system.string','nvarchar','ntext','text')}  { return [System.String] }
        {$_ -in @('int','int32','system.int32')}                         { return [System.Int32] }
        {$_ -in @('bigint','long','int64','system.int64')}               { return [System.Int64] }
        {$_ -in @('decimal','numeric','money','system.decimal')}         { return [System.Decimal] }
        {$_ -in @('float','double','system.double')}                     { return [System.Double] }
        {$_ -in @('bool','boolean','system.boolean')}                    { return [System.Boolean] }
        {$_ -in @('datetime','date','system.datetime')}                  { return [System.DateTime] }
        default {
            $dotnet = [System.Type]::GetType($TypeName)
            if ($dotnet) { return $dotnet }
            throw "Không nhận diện được kiểu dữ liệu: '$TypeName'. Hỗ trợ: string,int,int64,decimal,double,bool,datetime,date hoặc tên .NET đầy đủ."
        }
    }
}

function Validate-TableConfig {
    param([Parameter(Mandatory)]$TableConfig)

    if (-not $TableConfig) { throw "TableConfig null." }
    if (-not ($TableConfig.PSObject.Properties.Name -contains 'Columns')) {
        throw "Bảng '$($TableConfig.Name)' thiếu thuộc tính 'Columns' trong db.config.json."
    }

    $cols = @($TableConfig.Columns)   # ép về mảng thật
    if ($cols.Count -eq 0) {
        throw "Bảng '$($TableConfig.Name)' không có cột nào trong 'Columns'."
    }

    $i = 0
    foreach ($col in $cols) {
        $i++
        if ([string]::IsNullOrWhiteSpace($col.Db))    { throw "Bảng '$($TableConfig.Name)' cột thứ $i thiếu 'Db'." }
        if ([string]::IsNullOrWhiteSpace($col.Type))  { throw "Bảng '$($TableConfig.Name)' cột '$($col.Db)' thiếu 'Type'." }
        if ([string]::IsNullOrWhiteSpace($col.Excel)) { throw "Bảng '$($TableConfig.Name)' cột '$($col.Db)' thiếu 'Excel'." }
    }
}

function New-DataTableFromConfig {
    param([Parameter(Mandatory)]$TableConfig)

    Validate-TableConfig -TableConfig $TableConfig

    $cols = @($TableConfig.Columns)   # ép về mảng thật
    $dt = New-Object System.Data.DataTable ($TableConfig.Name)

    foreach ($col in $cols) {
        $dbCol   = $col.Db.Trim()
        $netType = Resolve-DotNetType -TypeName ($col.Type)
        Write-Host ("[Schema] add column {0} ({1})" -f $dbCol, $netType.FullName)  # log soi
        [void]$dt.Columns.Add($dbCol, $netType)
    }

    return ,$dt  # tránh unroll IEnumerable
}

function Convert-Cell {
    param(
        [object]$value,
        [string]$targetType,
        [string]$dateFormat = $null
    )

    if ($null -eq $value) { return [DBNull]::Value }
    if ($value -is [string]) {
        $t = $value.Trim()
        if ($t -eq "" -or $t -eq "-" -or $t -eq "N/A") { return [DBNull]::Value }
    }

    $type = Resolve-DotNetType -TypeName $targetType
    $tfn  = $type.FullName

    try {
        switch ($tfn) {
            "System.Int16" { return [int16]$value }
            "System.Int32" { return [int32]$value }
            "System.Int64" { return [int64]$value }
            "System.Decimal" {
                if ($value -is [string]) {
                    $s = $value.Trim()
                    if ($s -eq "") { return [DBNull]::Value }
                    $s = $s -replace ",",""
                    return [decimal]::Parse($s, [System.Globalization.CultureInfo]::InvariantCulture)
                }
                return [decimal]$value
            }
            "System.Double" {
                if ($value -is [string]) {
                    $s = $value.Trim()
                    if ($s -eq "") { return [DBNull]::Value }
                    $s = $s -replace ",",""
                    return [double]::Parse($s, [System.Globalization.CultureInfo]::InvariantCulture)
                }
                return [double]$value
            }
            "System.Boolean" {
                if ($value -is [bool]) { return $value }
                $s = "$value".Trim().ToLower()
                if     (@("1","true","yes","y","on").Contains($s)) { return $true }
                elseif (@("0","false","no","n","off").Contains($s)) { return $false }
                else { return [DBNull]::Value }
            }
            "System.DateTime" {
                if ($value -is [datetime]) { return $value }
                $s = "$value".Trim()
                if ($s -eq "") { return [DBNull]::Value }
                if ($dateFormat) {
                    return [datetime]::ParseExact($s, $dateFormat, $null)
                } else {
                    return [datetime]::Parse($s)
                }
            }
            default {
                $s = "$value"
                if ([string]::IsNullOrWhiteSpace($s)) { return [DBNull]::Value }
                return $s
            }
        }
    } catch {
        return [DBNull]::Value
    }
}

function BulkCopy-DataTable {
    param(
        [Parameter(Mandatory)][string]$ConnectionString,
        [Parameter(Mandatory)][System.Data.DataTable]$DataTable,
        [Parameter(Mandatory)][string]$TableName,
        [int]$BatchSize = 2000,
        [int]$BulkTimeoutSeconds = 0
    )
    $conn = New-Object System.Data.SqlClient.SqlConnection $ConnectionString
    $bulk = New-Object System.Data.SqlClient.SqlBulkCopy($conn, [System.Data.SqlClient.SqlBulkCopyOptions]::KeepIdentity, $null)
    $bulk.DestinationTableName = $TableName
    $bulk.BatchSize = $BatchSize
    $bulk.BulkCopyTimeout = $BulkTimeoutSeconds
    foreach ($col in $DataTable.Columns) {
        $bulk.ColumnMappings.Add($col.ColumnName, $col.ColumnName) | Out-Null
    }
    try {
        $conn.Open()
        $bulk.WriteToServer($DataTable)
    } finally {
        $bulk.Close()
        $conn.Close()
        $bulk.Dispose()
        $conn.Dispose()
    }
}

function Insert-Row {
    param(
        [Parameter(Mandatory)][string]$ConnectionString,
        [Parameter(Mandatory)][string]$TableName,
        [Parameter(Mandatory)][hashtable]$RowMap
    )
    $cols   = ($RowMap.Keys   | ForEach-Object { "[$_]" }) -join ","
    $params = ($RowMap.Keys   | ForEach-Object { "@$_" }) -join ","
    $sql    = "INSERT INTO [$TableName] ($cols) VALUES ($params);"
    $conn   = New-Object System.Data.SqlClient.SqlConnection $ConnectionString
    try {
        $conn.Open()
        $cmd = $conn.CreateCommand()
        $cmd.CommandTimeout = 0
        $cmd.CommandText = $sql
        foreach ($k in $RowMap.Keys) {
            $p = $cmd.Parameters.Add("@$k",[System.Data.SqlDbType]::Variant)
            $p.Value = $RowMap[$k]
        }
        [void]$cmd.ExecuteNonQuery()
    } finally {
        $conn.Close()
        $conn.Dispose()
    }
}

function Get-SafeExcelPath {
    param([Parameter(Mandatory)][string]$Path)
    # Import-Excel dùng wildcard khi là -Path, nên [ ] sẽ gây lỗi. Copy sang tên tạm trong %TEMP%.
    if ($Path -match '[\[\]]') {
        $tempDir  = [System.IO.Path]::GetTempPath()
        $safeName = [System.IO.Path]::GetFileName($Path) -replace '\[|\]', '_'
        $safePath = Join-Path $tempDir $safeName
        Copy-Item -LiteralPath $Path -Destination $safePath -Force
        return @{ ImportPath = $safePath; TempPath = $safePath }
    } else {
        return @{ ImportPath = $Path; TempPath = $null }
    }
}

function Do-Upsert {
    param(
        [Parameter(Mandatory)][string]$ConnectionString,
        [Parameter(Mandatory)][System.Data.DataTable]$DataTable,
        [Parameter(Mandatory)][string]$TargetTable,
        [Parameter(Mandatory)][string[]]$KeyColumns
    )
    # 1) tạo bảng tạm #stg với cùng schema
    $colsDef = $DataTable.Columns | ForEach-Object {
        $dotnet = $_.DataType.FullName
        $sqlType = switch -Regex ($dotnet) {
            "System\.Int(16|32|64)" { "BIGINT" }
            "System\.Decimal"       { "DECIMAL(38,10)" }
            "System\.Double"        { "FLOAT" }
            "System\.Boolean"       { "BIT" }
            "System\.DateTime"      { "DATETIME2(7)" }
            default                 { "NVARCHAR(MAX)" }
        }
        "[{0}] {1}" -f $_.ColumnName, $sqlType
    }
    $createStg = "CREATE TABLE #stg (" + ($colsDef -join ",") + ");"
    Invoke-NonQuery -ConnectionString $ConnectionString -Sql $createStg

    # 2) bulk vào #stg
    BulkCopy-DataTable -ConnectionString $ConnectionString -DataTable $DataTable -TableName "#stg"

    # 3) MERGE lên bảng đích
    $allCols = @($DataTable.Columns | ForEach-Object { $_.ColumnName })
    $nonKeys = $allCols | Where-Object { $KeyColumns -notcontains $_ }

    $onClause = ($KeyColumns | ForEach-Object { "T.[{0}] = S.[{0}]" -f $_ }) -join " AND "
    $setClause = ($nonKeys    | ForEach-Object { "T.[{0}] = S.[{0}]" -f $_ }) -join ", "
    $insCols   = ($allCols    | ForEach-Object { "[{0}]" -f $_ }) -join ", "
    $insVals   = ($allCols    | ForEach-Object { "S.[{0}]" -f $_ }) -join ", "

    $merge = @"
MERGE [$TargetTable] AS T
USING #stg AS S
ON ($onClause)
WHEN MATCHED THEN
    UPDATE SET $setClause
WHEN NOT MATCHED BY TARGET THEN
    INSERT ($insCols) VALUES ($insVals);
"@
    Invoke-NonQuery -ConnectionString $ConnectionString -Sql $merge
}

# ---- XỬ LÝ LOG FILE ĐÃ CÀI/CHẠY ----

function Get-ProcessedLogPath {
    param(
        [Parameter(Mandatory)]$DbFolder,
        [Parameter(Mandatory)]$Cfg
    )
    if ($Cfg.Defaults -and $Cfg.Defaults.ProcessedLogPath) {
        $p = [string]$Cfg.Defaults.ProcessedLogPath
        if ([System.IO.Path]::IsPathRooted($p)) { return $p }
        else { return (Join-Path $Root $p) }
    }
    # mặc định: 1 log/DB
    return (Join-Path $DbFolder.FullName "_processed.csv")
}

function Load-ProcessedIndex {
    param([Parameter(Mandatory)][string]$Path)
    if (Test-Path $Path) {
        try {
            $rows = Import-Csv -Path $Path -ErrorAction Stop
            if ($null -eq $rows) { return @() }
            return $rows
        } catch {
            return @()
        }
    }
    return @()
}


function Build-ProcessedMap {
    param([Parameter()][object[]]$Rows)
    $map = @{}
    if ($null -eq $Rows) { return $map }
    foreach ($r in $Rows) {
        if ($null -ne $r -and $null -ne $r.FullName) {
            $key = ($r.FullName.ToString()).ToLowerInvariant()
            $map[$key] = $r
        }
    }
    return $map
}


function Should-SkipFile {
    param(
        [Parameter(Mandatory)]$FileInfo,
        [Parameter()][hashtable]$ProcessedMap,
        [Parameter(Mandatory)][string]$SkipMode  # "seen" | "unchanged"
    )
    if ($null -eq $ProcessedMap) { return $false }

    $key = $FileInfo.FullName.ToLowerInvariant()
    if (-not $ProcessedMap.ContainsKey($key)) { return $false }

    if ($SkipMode -eq 'unchanged') {
        $row  = $ProcessedMap[$key]
        $len  = [int64]$row.Length
        $ts   = [string]$row.LastWriteTimeUtc
        $nowTs = $FileInfo.LastWriteTimeUtc.ToUniversalTime().ToString('o')
        return ($len -eq $FileInfo.Length -and $ts -eq $nowTs)
    }
    return $true
}


function Upsert-ProcessedEntry {
    param(
        [Parameter(Mandatory)]$FileInfo,
        [Parameter(Mandatory)][string]$DbName,
        [Parameter(Mandatory)][string]$TableName,
        [Parameter(Mandatory)]$ProcessedMap
    )
    $key = $FileInfo.FullName.ToLowerInvariant()
    $now = (Get-Date).ToUniversalTime().ToString('o')
    $row = $null
    if ($ProcessedMap.ContainsKey($key)) {
        $row = $ProcessedMap[$key]
        $row.LastProcessedUtc = $now
        $row.Length           = [string]$FileInfo.Length
        $row.LastWriteTimeUtc = $FileInfo.LastWriteTimeUtc.ToUniversalTime().ToString('o')
        $row.TableName        = $TableName
        $row.Database         = $DbName
        $ProcessedMap[$key]   = $row
    } else {
        $row = [pscustomobject]@{
            FullName         = $FileInfo.FullName
            Length           = [string]$FileInfo.Length
            LastWriteTimeUtc = $FileInfo.LastWriteTimeUtc.ToUniversalTime().ToString('o')
            Database         = $DbName
            TableName        = $TableName
            FirstProcessedUtc= $now
            LastProcessedUtc = $now
        }
        $ProcessedMap[$key] = $row
    }
}

function Save-ProcessedIndex {
    param(
        [Parameter()][hashtable]$ProcessedMap,
        [Parameter(Mandatory)][string]$Path
    )
    if ($null -eq $ProcessedMap) { $ProcessedMap = @{} }
    $items = $ProcessedMap.GetEnumerator() | ForEach-Object { $_.Value }
    $dir = Split-Path -Parent $Path
    if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
    $items | Sort-Object FullName | Export-Csv -Path $Path -NoTypeInformation -Encoding UTF8
}


# ========== LUỒNG CHÍNH ==========

# Quét tất cả folder database trong data\
$databaseFolders = Get-ChildItem -Path $DataRoot -Directory -ErrorAction Stop

foreach ($dbFolder in $databaseFolders) {
    $cfgPath = Join-Path $dbFolder.FullName "db.config.json"
    if (-not (Test-Path $cfgPath)) {
        Write-Warning ("Bỏ qua [{0}] vì không tìm thấy {1}" -f $dbFolder.Name, $cfgPath)
        continue
    }

    Write-Host ("==== Đọc cấu hình: {0} ====" -f $cfgPath)
    $cfg = Get-Content -Raw -Path $cfgPath | ConvertFrom-Json -Depth 100
    Write-Host ("Số bảng trong cấu hình: {0}" -f $cfg.Tables.Count)
    $cfg.Tables | ForEach-Object {
        Write-Host ("Bảng {0}: Columns.Count={1}" -f $_.Name, (@($_.Columns)).Count)
    }

    # Gán default
    if (-not $cfg.Defaults) { $cfg | Add-Member -NotePropertyName Defaults -NotePropertyValue @{} }
    if (-not $cfg.Defaults.FilePattern) { $cfg.Defaults.FilePattern = @("*.xlsx") }
    if (-not $cfg.Defaults.Mode) { $cfg.Defaults.Mode = "bulk" }
    if ($null -eq $cfg.Defaults.Recurse) { $cfg.Defaults | Add-Member -NotePropertyName Recurse -NotePropertyValue $true }

    # Cảnh báo nếu tên DB trong file khác tên thư mục
    if ($cfg.Database -ne $dbFolder.Name) {
        Write-Warning ("Tên DB trong cấu hình ({0}) khác tên thư mục ({1}). Vẫn tiếp tục." -f $cfg.Database, $dbFolder.Name)
    }

    $cs = Get-DbConnectionString -cfg $cfg
    Write-Host ("Kết nối tới: {0}:{1} DB={2}" -f $cfg.Server, $cfg.Port, $cfg.Database)

    # Nạp/chuẩn bị log đã xử lý
    $processedLogPath = Get-ProcessedLogPath -DbFolder $dbFolder -Cfg $cfg
    $processedRows = Load-ProcessedIndex -Path $processedLogPath
    if ($null -eq $processedRows) { $processedRows = @() }

    $processedMap  = Build-ProcessedMap -Rows $processedRows
    if ($null -eq $processedMap)  { $processedMap  = @{} }

    Write-Host ("Processed log: {0} (đã có {1} mục)" -f $processedLogPath, $processedMap.Count)

    foreach ($tbl in $cfg.Tables) {
        $tableName = $tbl.Name

        # SkipProcessed & SkipMode (bảng override Defaults)
        $skipProcessed = $true
        if ($null -ne $cfg.Defaults.SkipProcessed) { $skipProcessed = [bool]$cfg.Defaults.SkipProcessed }
        if ($null -ne $tbl.SkipProcessed)          { $skipProcessed = [bool]$tbl.SkipProcessed }

        $skipMode = 'seen'   # seen | unchanged
        if ($cfg.Defaults.SkipMode) { $skipMode = [string]$cfg.Defaults.SkipMode }
        if ($tbl.SkipMode)          { $skipMode = [string]$tbl.SkipMode }

        $folderRel = $tbl.Folder
        $tableDir  = Join-Path $dbFolder.FullName $folderRel

        try {
            Validate-TableConfig -TableConfig $tbl

            if (-not (Test-Path $tableDir)) {
                Write-Warning ("Bảng {0}: không thấy thư mục {1} -> bỏ qua." -f $tableName, $tableDir)
                continue
            }

            # ---- QUÉT TOÀN BỘ EXCEL (kể cả thư mục con), hỗ trợ nhiều pattern ----
            # 1) quyết định Recurse (bảng > default > true mặc định)
            $recurse = $true
            if ($null -ne $cfg.Defaults.Recurse) { $recurse = [bool]$cfg.Defaults.Recurse }
            if ($null -ne $tbl.Recurse)          { $recurse = [bool]$tbl.Recurse }  # bảng override

            # 2) gom danh sách pattern
            $patterns = @()
            if ($tbl.FilePattern) {
                $patterns = @($tbl.FilePattern)
            } elseif ($cfg.Defaults.FilePattern) {
                $patterns = @($cfg.Defaults.FilePattern)
            } else {
                $patterns = @("*.xlsx","*.xlsm")  # mặc định smart
            }

            # 3) quét file theo từng pattern
            $files = @()
            foreach ($pat in $patterns) {
                if ($recurse) {
                    $files += Get-ChildItem -Path $tableDir -File -Recurse -Filter $pat -ErrorAction SilentlyContinue
                } else {
                    $files += Get-ChildItem -Path $tableDir -File -Filter  $pat -ErrorAction SilentlyContinue
                }
            }
            # 4) loại file tạm Excel (~$...), uniq theo FullName, sắp xếp
            $files = $files |
                Where-Object { $_.Name -notlike "~$*" } |
                Sort-Object FullName -Unique

            if ($files.Count -eq 0) {
                $joined = ($patterns -join ", ")
                Write-Host ("Bảng {0}: không có file phù hợp ({1})." -f $tableName, $joined)
                continue
            }
            Write-Host ("Tìm thấy {0} file Excel cho [{1}] (Recurse={2})." -f $files.Count, $tableName, $recurse)
            # ----------------------------------------------------------------------

            # PreSql (nếu có)
            if ($tbl.PreSql) {
                foreach ($sql in $tbl.PreSql) {
                    Write-Host ("Chạy PreSql: {0}" -f $sql)
                    Invoke-NonQuery -ConnectionString $cs -Sql $sql
                }
            }

            # TruncateBeforeImport
            $truncate = $false
            if ($null -ne $tbl.TruncateBeforeImport) { $truncate = [bool]$tbl.TruncateBeforeImport }
            elseif ($null -ne $cfg.Defaults.TruncateBeforeImport) { $truncate = [bool]$cfg.Defaults.TruncateBeforeImport }

            $mode = if ($tbl.Mode) { $tbl.Mode } else { $cfg.Defaults.Mode }
            if ($truncate -and ($mode -ne "upsert")) {
                $sqlTrunc = "TRUNCATE TABLE [$tableName];"
                Write-Host ("Truncate {0} ..." -f $tableName)
                Invoke-NonQuery -ConnectionString $cs -Sql $sqlTrunc
            }

            $dateFormat     = if ($tbl.DateFormat) { $tbl.DateFormat } else { $cfg.Defaults.DateFormat }
            $sheetName      = $tbl.SheetName
            $identityInsert = [bool]$tbl.IdentityInsert

            foreach ($file in $files) {

                # --- SKIP nếu file đã xử lý ---
                if ($skipProcessed -and (Should-SkipFile -FileInfo $file -ProcessedMap $processedMap -SkipMode $skipMode)) {
                    Write-Host ("Skip (đã xử lý - mode={0}): {1}" -f $skipMode, $file.FullName)
                    continue
                }
                # --------------------------------

                Write-Host ("Đang xử lý: {0} -> [{1}] (mode={2})" -f $file.Name, $tableName, $mode)

                # 1) Đọc Excel (an toàn tên file có [ ])
                $safe = Get-SafeExcelPath -Path $file.FullName
                try {
                    $excelParams = @{ Path = $safe.ImportPath }
                    if ($sheetName) { $excelParams["WorksheetName"] = $sheetName }
                    Write-Host ("Đọc Excel từ: {0}" -f $safe.ImportPath)
                    $rows = Import-Excel @excelParams
                }
                finally {
                    if ($safe.TempPath) {
                        Remove-Item -LiteralPath $safe.TempPath -ErrorAction SilentlyContinue
                    }
                }

                if (-not $rows -or $rows.Count -eq 0) {
                    Write-Warning ("File rỗng hoặc không đọc được: {0}" -f $file.Name)
                    continue
                }

                # 2) Dựng DataTable đúng schema
                $dt = New-DataTableFromConfig -TableConfig $tbl
                if ($null -eq $dt -or $dt.Columns.Count -eq 0) {
                    throw ("Không tạo được DataTable cho bảng '{0}'. Kiểm tra 'Columns' / 'Type' trong db.config.json." -f $tableName)
                }

                # 3) Nạp dữ liệu
                Write-Host ("=== Columns mapping cho {0} ===" -f $tableName)
                $tbl.Columns | ForEach-Object {
                    Write-Host ("Db={0} | Type={1} | Excel={2}" -f $_.Db, $_.Type, $_.Excel)
                }

                # xác định header "Keyword" để dừng sớm nếu trống
                $keywordExcelHeader = (
                    $tbl.Columns | Where-Object { $_.Db -eq 'Keyword' -or $_.Excel -eq 'Keyword' } | Select-Object -First 1
                ).Excel
                if (-not $keywordExcelHeader) { $keywordExcelHeader = 'Keyword' }

                foreach ($r in $rows) {

                    # STOP nếu Keyword trống (break)
                    $kwProp = $r.PSObject.Properties | Where-Object { $_.Name.Trim() -eq $keywordExcelHeader } | Select-Object -First 1
                    if (-not $kwProp) { $kwProp = $r.PSObject.Properties | Where-Object { $_.Name.Trim() -ieq $keywordExcelHeader } | Select-Object -First 1 }
                    $kwVal = $null; if ($kwProp) { $kwVal = $r.($kwProp.Name) }
                    if ($null -eq $kwVal -or ([string]::IsNullOrWhiteSpace([string]$kwVal))) {
                        Write-Host ("Gặp dòng 'Keyword' trống -> dừng đọc file {0}" -f $file.Name)
                        break
                    }

                    $dr = $dt.NewRow()
                    foreach ($col in $tbl.Columns) {
                        $dbCol       = if ($col.Db    -is [string]) { $col.Db.Trim() }    else { $col.Db }
                        $excelHeader = if ($col.Excel -is [string]) { $col.Excel.Trim() } else { $col.Excel }
                        $targetType  = $col.Type

                        if ([string]::IsNullOrWhiteSpace($dbCol)) {
                            throw ("Mapping lỗi: có cột 'Db' rỗng/null trong bảng '{0}'." -f $tableName)
                        }
                        if (-not $dt.Columns.Contains($dbCol)) {
                            throw ("Mapping lỗi: cột đích '{0}' không tồn tại trong DataTable (bảng '{1}')." -f $dbCol, $tableName)
                        }

                        # Lấy giá trị từ hàng Excel (thử khớp đúng, sau đó case-insensitive)
                        $prop = $r.PSObject.Properties | Where-Object { $_.Name.Trim() -eq  $excelHeader } | Select-Object -First 1
                        if (-not $prop) {
                            $prop = $r.PSObject.Properties | Where-Object { $_.Name.Trim() -ieq $excelHeader } | Select-Object -First 1
                        }

                        $val = $null
                        if ($prop) { $val = $r.($prop.Name) }

                        $converted = Convert-Cell -value $val -targetType $targetType -dateFormat $dateFormat
                        if ($null -eq $converted) { $converted = [DBNull]::Value }  # phòng hờ
                        $dr[$dbCol] = $converted
                    }

                    [void]$dt.Rows.Add($dr)
                }

                # 4) Import theo mode
                if ($mode -eq "bulk") {
                    if ($identityInsert) {
                        Invoke-NonQuery -ConnectionString $cs -Sql ("SET IDENTITY_INSERT [{0}] ON;" -f $tableName)
                    }
                    BulkCopy-DataTable -ConnectionString $cs -DataTable $dt -TableName $tableName
                    if ($identityInsert) {
                        Invoke-NonQuery -ConnectionString $cs -Sql ("SET IDENTITY_INSERT [{0}] OFF;" -f $tableName)
                    }
                }
                elseif ($mode -eq "insert") {
                    foreach ($row in $dt.Rows) {
                        $map = @{}
                        foreach ($c in $dt.Columns) {
                            $map[$c.ColumnName] = $row[$c.ColumnName]
                        }
                        Insert-Row -ConnectionString $cs -TableName $tableName -RowMap $map
                    }
                }
                elseif ($mode -eq "upsert") {
                    if (-not $tbl.KeyColumns -or $tbl.KeyColumns.Count -eq 0) {
                        throw ("Bảng {0} dùng upsert nhưng thiếu KeyColumns trong cấu hình." -f $tableName)
                    }
                    Do-Upsert -ConnectionString $cs -DataTable $dt -TargetTable $tableName -KeyColumns $tbl.KeyColumns
                }
                else {
                    throw ("Mode không hợp lệ: {0} (hỗ trợ: bulk | insert | upsert)" -f $mode)
                }

                # cập nhật log đã xử lý (chỉ làm sau khi import thành công)
                Upsert-ProcessedEntry -FileInfo $file -DbName $cfg.Database -TableName $tableName -ProcessedMap $processedMap
            }

            # PostSql (nếu có)
            if ($tbl.PostSql) {
                foreach ($sql in $tbl.PostSql) {
                    Write-Host ("Chạy PostSql: {0}" -f $sql)
                    Invoke-NonQuery -ConnectionString $cs -Sql $sql
                }
            }

            Write-Host (">> Hoàn tất bảng {0}" -f $tableName)
        }
        catch {
            Write-Warning ("Lỗi khi xử lý bảng {0}: {1}" -f $tableName, $_.Exception.Message)
        }

        # Lưu log sau mỗi bảng
        Save-ProcessedIndex -ProcessedMap $processedMap -Path $processedLogPath
    }

    Write-Host ("==== Hoàn tất database: {0} ====" -f $cfg.Database)
}


Write-Host ">>> DONE."
