# Resolves Windows directory symlinks/junctions for CMake (cargokit).
# Uses Join-Path + -LiteralPath (PS 5.1 safe for ...\AppData\...).
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$InputPath
)

$ErrorActionPreference = 'Stop'

function Get-ReparseTarget {
    param([System.IO.FileSystemInfo]$Item)

    if ($Item.PSObject.Properties['Target'] -and $Item.Target) {
        $t = $Item.Target
        if ($t -is [array]) { return [string]$t[0] }
        return [string]$t
    }
    if ($Item.PSObject.Properties['LinkTarget'] -and $Item.LinkTarget) {
        return [string]$Item.LinkTarget
    }
    return $null
}

function Resolve-SymlinksPath {
    param([string]$Path)

    $full = [System.IO.Path]::GetFullPath(
        ($Path -replace '/', [System.IO.Path]::DirectorySeparatorChar)
    )
    $root = [System.IO.Path]::GetPathRoot($full)
    if (-not $root) {
        throw "Invalid path: $Path"
    }

    $tail = $full.Substring($root.Length).TrimStart('\')
    if (-not $tail) {
        return $root.TrimEnd('\')
    }

    $segments = $tail.Split([char]'\', [StringSplitOptions]::RemoveEmptyEntries)
    $current = $root

    foreach ($seg in $segments) {
        $current = Join-Path $current $seg

        while ($true) {
            if (-not (Test-Path -LiteralPath $current)) {
                throw "Path not found: $current"
            }
            $item = Get-Item -LiteralPath $current -Force
            $target = Get-ReparseTarget $item
            if (-not $target) { break }

            $target = $target.TrimEnd('\')
            if (-not [System.IO.Path]::IsPathRooted($target)) {
                $parent = Split-Path -LiteralPath $current -Parent
                $target = Join-Path $parent $target
            }
            $current = [System.IO.Path]::GetFullPath($target)
        }
    }

    return $current
}

try {
    $resolved = Resolve-SymlinksPath $InputPath
    Write-Output ($resolved -replace '\\', '/')
    exit 0
}
catch {
    Write-Error "resolve_symlinks.ps1 failed for '$InputPath': $_"
    exit 1
}
