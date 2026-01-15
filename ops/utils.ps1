# ==========================================
# HELPER FUNCTION FOR LOGGING
# ==========================================

function Write-Log {
    param (
        [Parameter(Mandatory=$true)]
        [string]$Message,

        [Parameter(Mandatory=$false)]
        [ValidateSet("INFO", "WARN", "ERROR", "SUCCESS", "STEP")]
        [string]$Level = "INFO",

        [switch]$NoNewline
    )

    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    
    # set color based on log level
    switch ($Level) {
        "INFO"    { $color = "White" }
        "WARN"    { $color = "Yellow" }
        "ERROR"   { $color = "Red" }
        "SUCCESS" { $color = "Green" }
        "STEP"    { $color = "Cyan" }
    }

    # Format: [TIMESTAMP] [LEVEL] Message
    Write-Host "[$timestamp] [$Level] $Message" -ForegroundColor $color -NoNewline:$NoNewline
}