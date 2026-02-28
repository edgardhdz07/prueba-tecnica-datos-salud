# ==============================================================================
# SCRIPT DE AUTOMATIZACIÓN - PIPELINE SERSOCIAL
# Ejecuta: Inicialización BD + ETL Python + Validaciones + Logs
# ==============================================================================

Clear-Host
$StartTime = Get-Date

Write-Host ">>> INICIANDO PIPELINE DE DATOS - PRUEBA TECNICA INGENIERO DE DATOS FUNDACIÓN SERSOCIAL <<<" -ForegroundColor Cyan

# Obtener ruta raíz del proyecto (independiente desde dónde se ejecute)
$RootPath = Split-Path -Parent $PSScriptRoot

# ------------------------------------------------------------------------------
# 1. VALIDACIÓN DE REQUISITOS
# ------------------------------------------------------------------------------

Write-Host "[1/4] Verificando dependencias..." -ForegroundColor Yellow

if (!(Test-Path "$RootPath\etl\data_etl.py")) {
    Write-Host "ERROR: No se encuentra el archivo etl\data_etl.py" -ForegroundColor Red
    exit 1
}

if (!(Test-Path "$RootPath\sql\ddl\01_inicializacion_db.sql")) {
    Write-Host "ERROR: No se encuentra el script SQL de inicialización." -ForegroundColor Red
    exit 1
}

# Verificar que Python esté disponible
if (!(Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Host "ERROR: Python no está instalado o no está en el PATH." -ForegroundColor Red
    exit 1
}

Write-Host "Dependencias verificadas correctamente." -ForegroundColor Green

# ------------------------------------------------------------------------------
# 2. INICIALIZACIÓN DE BASE DE DATOS (SQL SERVER)
# ------------------------------------------------------------------------------

Write-Host "[2/4] Inicializando esquemas y tablas en SQL Server..." -ForegroundColor Yellow

sqlcmd `
  -S ".\SQLEXPRESS" `
  -d "salud_analytics" `
  -i "$RootPath\sql\ddl\01_inicializacion_db.sql" `
  -E -C -f 65001

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR al inicializar la base de datos." -ForegroundColor Red
    exit 1
}

Write-Host "Base de datos inicializada correctamente." -ForegroundColor Green

# ------------------------------------------------------------------------------
# 3. EJECUCIÓN DEL PROCESO ETL (PYTHON)
# ------------------------------------------------------------------------------

Write-Host "[3/4] Ejecutando proceso ETL (data_etl.py)..." -ForegroundColor Yellow

python "$RootPath\etl\data_etl.py"

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR en la ejecución del script Python." -ForegroundColor Red
    exit 1
}

Write-Host "ETL ejecutado correctamente." -ForegroundColor Green

# ------------------------------------------------------------------------------
# 4. FINALIZACIÓN Y MÉTRICAS
# ------------------------------------------------------------------------------

$EndTime = Get-Date
$Duration = New-TimeSpan -Start $StartTime -End $EndTime

Write-Host "`n>>> PIPELINE FINALIZADO EXITOSAMENTE <<<" -ForegroundColor Green
Write-Host "Tiempo total: $($Duration.TotalSeconds) segundos." -ForegroundColor White
Write-Host "Estado: PASSED" -ForegroundColor Green