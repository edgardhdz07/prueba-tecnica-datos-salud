-- =============================================
-- CONFIGURACIÓN DE ENTORNO
-- =============================================

SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
GO

-- 1. ESQUEMAS
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'staging') BEGIN EXEC('CREATE SCHEMA staging') END
GO
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'core') BEGIN EXEC('CREATE SCHEMA core') END
GO

-- 2. LIMPIEZA JERÁRQUICA
IF OBJECT_ID('core.fact_capacidad_instalada', 'U') IS NOT NULL DROP TABLE core.fact_capacidad_instalada;
IF OBJECT_ID('core.dim_prestador', 'U') IS NOT NULL DROP TABLE core.dim_prestador;
IF OBJECT_ID('core.dim_municipio', 'U') IS NOT NULL DROP TABLE core.dim_municipio;
IF OBJECT_ID('core.dim_departamento', 'U') IS NOT NULL DROP TABLE core.dim_departamento;
IF OBJECT_ID('staging.stg_rechazos', 'U') IS NOT NULL DROP TABLE staging.stg_rechazos;
GO

-- 3. TABLAS DE DIMENSIONES
CREATE TABLE core.dim_departamento (
    departamento_codigo VARCHAR(5) PRIMARY KEY,
    departamento_nombre NVARCHAR(150)
);
GO

CREATE TABLE core.dim_municipio (
    municipio_codigo VARCHAR(10) PRIMARY KEY,
    municipio_nombre NVARCHAR(150),
    departamento_codigo VARCHAR(5) FOREIGN KEY REFERENCES core.dim_departamento(departamento_codigo)
);
GO

CREATE TABLE core.dim_prestador (
    prestador_id INT IDENTITY(1,1) PRIMARY KEY,
    codigo_prestador VARCHAR(50),
    nit_prestador VARCHAR(20),
    prestador_nombre NVARCHAR(255),
    clase_prestador NVARCHAR(100),
    naturaleza_juridica NVARCHAR(100),
    municipio_codigo VARCHAR(10) FOREIGN KEY REFERENCES core.dim_municipio(municipio_codigo)
);

CREATE INDEX IX_dim_prestador_nit ON core.dim_prestador (nit_prestador);
GO

-- 4. TABLA DE HECHOS
CREATE TABLE core.fact_capacidad_instalada (
    capacidad_id INT IDENTITY(1,1) PRIMARY KEY,
    prestador_id INT NOT NULL FOREIGN KEY REFERENCES core.dim_prestador(prestador_id),
    servicio_codigo VARCHAR(100),
    servicio_nombre NVARCHAR(500),
    capacidad_cantidad INT,
    fecha_corte_capacidad DATE
);

CREATE INDEX IX_fact_prestador_id ON core.fact_capacidad_instalada (prestador_id);
GO

-- 5. TABLA DE RECHAZOS Y LOGS
CREATE TABLE staging.stg_rechazos (
    rechazo_id INT IDENTITY(1,1) PRIMARY KEY,
    motivo_rechazo NVARCHAR(255),
    tabla_origen VARCHAR(100),
    registro_crudo NVARCHAR(MAX),
    fecha_rechazo DATETIME DEFAULT GETDATE()
);
GO

IF OBJECT_ID('staging.etl_log', 'U') IS NULL
BEGIN
    CREATE TABLE staging.etl_log (
        log_id INT IDENTITY(1,1) PRIMARY KEY,
        nombre_proceso VARCHAR(100),
        fecha_inicio DATETIME,
        fecha_fin DATETIME,
        registros_procesados INT,
        registros_cargados INT,
        registros_rechazados INT,
        estado VARCHAR(20),
        mensaje_error NVARCHAR(MAX)
    );
END
GO

-- ==============================================================================
-- 6. VISTAS DE NEGOCIO (CAPA DE CONSUMO)
-- ==============================================================================

CREATE OR ALTER VIEW [core].[vw_perfil_detallado_prestadores] AS
SELECT 
    p.prestador_nombre AS Nombre_Institucion,
    p.nit_prestador AS Numero_NIT,
    p.clase_prestador AS Clase_IPS,
    p.naturaleza_juridica AS Naturaleza_Legal,
    f.servicio_nombre AS Descripcion_Servicio,
    f.capacidad_cantidad AS Cantidad_Reportada,
    f.fecha_corte_capacidad AS Fecha_Ultimo_Reporte
FROM core.fact_capacidad_instalada f
INNER JOIN core.dim_prestador p ON f.prestador_id = p.prestador_id;
GO

CREATE OR ALTER VIEW [core].[vw_estado_operativo_red_salud] AS
SELECT 
    p.prestador_nombre AS Institucion,
    p.clase_prestador AS Tipo_Prestador,
    f.servicio_nombre AS Grupo_Servicio,
    f.capacidad_cantidad AS Unidades,
    f.fecha_corte_capacidad AS Fecha_Corte,
    CASE 
        WHEN DATEDIFF(DAY, f.fecha_corte_capacidad, GETDATE()) > 365 THEN N'Información Desactualizada'
        WHEN DATEDIFF(DAY, f.fecha_corte_capacidad, GETDATE()) > 180 THEN N'Requiere Actualización'
        ELSE N'Información Vigente'
    END AS Semaforo_Vigencia
FROM core.fact_capacidad_instalada f
INNER JOIN core.dim_prestador p ON f.prestador_id = p.prestador_id;
GO

-- ==============================================================================
-- 7. DOCUMENTACIÓN (PROPIEDADES EXTENDIDAS)
-- ==============================================================================


-- Departamento
EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'Código DANE de 2 dígitos del departamento.', 
    @level0type = N'SCHEMA', @level0name = 'core', 
    @level1type = N'TABLE', @level1name = 'dim_departamento', 
    @level2type = N'COLUMN', @level2name = 'departamento_codigo';

-- Municipio
EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'Código DANE de 5 dígitos (Depto + Municipio).', 
    @level0type = N'SCHEMA', @level0name = 'core', 
    @level1type = N'TABLE', @level1name = 'dim_municipio', 
    @level2type = N'COLUMN', @level2name = 'municipio_codigo';

-- prestador_id
EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Identificador único del prestador (clave primaria)',
    @level0type = N'SCHEMA', @level0name = 'core',
    @level1type = N'TABLE',  @level1name = 'dim_prestador',
    @level2type = N'COLUMN', @level2name = 'prestador_id';

-- nit_prestador
EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'NIT oficial del prestador de servicios de salud',
    @level0type = N'SCHEMA', @level0name = 'core',
    @level1type = N'TABLE',  @level1name = 'dim_prestador',
    @level2type = N'COLUMN', @level2name = 'nit_prestador';

-- prestador_nombre
EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Nombre oficial registrado en REPS',
    @level0type = N'SCHEMA', @level0name = 'core',
    @level1type = N'TABLE',  @level1name = 'dim_prestador',
    @level2type = N'COLUMN', @level2name = 'prestador_nombre';

-- clase_prestador
EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Clasificación del prestador según categoría REPS',
    @level0type = N'SCHEMA', @level0name = 'core',
    @level1type = N'TABLE',  @level1name = 'dim_prestador',
    @level2type = N'COLUMN', @level2name = 'clase_prestador';

-- Descripción de la Tabla de Hechos
EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'Registro centralizado de la capacidad operativa (camas, consultorios, etc.) reportada por las IPS.', 
    @level0type = N'SCHEMA', @level0name = 'core', 
    @level1type = N'TABLE', @level1name = 'fact_capacidad_instalada';

-- capacidad_cantidad
EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'Cantidad física reportada para el servicio específico.', 
    @level0type = N'SCHEMA', @level0name = 'core', 
    @level1type = N'TABLE', @level1name = 'fact_capacidad_instalada', 
    @level2type = N'COLUMN', @level2name = 'capacidad_cantidad';

-- servicio_nombre
EXEC sp_addextendedproperty 
    @name = N'MS_Description', @value = N'Nombre del servicio de salud según habilitación REPS.', 
    @level0type = N'SCHEMA', @level0name = 'core', 
    @level1type = N'TABLE', @level1name = 'fact_capacidad_instalada', 
    @level2type = N'COLUMN', @level2name = 'servicio_nombre';

-- fecha_corte_capacidad
EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'Fecha de vigencia de la capacidad reportada por la institución.', 
    @level0type = N'SCHEMA', @level0name = 'core', 
    @level1type = N'TABLE', @level1name = 'fact_capacidad_instalada', 
    @level2type = N'COLUMN', @level2name = 'fecha_corte_capacidad';
