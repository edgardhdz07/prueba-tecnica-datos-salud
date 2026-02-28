# Prueba Técnica – Ingeniería de Datos

## 1. Descripción General

Este repositorio contiene el modelo de datos desarrollado en SQL Server como parte de una prueba técnica para el rol de Ingeniero de Datos.

El objetivo del modelo es soportar análisis de capacidad instalada y habilitación de prestadores de servicios de salud en Colombia, bajo un enfoque estructurado, documentado y alineado con buenas prácticas de ingeniería de datos.

---

## 2. Arquitectura Técnica y Requisitos del Sistema

- **Modelo de Datos:** Snowflake Schema (Esquemas `core` para negocio y `staging` para auditoría).
- **Base de Datos:** SQL Server Express (`.\SQLEXPRESS`).
- **Lenguajes:** Python (ETL) y SQL (DDL/DML).

API SOCRATA → Python ETL → SQL (DDL + Views) → Power BI

---

### Motor de Base de Datos

- Microsoft SQL Server Express 2017 o superior
- Instancia utilizada: `.\SQLEXPRESS`
- Nivel de compatibilidad recomendado: 140+

---

### Entorno Python

- Python 3.9 o superior

#### Librerías utilizadas

#### Pueden instalarse ejecutando
```python pip install -r requirements.txt```

```python
import pandas as pd
import sqlalchemy as sql
from sodapy import Socrata
import time
from tqdm import tqdm
```

### Justificación Técnica de Librerías

#### pandas
Utilizada para transformación, limpieza y normalización de datos.  
Permite trabajar con estructuras tipo DataFrame, facilitando procesos ETL intermedios antes de la carga a SQL Server.

---

#### sqlalchemy
Proporciona una capa de abstracción para la conexión a SQL Server.  
Permite manejar el motor de base de datos de forma desacoplada y facilita la inserción eficiente mediante `to_sql()`.

---

#### pyodbc
Driver requerido por SQLAlchemy para conectarse a SQL Server vía ODBC.  
Es necesario para establecer comunicación con la instancia `.\SQLEXPRESS`.

---

#### sodapy
Cliente oficial de Socrata utilizado para consumir datasets públicos de datos.gov.co.  

Se incorporó adicionalmente porque:

- Maneja autenticación y paginación automáticamente.
- Reduce errores en solicitudes HTTP manuales.
- Permite consumir grandes volúmenes de datos de manera estructurada.
- Es más robusto que realizar llamadas directas con `requests`.

---

#### tqdm
Librería utilizada para mostrar barra de progreso durante la descarga de datos.  
Aporta visibilidad y trazabilidad al proceso ETL cuando se manejan grandes volúmenes de información.

---

#### time
Utilizado para controlar pausas entre solicitudes HTTP, evitando saturar la API pública y reduciendo el riesgo de bloqueo por límite de peticiones.


### Herramientas utilizadas
- SQL Server Management Studio (SSMS) 18+
- Visual Studio Code
- PowerShell (para ejecución del pipeline automatizado)
- Git (control de versiones)
- Power BI Desktop

### Permisos Requeridos
El usuario que ejecute los scripts debe contar con permisos para:

- `CREATE SCHEMA`
- `CREATE TABLE`
- `CREATE VIEW`
- `ALTER`
- `CREATE INDEX`
- `EXECUTE` sobre `sp_addextendedproperty`

### Configuración Esperada
- Base de datos previamente creada
- Collation compatible con `Latin1_General_CI_AS` (recomendado)
- Compatibilidad mínima nivel 140+

## 3. Selección y Justificación de Datasets

Para el desarrollo del modelo se seleccionaron dos conjuntos de datos principales:

### Dataset 1 – Información de Prestadores (REPS)

Este dataset contiene la información estructural de los prestadores de servicios de salud, incluyendo:

- Identificación (NIT)
- Nombre del prestador
- Ubicación geográfica
- Clasificación del prestador

**Justificación:**

- Representa la entidad principal del dominio de negocio.
- Permite construir dimensiones geográficas y organizacionales.
- Es estable en el tiempo, lo que facilita modelado tipo dimensión.
- Es fundamental para cualquier análisis de oferta en el sector salud.

Este dataset se modeló principalmente en:

- `dim_prestador`
- `dim_departamento`
- `dim_municipio`

---

### Dataset 2 – Capacidad Instalada (CIIPS)

Este dataset contiene la capacidad habilitada por servicio para cada prestador, incluyendo:

- Tipo de servicio
- Cantidad habilitada
- Fecha de corte

**Justificación:**

- Representa una métrica cuantificable (hecho).
- Permite análisis agregados por geografía, servicio y prestador.
- Es un dataset naturalmente transaccional / medible.
- Complementa el dataset estructural con datos analíticos.

Este dataset se modeló principalmente en:

- `fact_capacidad_instalada`

---

### Enfoque de Modelado

La combinación de ambos datasets permitió:

- Separar entidades descriptivas (dimensiones)
- Centralizar métricas en una tabla de hechos
- Facilitar agregaciones analíticas
- Construir vistas optimizadas para consulta

La selección responde a un enfoque orientado a análisis de oferta en salud, priorizando datasets complementarios (estructura + capacidad) que permiten generar valor analítico real.


## 4. Arquitectura del Modelo

El modelo sigue un enfoque dimensional simplificado:

### 🔹Tablas Dimensión
- `core.dim_departamento`
- `core.dim_municipio`
- `core.dim_prestador`

### 🔹 Tabla de Hechos
- `core.fact_capacidad_instalada`

### 🔹 Vistas Analíticas
- `core.vw_capacidad_por_prestador`
- `core.vw_capacidad_por_departamento`
- `core.vw_capacidad_detalle`
- `core.vw_diccionario_datos`

---

## 5. Características Técnicas Implementadas

- Modelado estructurado por esquema
- Claves primarias y foráneas definidas
- Tipos de datos consistentes
- Integridad referencial
- Vista técnica de diccionario de datos
- Documentación integrada mediante `MS_Description`
- Scripts organizados para ejecución ordenada
- Separación lógica de responsabilidades (DDL, constraints, documentación)

---

## 6. Instalación

1. Crear la base de datos (si no existe):

```sql
CREATE DATABASE SaludAnalytics;
GO
````

2. Ejecutar los scripts en orden numérico dentro de la base de datos:

```
01_create_schema.sql
02_create_tables.sql
03_constraints_fk.sql
04_create_views.sql
05_extended_properties.sql
06_indexes.sql
```

3. Validar creación de objetos:

```sql
SELECT name 
FROM sys.tables 
WHERE schema_id = SCHEMA_ID('core');
```

4. Validar documentación técnica:

```sql
SELECT *
FROM core.vw_diccionario_datos
ORDER BY tabla_vista, column_id;
```

---

## 7. Estructura del Proyecto

```
prueda_datos_salud/
│
├── README.md
├── .gitignore
├── requirements.txt
├── LICENSE
│
├── data/
│   ├── Registro_Especial_de_Prestadores_y_Sedes_de_Servicios_de_Salud_20260224.csv
│   └── Relacion_de_IPS_publicas_y_privadas_según_el_nivel_de_atencion_y_capacidad_instalada_20260226.csv
│
├── etl/
│   └── data_etl.py
│
├── sql/
│   ├── ddl/
│   │   └── 01_inicializacion_db.sql
│   ├── views/
│   │   └── query_diccionario_datos_base_salud.sql
│
├── scripts/
│   └── run_pipeline.ps1
│
├── docs/
│   └── Diccionario_Datos_Salud.xlsx
│
└── bi/
    └── consumo_vistas.pbix
```

---

## 8. Documentación

El modelo incluye documentación técnica integrada en el motor de base de datos mediante Extended Properties (`MS_Description`).

Adicionalmente, se dispone de una vista de diccionario técnico:

```sql
SELECT *
FROM core.vw_diccionario_datos;
```

El diccionario de datos también se encuentra disponible en formato Excel dentro del repositorio:

```/docs/Diccionario_Datos_Salud.xlsx```

Este archivo contiene la descripción funcional de tablas, columnas, tipos de datos y consideraciones de negocio, y se entrega como soporte documental complementario a la documentación embebida en la base de datos.

---

## 9. Automatización del Pipeline

El proyecto incluye un script de automatización que ejecuta de forma secuencial todos los scripts de base de datos:


```/scripts/run_pipeline.ps1```


Este script permite:

- Crear el esquema
- Crear tablas
- Aplicar restricciones
- Crear vistas
- Aplicar documentación (`MS_Description`)
- Crear índices

### Ejecución

Desde PowerShell se puede ejecutar el pipeline completo:

```powershell
.\scripts\run_pipeline.ps1
```

## 10. Decisiones de Diseño

* Separación clara entre dimensiones y hechos.
* Uso de claves surrogate (`INT IDENTITY`) para control interno.
* Estandarización de nomenclatura.
* Preparado para escalabilidad futura (históricos, particionamiento, SCD).
* Documentación embebida a nivel de metadatos del motor.

---

## 11. Declaración de Uso de Inteligencia Artificial

Durante el desarrollo de esta prueba técnica se utilizaron herramientas de Inteligencia Artificial generativa (ChatGPT y Google Gemini) como apoyo en tareas específicas tales como:

* Revisión de estructura sintáctica SQL.
* Optimización de consultas.
* Redacción de documentación técnica y README..
* Validación de buenas prácticas.

Las decisiones de modelado, diseño arquitectónico, implementación de integridad referencial, definición de tipos de datos y validación final del código fueron realizadas y verificadas manualmente por el autor.

La responsabilidad técnica del diseño e implementación del modelo recae completamente en el autor de esta entrega.

---

## 12. Versión

| Versión | Fecha      | Descripción                    |
| ------- | -------    | ------------------------------ |
| 1.0.0   | 2026-02-29 | Entrega inicial prueba técnica |

---

## 13. Autor

### Prueba técnica presentada para proceso de selección – Ingeniero de Datos.
#### Candidato: Edgar David Hernández  Medina

## License

This project is licensed under the MIT License - see the LICENSE file for details.