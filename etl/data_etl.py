import pandas as pd
import sqlalchemy as sql
from sodapy import Socrata # Cliente oficial de Socrata para consumir datasets públicos de datos.gov.co de forma estructurada.
import time # Se utiliza para controlar pausas entre solicitudes HTTP y evitar saturar la API.
from tqdm import tqdm # Barra de progreso para la descarga
# Variables de configuración del API y SQLSERVER

app_token = "RjC4dUfGBI1d8e0a2pGPVjAAH"

datasets = {
    "reps": "c36g-9fc2", # Registro Especial de Prestadores de Servicios de Salud
    "ciips": "s2ru-bqt6" # Capacidad instalada de IPS habilitadas por departamento
}

base_url = "www.datos.gov.co"

client = Socrata(base_url,app_token)

connection_string = (
    "mssql+pyodbc://@localhost\\SQLEXPRESS/salud_analytics?"
    "driver=ODBC+Driver+18+for+SQL+Server&"
    "trusted_connection=yes&"
    "TrustServerCertificate=yes"
)

engine = sql.create_engine(connection_string, fast_executemany=True)

# FUNCIONES 
def get_total_records(dataset_id):
    result = client.get(
        dataset_id,
        select="count(*)"
    )
    return int(result[0]["count"])

# Función para la descarga del dataset

def download_dataset(
    dataset_id: str,
    mode: str = "full",
    limit: int = 1000,
    batch_size: int = 5000,
    max_retries: int = 3,
    sleep_seconds: float = 0.2,
) -> pd.DataFrame:

    if mode not in ["full", "sample"]:
        raise ValueError("mode debe ser 'full' o 'sample'")
    print(f"\nIniciando descarga del dataset {dataset_id} - Modo: {mode}")

    # MODO SAMPLE
    if mode == "sample":
        results = client.get(dataset_id, limit=limit)
        df = pd.DataFrame.from_records(results)

        if df.empty:
            raise Exception("Dataset descargado vacío en modo sample.")

        print(f"Descargados {len(df)} registros (sample).")
        print(f"Columnas detectadas: {list(df.columns)}")
        return df

    # MODO FULL
    total_records = get_total_records(dataset_id)
    all_results = []
    offset = 0

    with tqdm(total=total_records, desc="Descargando", unit=" registros", dynamic_ncols=True, mininterval=0.2, smoothing=0.1 ) as pbar:

        while True:

            for attempt in range(max_retries):
                try:
                    results = client.get(
                        dataset_id,
                        limit=batch_size,
                        offset=offset
                    )
                    break
                except Exception as e:
                    print(f"Error en offset {offset}, intento {attempt + 1}: {e}")
                    time.sleep(1)
            else:
                raise Exception(f"No se pudo descargar batch en offset {offset}")

            if not results:
                break

            batch_count = len(results)
            all_results.extend(results)
            offset += batch_count

            pbar.update(batch_count)
            time.sleep(sleep_seconds)

    df = pd.DataFrame.from_records(all_results)

    if df.empty:
        raise Exception("Dataset descargado vacío.")

    print(f"Descarga completa: {len(df)} registros.")
    print(f"Columnas detectadas: {list(df.columns)}")
    return df

# Función para crear el perfilamiento de los datasets en pandas para revisar la calidad de los datos antes de transformar

def profile_dataframe(df, name):
    print(f"\n--- Perfilamiento {name} ---")
    print("Filas:", df.shape[0])
    print("Columnas:", df.shape[1])
    print("\nNulos por columna:")
    print(df.isnull().sum().sort_values(ascending=False).head(10))
    print("\nDuplicados:", df.duplicated().sum())

# Función para la carga de los datasets a una tabla de SQLSERVER validando estructura contra metadata real

def load_dataframe(
    df, # DataFrame origen
    engine, # Engine SQLAlchemy
    table_name, # Nombre tabla destino
    schema, # Esquema SQL destino
    truncate=False, # Si hace TRUNCATE antes de cargar
    if_exists="append", # Comportamiento de to_sql ('append' o 'replace')
    exclude_columns=None, # Lista de columnas técnicas en DB que no vienen en el DF (Como surrogate_id y fecha_carga)
    drop_duplicates=False, # Si elimina duplicados antes de cargar
    validate_count=True # Valida conteo final
):
    
    if exclude_columns is None:
        exclude_columns = []

    print(f"\nProceso de carga hacia {schema}.{table_name}")

    inspector = sql.inspect(engine)
    db_columns_info = inspector.get_columns(table_name, schema=schema)

    db_columns = {
        col["name"]
        for col in db_columns_info
        if col["name"] not in exclude_columns
    }

    df_columns = set(df.columns)

    missing = db_columns - df_columns
    extra = df_columns - db_columns

    if missing:
        raise ValueError(f"Columnas faltantes en DataFrame: {missing}")

    if extra:
        print(f"Columnas extra serán eliminadas: {extra}")
        df = df[list(db_columns)]

    if drop_duplicates:
        duplicates = df.duplicated().sum()
        if duplicates > 0:
            print(f"Eliminando {duplicates} duplicados.")
            df = df.drop_duplicates()

    print(f"Registros a cargar: {len(df)}")

    with engine.begin() as conn:

        if truncate:
            conn.execute(
                sql.text(f"TRUNCATE TABLE {schema}.{table_name}")
            )
            print(f"Tabla {schema}.{table_name} truncada exitosamente")

        df.to_sql(
            table_name,
            con=conn,
            schema=schema,
            if_exists=if_exists,
            index=False,
            method=None
        )

    if validate_count:
        with engine.connect() as conn:
            result = conn.execute(
                sql.text(f"SELECT COUNT(*) FROM {schema}.{table_name}")
            )
            db_count = result.scalar()

        print(f"Registros en base de datos: {db_count}")

        if truncate and db_count != len(df):
            raise ValueError("Mismatch de conteo después de la carga.")

    print("Carga finalizada correctamente.")

# Función que transforma y estandariza los datos de Staging a Core.
def process_core_layer(engine):
    print("\n=== INICIANDO TRANSFORMACIÓN INTEGRAL (CAPA CORE) ===")
    
    with engine.connect() as conn:
        df_reps_raw = pd.read_sql("SELECT * FROM staging.stg_reps_prestadores", conn)
        df_ciips_raw = pd.read_sql("SELECT * FROM staging.stg_ciips_capacidad_instalada", conn)

    # 1. NORMALIZACIÓN: dim_departamento
    # Extraemos los 2 primeros dígitos de 'municipiosede' para crear el código de depto
    df_depto = df_reps_raw[['municipiosede', 'departamentodededesc']].copy()
    df_depto.columns = ['municipio_codigo', 'departamento_nombre']
    df_depto['departamento_codigo'] = df_depto['municipio_codigo'].astype(str).str.zfill(5).str[:2]
    df_depto['departamento_nombre'] = df_depto['departamento_nombre'].str.upper().str.strip()
    df_depto = df_depto.drop_duplicates(subset=['departamento_codigo'])

    # 2. NORMALIZACIÓN: dim_municipio
    df_municipio = df_reps_raw[['municipiosede', 'municipiosededesc']].copy()
    df_municipio.columns = ['municipio_codigo', 'municipio_nombre']
    df_municipio['departamento_codigo'] = df_municipio['municipio_codigo'].astype(str).str.zfill(5).str[:2]
    df_municipio['municipio_nombre'] = df_municipio['municipio_nombre'].str.upper().str.strip()
    df_municipio = df_municipio.drop_duplicates(subset=['municipio_codigo'])

    # 3. NORMALIZACIÓN: dim_prestador
    df_prestador = df_reps_raw[[
        'codigoprestador', 'numeroidentificacion', 'nombreprestador', 
        'claseprestador', 'naturalezajuridica', 'municipiosede'
    ]].copy()
    df_prestador.columns = ['codigo_prestador', 'nit_prestador', 'prestador_nombre', 
                           'clase_prestador', 'naturaleza_juridica', 'municipio_codigo']
    
    for col in ['prestador_nombre', 'clase_prestador', 'naturaleza_juridica']:
        df_prestador[col] = df_prestador[col].fillna('NO REPORTA').str.upper().str.strip()
    
    df_prestador = df_prestador.drop_duplicates(subset=['nit_prestador'])

    # 4. LIMPIEZA JERÁRQUICA EN SQL
    with engine.begin() as conn:
        print("Vaciando tablas Core para recarga...")
        conn.execute(sql.text("DELETE FROM core.fact_capacidad_instalada"))
        conn.execute(sql.text("DELETE FROM core.dim_prestador"))
        conn.execute(sql.text("DELETE FROM core.dim_municipio"))
        conn.execute(sql.text("DELETE FROM core.dim_departamento"))

    # 5. CARGA DE DIMENSIONES (Orden de Dependencia)
    load_dataframe(df_depto, engine, "dim_departamento", schema="core", truncate=False)
    load_dataframe(df_municipio, engine, "dim_municipio", schema="core", truncate=False)
    load_dataframe(df_prestador, engine, "dim_prestador", schema="core", truncate=False, exclude_columns=['prestador_id'])

    # 6. LOOKUP: OBTENER IDs SUBROGADOS
    with engine.connect() as conn:
        dim_prest_db = pd.read_sql("SELECT prestador_id, nit_prestador FROM core.dim_prestador", conn)

    # 7. NORMALIZACIÓN Y CARGA: fact_capacidad_instalada
    df_fact_cap = df_ciips_raw[[
        'nit_ips', 'nom_grupo_capacidad', 'nom_descripcion_capacidad', 
        'num_cantidad_capacidad_instalada', 'fecha_corte'
    ]].copy()
    df_fact_cap.columns = ['nit_prestador', 'servicio_codigo', 'servicio_nombre', 
                          'capacidad_cantidad', 'fecha_corte_capacidad']

    # Limpieza de datos
    def clean_date(x):
        try: return pd.to_datetime(str(x).replace('Fecha corte REPS: ', '').strip()).date()
        except: return None

    df_fact_cap['fecha_corte_capacidad'] = df_fact_cap['fecha_corte_capacidad'].apply(clean_date)
    df_fact_cap['capacidad_cantidad'] = pd.to_numeric(df_fact_cap['capacidad_cantidad'], errors='coerce').fillna(0).astype(int)
    
    # Cruce con validación (LEFT JOIN)
    df_merge = pd.merge(df_fact_cap, dim_prest_db, on='nit_prestador', how='left')

    # Separación: Válidos vs Rechazados
    df_final = df_merge[df_merge['prestador_id'].notnull()].copy()
    df_rechazos = df_merge[df_merge['prestador_id'].isnull()].copy()

    # 8. CARGA DE REGISTROS VÁLIDOS
    if not df_final.empty:
        df_final = df_final.drop(columns=['nit_prestador'])
        load_dataframe(df_final, engine, "fact_capacidad_instalada", schema="core", 
                       truncate=False, exclude_columns=['capacidad_id'])

    # 9. CARGA DE RECHAZOS (Si existen) 
    if not df_rechazos.empty:
        df_rechazos_to_load = pd.DataFrame({
            'motivo_rechazo': 'NIT NO ENCONTRADO EN MAESTRO PRESTADORES (REPS)',        
            'tabla_origen': 'stg_ciips_capacidad_instalada',
            'registro_crudo': df_rechazos.apply(lambda row: row.to_json(), axis=1),
            'fecha_rechazo': pd.Timestamp.now()                       
        })
        load_dataframe(df_rechazos_to_load, engine, "stg_rechazos", schema="staging", truncate=True,exclude_columns=["rechazo_id"])
        print(f"ALERTA: {len(df_rechazos_to_load)} registros enviados a la tabla de RECHAZOS.")
    else:
        print("Calidad de datos: 100%. No se generaron rechazos.")

    print("\n=== PROCESO DE CARGA, ESTANDARIZACIÓN Y AUDITORÍA FINALIZADO ===")

# Función que realiza validaciones post-carga para asegurar la integridad del modelo.
def run_data_quality_tests(engine):

    print("\n=== INICIANDO TESTS DE CALIDAD DE DATOS (POST-CARGA) ===")
    results = []

    with engine.connect() as conn:
        # Test 1: ¿Existen registros huérfanos en la tabla de hechos?
        query_huerfanos = """
            SELECT COUNT(*) FROM core.fact_capacidad_instalada f
            LEFT JOIN core.dim_prestador p ON f.prestador_id = p.prestador_id
            WHERE p.prestador_id IS NULL
        """
        huerfanos = conn.execute(sql.text(query_huerfanos)).scalar()
        test_1 = "PASSED" if huerfanos == 0 else "FAILED"
        results.append(f"Test 1 - Integridad Referencial (0 huérfanos): {test_1} ({huerfanos} detectados)")

        # Test 2: ¿Hay campos obligatorios con nulos en Dimensiones?
        query_nulos = "SELECT COUNT(*) FROM core.dim_prestador WHERE prestador_nombre IS NULL"
        nulos = conn.execute(sql.text(query_nulos)).scalar()
        test_2 = "PASSED" if nulos == 0 else "FAILED"
        results.append(f"Test 2 - No nulos en campos clave (dim_prestador): {test_2}")

        # Test 3: Cuadre de registros (Fuente vs Core + Rechazos)
        # Este test valida que no se perdió información en el camino
        count_stg = conn.execute(sql.text("SELECT COUNT(*) FROM staging.stg_ciips_capacidad_instalada")).scalar()
        count_core = conn.execute(sql.text("SELECT COUNT(*) FROM core.fact_capacidad_instalada")).scalar()
        count_rech = conn.execute(sql.text("SELECT COUNT(*) FROM staging.stg_rechazos WHERE tabla_origen = 'stg_ciips_capacidad_instalada'")).scalar()
        
        test_3 = "PASSED" if count_stg == (count_core + count_rech) else "WARNING"
        results.append(f"Test 3 - Balance de registros (Staging vs Core+Rechazos+Eliminados): {test_3} (STG:{count_stg} | CORE:{count_core} | RECH:{count_rech})")

    for res in results:
        print(f"[DATA QUALITY] {res}")
    
    return results

# Registra el resultado de la ejecución en la tabla etl_log.
def log_etl_execution(engine, table_name, read, inserted, rejected, duration, status):
    query = f"""
    INSERT INTO staging.etl_log (tabla_destino, registros_leidos, registros_insertados, registros_rechazados, duracion_segundos, estado)
    VALUES ('{table_name}', {read}, {inserted}, {rejected}, {int(duration)}, '{status}')
    """
    with engine.begin() as conn:
        conn.execute(sql.text(query))

def main():
    start_time = time.time()
    print("+++ INICIANDO PIPELINE ETL DATOS ABIERTOS DE SALUD +++")
    start_all = time.time()

    try:
        # --- PASO 1: EXTRACCIÓN ---
        print("\n[1/4] Extrayendo datos de la API pública...")
        df_reps = download_dataset(dataset_id=datasets["reps"])
        df_ciips = download_dataset(dataset_id=datasets["ciips"])

        # --- PASO 2: STAGING ---
        print("\n[2/4] Cargando capa de Staging...")
        
        # Carga REPS
        t_start = time.time()
        load_dataframe(df_reps, engine, "stg_reps_prestadores", schema="staging", truncate=True, exclude_columns=["stg_reps_id","fecha_carga"],drop_duplicates=False)
        log_etl_execution(engine, "stg_reps_prestadores", len(df_reps), len(df_reps), 0, time.time()-t_start, "EXITOSO")

        # Carga CIIPS
        t_start = time.time()
        load_dataframe(df_ciips, engine, "stg_ciips_capacidad_instalada", schema="staging", truncate=True,exclude_columns=["stg_ciips_id","fecha_carga"],drop_duplicates=False)
        log_etl_execution(engine, "stg_ciips_capacidad_instalada", len(df_ciips), len(df_ciips), 0, time.time()-t_start, "EXITOSO")

        # --- PASO 3: CORE & TRANSFORMACIÓN ---
        print("\n[3/4] Ejecutando transformaciones y carga a capa Core...")
        # Esta función internamente ya maneja sus propios logs de éxito/rechazo
        process_core_layer(engine)

        # --- PASO 4: CALIDAD ---
        print("\n[4/4] Ejecutando Tests de Calidad de Datos...")
        run_data_quality_tests(engine)

        total_duration = time.time() - start_all
        print(f"\nPIPELINE FINALIZADO EXITOSAMENTE EN {total_duration:.2f} SEGUNDOS")
        log_etl_execution(engine, "PIPELINE_GLOBAL", 0, 0, 0, total_duration, "COMPLETO")

    except Exception as e:
        print(f"\n ERROR CRÍTICO: {str(e)}")
        log_etl_execution(engine, "PIPELINE_GLOBAL", 0, 0, 0, 0, f"FALLIDO: {str(e)[:50]}")

if __name__ == "__main__":
    main()