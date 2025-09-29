import pandas as pd
import snowflake.connector
from datetime import datetime
import logging
import time
import numpy as np
from typing import List, Tuple, Dict, Any
import gc

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

logger = logging.getLogger(__name__)

@data_exporter
def export_to_snowflake_optimized(df: pd.DataFrame, *args, **kwargs) -> None:
    from mage_ai.data_preparation.shared.secrets import get_secret_value

    connection_params = {
        'account': get_secret_value('SNOWFLAKE_ACCOUNT'),
        'user': get_secret_value('SNOWFLAKE_USER'),
        'password': get_secret_value('SNOWFLAKE_PASSWORD'),
        'database': get_secret_value('SNOWFLAKE_DATABASE'),
        'warehouse': get_secret_value('SNOWFLAKE_WAREHOUSE'),
        'role': get_secret_value('SNOWFLAKE_ROLE'),
        'schema': kwargs.get('schema_name', 'RAW')
    }

    missing_params = [key for key, value in connection_params.items() if not value]
    if missing_params:
        raise ValueError(f"Missing Snowflake connection parameters: {missing_params}")

    service_type = df['service_type'].iloc[0] if 'service_type' in df.columns else kwargs.get('service_type', 'unknown')
    table_name = kwargs.get('table_name', f"{service_type.upper()}_TRIPS")
    batch_size = kwargs.get('batch_size', 100000)

    start_time = time.time()
    total_rows = len(df)

    print(f"\n{'='*80}")
    print(f"🚀 SNOWFLAKE EXPORT INICIADO")
    print(f"{'='*80}")
    print(f"📊 Dataset: {total_rows:,} filas | Servicio: {service_type}")
    print(f"🎯 Destino: {connection_params['database']}.{connection_params['schema']}.{table_name}")
    print(f"📦 Tamaño batch: {batch_size:,} filas")
    print(f"⏰ Inicio: {datetime.now().strftime('%H:%M:%S')}")

    memory_mb = df.memory_usage(deep=True).sum() / 1024**2
    print(f"💾 Memoria dataset: {memory_mb:.1f} MB")
    print(f"{'='*80}\n")

    conn = snowflake.connector.connect(**connection_params)

    try:
        cursor = conn.cursor()
        create_bronze_table_verbose(cursor, table_name, service_type)

        df_export = prepare_dataframe_verbose(df)

        rows_inserted = insert_dataframe_verbose(cursor, table_name, df_export, batch_size, total_rows)

        elapsed_time = time.time() - start_time
        rows_per_second = rows_inserted / elapsed_time if elapsed_time > 0 else 0

        print(f"\n{'='*80}")
        print(f"✅ EXPORT COMPLETADO EXITOSAMENTE")
        print(f"{'='*80}")
        print(f"📊 Filas procesadas: {rows_inserted:,} / {total_rows:,}")
        print(f"⏱️  Tiempo total: {elapsed_time:.2f}s")
        print(f"🚄 Velocidad: {rows_per_second:.0f} filas/seg")
        print(f"⏰ Finalizado: {datetime.now().strftime('%H:%M:%S')}")
        print(f"{'='*80}\n")

        cursor.close()

    except Exception as e:
        print(f"\n❌ ERROR EN EXPORT: {e}")
        logger.error(f"Export failed: {e}")
        raise
    finally:
        conn.close()
        gc.collect()

def prepare_dataframe_verbose(df: pd.DataFrame) -> pd.DataFrame:
    print("🔧 Preparando dataframe...")
    prep_start = time.time()

    df_clean = df.copy()

    datetime_columns = [col for col in df_clean.columns if 'datetime' in col.lower() or 'timestamp' in col.lower()]
    print(f"   📅 Procesando {len(datetime_columns)} columnas datetime")
    for col in datetime_columns:
        if df_clean[col].dtype == 'object':
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce', utc=False)

    numeric_columns = [col for col in df_clean.columns if any(x in col.lower() for x in ['amount', 'fare', 'tip', 'distance'])]
    print(f"   🔢 Procesando {len(numeric_columns)} columnas numéricas")
    for col in numeric_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce', downcast='float')

    string_columns = df_clean.select_dtypes(include=['object']).columns
    print(f"   📝 Procesando {len(string_columns)} columnas string")
    for col in string_columns:
        if col not in datetime_columns:
            df_clean[col] = df_clean[col].astype('string').fillna('')

    df_clean['SNOWFLAKE_LOADED_AT'] = datetime.now()
    df_clean.columns = [col.upper() for col in df_clean.columns]

    prep_time = time.time() - prep_start
    print(f"   ✅ Dataframe preparado en {prep_time:.2f}s")
    return df_clean

def create_bronze_table_verbose(cursor, table_name: str, service_type: str):
    print(f"🏗️  Verificando tabla {table_name}...")

    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    if cursor.fetchone():
        print(f"   ✅ Tabla {table_name} ya existe")
        return

    print(f"   🔨 Creando tabla {table_name}...")
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        VENDORID NUMBER(3,0),
        TPEP_PICKUP_DATETIME TIMESTAMP_NTZ,
        TPEP_DROPOFF_DATETIME TIMESTAMP_NTZ,
        PASSENGER_COUNT NUMBER(3,0),
        TRIP_DISTANCE NUMBER(8,2),
        RATECODEID NUMBER(3,0),
        STORE_AND_FWD_FLAG STRING(1),
        PULOCATIONID NUMBER(3,0),
        DOLOCATIONID NUMBER(3,0),
        PAYMENT_TYPE NUMBER(3,0),
        FARE_AMOUNT NUMBER(10,2),
        EXTRA NUMBER(10,2),
        MTA_TAX NUMBER(10,2),
        TIP_AMOUNT NUMBER(10,2),
        TOLLS_AMOUNT NUMBER(10,2),
        IMPROVEMENT_SURCHARGE NUMBER(10,2),
        TOTAL_AMOUNT NUMBER(10,2),
        CONGESTION_SURCHARGE NUMBER(10,2),
        AIRPORT_FEE NUMBER(10,2),
        SOURCE_FILE STRING(100),
        FILE_INDEX NUMBER(5,0),
        LOAD_TIMESTAMP TIMESTAMP_NTZ,
        BATCH_ID STRING(50),
        INGESTION_TIMESTAMP TIMESTAMP_NTZ,
        SERVICE_TYPE STRING(10),
        SNOWFLAKE_LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    COMMENT = 'Bronze layer - {service_type} taxi trip data'
    """

    cursor.execute(create_sql)
    print(f"   ✅ Tabla {table_name} creada exitosamente")

def insert_dataframe_verbose(cursor, table_name: str, df: pd.DataFrame, batch_size: int, total_rows: int) -> int:
    columns = list(df.columns)
    total_inserted = 0
    num_batches = (total_rows + batch_size - 1) // batch_size

    print(f"\n📤 INICIANDO INSERCIÓN DE DATOS")
    print(f"   📦 Batches a procesar: {num_batches}")
    print(f"   📊 Filas por batch: {batch_size:,}")
    print(f"   📋 Columnas: {len(columns)}")
    print(f"{'─'*60}")

    overall_start = time.time()

    try:
        for i in range(0, total_rows, batch_size):
            batch_start = time.time()
            batch_number = i // batch_size + 1

            batch_df = df.iloc[i:i + batch_size]
            actual_batch_size = len(batch_df)

            print(f"🔄 Batch {batch_number}/{num_batches}: Procesando {actual_batch_size:,} filas...")

            batch_data = convert_batch_verbose(batch_df, batch_number)

            placeholders = ', '.join(['%s'] * len(columns))
            insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

            print(f"   💾 Insertando en Snowflake...")
            cursor.executemany(insert_sql, batch_data)
            total_inserted += actual_batch_size

            batch_time = time.time() - batch_start
            batch_rate = actual_batch_size / batch_time if batch_time > 0 else 0
            progress = (total_inserted / total_rows) * 100

            elapsed_total = time.time() - overall_start
            avg_rate = total_inserted / elapsed_total if elapsed_total > 0 else 0
            remaining_rows = total_rows - total_inserted
            eta_seconds = remaining_rows / avg_rate if avg_rate > 0 else 0
            eta_minutes = eta_seconds / 60

            print(f"   ✅ Batch {batch_number} completado:")
            print(f"      • Filas: {actual_batch_size:,} en {batch_time:.2f}s ({batch_rate:.0f} filas/seg)")
            print(f"      • Progreso: {progress:.1f}% ({total_inserted:,}/{total_rows:,})")
            print(f"      • Velocidad promedio: {avg_rate:.0f} filas/seg")
            print(f"      • ETA: {eta_minutes:.1f} minutos restantes")
            print(f"{'─'*60}")

            del batch_data, batch_df
            if i % 500000 == 0:
                gc.collect()

    except Exception as e:
        print(f"❌ Error en batch {i//batch_size + 1}: {e}")
        logger.error(f"Insert failed at batch {i//batch_size + 1}: {e}")
        raise

    return total_inserted

def convert_batch_verbose(batch_df: pd.DataFrame, batch_num: int) -> List[Tuple]:
    convert_start = time.time()
    batch_data = []

    for _, row in batch_df.iterrows():
        row_data = []
        for val in row:
            if pd.isna(val):
                row_data.append(None)
            elif isinstance(val, (pd.Timestamp, np.datetime64)):
                if pd.isna(val):
                    row_data.append(None)
                else:
                    row_data.append(pd.to_datetime(val).to_pydatetime())
            elif isinstance(val, (np.integer)):
                row_data.append(int(val))
            elif isinstance(val, (np.floating)):
                if np.isnan(val):
                    row_data.append(None)
                else:
                    row_data.append(float(val))
            elif isinstance(val, bytes):
                row_data.append(val.decode('utf-8', errors='ignore'))
            else:
                row_data.append(str(val) if val is not None else None)

        batch_data.append(tuple(row_data))

    convert_time = time.time() - convert_start
    print(f"   🔧 Conversión completada en {convert_time:.2f}s")
    return batch_data