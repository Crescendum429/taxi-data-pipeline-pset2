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
    enable_compression = kwargs.get('enable_compression', True)

    start_time = time.time()
    logger.info(f"Exporting to {connection_params['database']}.{connection_params['schema']}.{table_name}")
    logger.info(f"Rows: {len(df):,}, Service: {service_type}, Batch size: {batch_size:,}")

    conn = snowflake.connector.connect(**connection_params)

    try:
        optimize_session(conn)

        cursor = conn.cursor()
        create_bronze_table_optimized(cursor, table_name, service_type)

        df_export = prepare_dataframe_optimized(df)

        rows_inserted = insert_dataframe_ultra_optimized(cursor, table_name, df_export, batch_size)

        elapsed_time = time.time() - start_time
        rows_per_second = rows_inserted / elapsed_time if elapsed_time > 0 else 0
        mb_per_second = (df_export.memory_usage(deep=True).sum() / 1024**2) / elapsed_time if elapsed_time > 0 else 0

        logger.info(f"Export completed: {rows_inserted:,} rows in {elapsed_time:.2f}s")
        logger.info(f"Performance: {rows_per_second:.0f} rows/sec, {mb_per_second:.1f} MB/sec")

        cursor.close()

    finally:
        conn.close()
        gc.collect()

def optimize_session(conn) -> None:
    cursor = conn.cursor()
    try:
        cursor.execute("ALTER SESSION SET TIMEZONE = 'UTC'")
        cursor.execute("ALTER SESSION SET TIMESTAMP_TYPE_MAPPING = 'TIMESTAMP_NTZ'")
        cursor.execute("ALTER SESSION SET JDBC_QUERY_RESULT_FORMAT = 'JSON'")
        cursor.execute("ALTER SESSION SET CLIENT_MEMORY_LIMIT = 2048")
        cursor.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 3600")
    except Exception as e:
        logger.warning(f"Session optimization warning: {e}")
    finally:
        cursor.close()

def prepare_dataframe_optimized(df: pd.DataFrame) -> pd.DataFrame:
    df_clean = df.copy()

    datetime_columns = [col for col in df_clean.columns if 'datetime' in col.lower() or 'timestamp' in col.lower()]
    for col in datetime_columns:
        if df_clean[col].dtype == 'object':
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce', utc=False)

    numeric_columns = [col for col in df_clean.columns if any(x in col.lower() for x in ['amount', 'fare', 'tip', 'distance'])]
    for col in numeric_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce', downcast='float')

    string_columns = df_clean.select_dtypes(include=['object']).columns
    for col in string_columns:
        if col not in datetime_columns:
            df_clean[col] = df_clean[col].astype('string').fillna('')

    df_clean['SNOWFLAKE_LOADED_AT'] = datetime.now()

    df_clean.columns = [col.upper() for col in df_clean.columns]

    memory_usage_mb = df_clean.memory_usage(deep=True).sum() / 1024**2
    logger.info(f"Prepared dataframe: {df_clean.shape}, Memory: {memory_usage_mb:.1f} MB")

    return df_clean

def create_bronze_table_optimized(cursor, table_name: str, service_type: str):
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    if cursor.fetchone():
        return

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
        SNOWFLAKE_LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        BRONZE_CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        BRONZE_RUN_ID STRING(100) DEFAULT 'MAGE_' || REPLACE(REPLACE(CURRENT_TIMESTAMP()::STRING, ' ', '_'), ':', '-')
    )
    CLUSTER BY (TPEP_PICKUP_DATETIME, SERVICE_TYPE)
    COMMENT = 'Bronze layer - {service_type} taxi trip data optimized'
    """

    cursor.execute(create_sql)
    logger.info(f"Optimized table {table_name} created with clustering")

def insert_dataframe_ultra_optimized(cursor, table_name: str, df: pd.DataFrame, batch_size: int = 100000) -> int:
    columns = list(df.columns)
    total_rows = len(df)
    total_inserted = 0

    logger.info(f"Starting ultra-optimized insert with batch size: {batch_size:,}")

    cursor.execute("BEGIN")

    try:
        for i in range(0, total_rows, batch_size):
            batch_start = time.time()
            batch_df = df.iloc[i:i + batch_size]
            actual_batch_size = len(batch_df)

            batch_data = convert_batch_optimized(batch_df)

            placeholders = ', '.join(['%s'] * len(columns))
            insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

            cursor.executemany(insert_sql, batch_data)
            total_inserted += actual_batch_size

            batch_time = time.time() - batch_start
            batch_rate = actual_batch_size / batch_time if batch_time > 0 else 0
            progress = (total_inserted / total_rows) * 100

            logger.info(f"Batch {i//batch_size + 1}: {actual_batch_size:,} rows in {batch_time:.2f}s ({batch_rate:.0f} rows/sec) - Progress: {progress:.1f}%")

            del batch_data, batch_df
            gc.collect()

        cursor.execute("COMMIT")

    except Exception as e:
        cursor.execute("ROLLBACK")
        logger.error(f"Transaction rolled back due to error: {e}")
        raise

    return total_inserted

def convert_batch_optimized(batch_df: pd.DataFrame) -> List[Tuple]:
    batch_data = []

    numpy_arrays = {col: batch_df[col].values for col in batch_df.columns}

    for i in range(len(batch_df)):
        row_data = []
        for col in batch_df.columns:
            val = numpy_arrays[col][i]

            if pd.isna(val):
                row_data.append(None)
            elif isinstance(val, (pd.Timestamp, np.datetime64)):
                if pd.isna(val):
                    row_data.append(None)
                else:
                    row_data.append(pd.to_datetime(val).to_pydatetime())
            elif isinstance(val, (np.integer, np.floating)):
                if np.isnan(val):
                    row_data.append(None)
                else:
                    row_data.append(float(val) if isinstance(val, np.floating) else int(val))
            elif isinstance(val, bytes):
                row_data.append(val.decode('utf-8', errors='ignore'))
            else:
                row_data.append(str(val) if val is not None else None)

        batch_data.append(tuple(row_data))

    return batch_data

def log_performance_metrics(df: pd.DataFrame, start_time: float, rows_inserted: int):
    elapsed_time = time.time() - start_time
    rows_per_second = rows_inserted / elapsed_time if elapsed_time > 0 else 0
    memory_usage_mb = df.memory_usage(deep=True).sum() / 1024**2
    mb_per_second = memory_usage_mb / elapsed_time if elapsed_time > 0 else 0

    logger.info(f"Performance Summary:")
    logger.info(f"  Total time: {elapsed_time:.2f}s")
    logger.info(f"  Rows/second: {rows_per_second:,.0f}")
    logger.info(f"  Memory processed: {memory_usage_mb:.1f} MB")
    logger.info(f"  MB/second: {mb_per_second:.1f}")
    logger.info(f"  Average row size: {memory_usage_mb * 1024 / len(df):.1f} KB")