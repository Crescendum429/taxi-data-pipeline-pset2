import pandas as pd
import snowflake.connector
from datetime import datetime
import logging
import time
import numpy as np
from typing import List, Tuple
import gc

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

logger = logging.getLogger(__name__)

@data_exporter
def export_to_snowflake_bronze(df: pd.DataFrame, *args, **kwargs) -> None:
    from mage_ai.data_preparation.shared.secrets import get_secret_value

    connection_params = {
        'account': get_secret_value('SNOWFLAKE_ACCOUNT'),
        'user': get_secret_value('SNOWFLAKE_USER'),
        'password': get_secret_value('SNOWFLAKE_PASSWORD'),
        'database': get_secret_value('SNOWFLAKE_DATABASE'),
        'warehouse': get_secret_value('SNOWFLAKE_WAREHOUSE'),
        'role': get_secret_value('SNOWFLAKE_ROLE'),
        'schema': 'RAW'
    }

    missing_params = [key for key, value in connection_params.items() if not value]
    if missing_params:
        raise ValueError(f"Missing Snowflake connection parameters: {missing_params}")

    service_type = df['service_type'].iloc[0] if 'service_type' in df.columns else 'unknown'
    table_name = "YELLOW_TRIPS"
    batch_size = 50000

    start_time = time.time()
    total_rows = len(df)
    memory_mb = df.memory_usage(deep=True).sum() / 1024**2

    logger.info("SNOWFLAKE BRONZE LAYER EXPORT STARTING")
    logger.info(f"Target table: {connection_params['database']}.{connection_params['schema']}.{table_name}")
    logger.info(f"Dataset: {total_rows:,} rows | {memory_mb:.1f} MB | Service: {service_type}")
    logger.info(f"Batch size: {batch_size:,} rows")

    conn = snowflake.connector.connect(**connection_params)

    try:
        cursor = conn.cursor()

        create_bronze_table(cursor, table_name)
        df_export = prepare_dataframe(df)
        rows_inserted = insert_dataframe_optimized(cursor, table_name, df_export, batch_size, total_rows)

        elapsed_time = time.time() - start_time
        rows_per_second = rows_inserted / elapsed_time if elapsed_time > 0 else 0

        logger.info("BRONZE LAYER EXPORT COMPLETED")
        logger.info(f"Rows processed: {rows_inserted:,} / {total_rows:,}")
        logger.info(f"Total time: {elapsed_time:.2f}s")
        logger.info(f"Performance: {rows_per_second:.0f} rows/sec")

        cursor.close()

    except Exception as e:
        logger.error(f"Export failed: {e}")
        raise
    finally:
        conn.close()
        gc.collect()

def create_bronze_table(cursor, table_name: str):
    logger.info(f"Creating bronze table: {table_name}")

    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    if cursor.fetchone():
        logger.info(f"Bronze table {table_name} already exists")
        return

    create_sql = f"""
    CREATE TABLE {table_name} (
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
        BRONZE_LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        BRONZE_RUN_ID STRING(100) DEFAULT 'MAGE_' || REPLACE(REPLACE(CURRENT_TIMESTAMP()::STRING, ' ', '_'), ':', '-')
    )
    CLUSTER BY (TPEP_PICKUP_DATETIME, SERVICE_TYPE)
    COMMENT = 'Bronze layer - Raw Yellow and Green taxi trip data 2015-2025'
    """

    cursor.execute(create_sql)
    logger.info(f"Bronze table {table_name} created with clustering on (TPEP_PICKUP_DATETIME, SERVICE_TYPE)")

def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Preparing dataframe for bronze layer")

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

    df_clean['BRONZE_LOADED_AT'] = datetime.now()
    df_clean.columns = [col.upper() for col in df_clean.columns]

    logger.info(f"Dataframe prepared: {df_clean.shape[0]:,} rows, {df_clean.shape[1]} columns")
    return df_clean

def insert_dataframe_optimized(cursor, table_name: str, df: pd.DataFrame, batch_size: int, total_rows: int) -> int:
    columns = list(df.columns)
    total_inserted = 0
    num_batches = (total_rows + batch_size - 1) // batch_size

    logger.info(f"Starting batch insert: {num_batches} batches of {batch_size:,} rows")

    cursor.execute("BEGIN")

    try:
        for i in range(0, total_rows, batch_size):
            batch_start = time.time()
            batch_number = i // batch_size + 1

            batch_df = df.iloc[i:i + batch_size]
            actual_batch_size = len(batch_df)

            batch_data = convert_batch_simple(batch_df)

            placeholders = ', '.join(['%s'] * len(columns))
            insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

            cursor.executemany(insert_sql, batch_data)
            total_inserted += actual_batch_size

            batch_time = time.time() - batch_start
            batch_rate = actual_batch_size / batch_time if batch_time > 0 else 0
            progress = (total_inserted / total_rows) * 100

            logger.info(f"Batch {batch_number}/{num_batches}: {actual_batch_size:,} rows in {batch_time:.2f}s ({batch_rate:.0f} rows/sec) - {progress:.1f}% complete")

            del batch_data, batch_df
            if batch_number % 10 == 0:
                gc.collect()

        cursor.execute("COMMIT")
        logger.info("Bronze layer transaction committed successfully")

    except Exception as e:
        cursor.execute("ROLLBACK")
        logger.error(f"Transaction rolled back due to error: {e}")
        raise

    return total_inserted

def convert_batch_simple(batch_df: pd.DataFrame) -> List[Tuple]:
    batch_data = []

    for _, row in batch_df.iterrows():
        row_data = []
        for val in row:
            if pd.isna(val):
                row_data.append(None)
            elif isinstance(val, (pd.Timestamp, np.datetime64)):
                row_data.append(pd.to_datetime(val).to_pydatetime() if not pd.isna(val) else None)
            elif isinstance(val, np.integer):
                row_data.append(int(val))
            elif isinstance(val, np.floating):
                row_data.append(float(val) if not np.isnan(val) else None)
            else:
                row_data.append(str(val) if val is not None else None)

        batch_data.append(tuple(row_data))

    return batch_data