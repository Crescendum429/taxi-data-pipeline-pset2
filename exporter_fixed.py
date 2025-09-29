import pandas as pd
import snowflake.connector
from datetime import datetime
import logging
import time

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

    service_type = df['service_type'].iloc[0]
    table_name = "YELLOW_TRIPS"

    start_time = time.time()
    logger.info(f"Exporting to {table_name}")
    logger.info(f"Dataset size: {len(df):,} rows")

    conn = snowflake.connector.connect(**connection_params)
    cursor = conn.cursor()

    try:
        create_bronze_table(cursor, table_name)
        df_export = prepare_dataframe(df)
        rows_inserted = insert_batch_simple(cursor, table_name, df_export)

        elapsed_time = time.time() - start_time
        rows_per_second = rows_inserted / elapsed_time if elapsed_time > 0 else 0

        logger.info(f"Export completed: {rows_inserted:,} rows in {elapsed_time:.2f}s")
        logger.info(f"Performance: {rows_per_second:.0f} rows/sec")

    finally:
        cursor.close()
        conn.close()

def create_bronze_table(cursor, table_name: str):
    logger.info(f"Creating table: {table_name}")

    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    if cursor.fetchone():
        logger.info(f"Table {table_name} already exists")
        return

    create_sql = f"""
    CREATE TABLE {table_name} (
        VENDORID NUMBER,
        TPEP_PICKUP_DATETIME TIMESTAMP_NTZ,
        TPEP_DROPOFF_DATETIME TIMESTAMP_NTZ,
        PASSENGER_COUNT NUMBER,
        TRIP_DISTANCE FLOAT,
        RATECODEID NUMBER,
        STORE_AND_FWD_FLAG STRING,
        PULOCATIONID NUMBER,
        DOLOCATIONID NUMBER,
        PAYMENT_TYPE NUMBER,
        FARE_AMOUNT FLOAT,
        EXTRA FLOAT,
        MTA_TAX FLOAT,
        TIP_AMOUNT FLOAT,
        TOLLS_AMOUNT FLOAT,
        IMPROVEMENT_SURCHARGE FLOAT,
        TOTAL_AMOUNT FLOAT,
        CONGESTION_SURCHARGE FLOAT,
        AIRPORT_FEE FLOAT,
        SOURCE_FILE STRING,
        FILE_INDEX NUMBER,
        LOAD_TIMESTAMP TIMESTAMP_NTZ,
        BATCH_ID STRING,
        INGESTION_TIMESTAMP TIMESTAMP_NTZ,
        SERVICE_TYPE STRING,
        BRONZE_LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        BRONZE_RUN_ID STRING DEFAULT 'MAGE_' || REPLACE(REPLACE(CURRENT_TIMESTAMP()::STRING, ' ', '_'), ':', '-')
    )
    CLUSTER BY (TPEP_PICKUP_DATETIME, SERVICE_TYPE)
    COMMENT = 'Bronze layer - Yellow and Green taxi trip data 2015-2025'
    """

    cursor.execute(create_sql)
    logger.info(f"Table {table_name} created with clustering")

def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Preparing dataframe")

    df_clean = df.copy()

    datetime_columns = [col for col in df_clean.columns if 'datetime' in col.lower() or 'timestamp' in col.lower()]
    for col in datetime_columns:
        if df_clean[col].dtype == 'object':
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')

    numeric_columns = [col for col in df_clean.columns if any(x in col.lower() for x in ['amount', 'fare', 'tip', 'distance'])]
    for col in numeric_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')

    df_clean['BRONZE_LOADED_AT'] = datetime.now()
    df_clean.columns = [col.upper() for col in df_clean.columns]

    logger.info(f"Dataframe prepared: {df_clean.shape[0]:,} rows, {df_clean.shape[1]} columns")
    return df_clean

def insert_batch_simple(cursor, table_name: str, df: pd.DataFrame) -> int:
    logger.info(f"Inserting {len(df):,} rows into {table_name}")

    columns = list(df.columns)
    placeholders = ', '.join(['%s'] * len(columns))
    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

    batch_data = []
    for _, row in df.iterrows():
        row_data = []
        for val in row:
            if pd.isna(val):
                row_data.append(None)
            elif isinstance(val, pd.Timestamp):
                row_data.append(val.to_pydatetime())
            else:
                row_data.append(val)
        batch_data.append(tuple(row_data))

    cursor.executemany(insert_sql, batch_data)
    logger.info(f"Inserted {len(batch_data):,} rows successfully")
    return len(batch_data)