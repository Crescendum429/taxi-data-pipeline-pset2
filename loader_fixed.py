import pandas as pd
import requests
from datetime import datetime
import logging
import time

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

logger = logging.getLogger(__name__)

@data_loader
def load_yellow_taxi_2015_01(*args, **kwargs) -> pd.DataFrame:
    year = 2015
    month = 1
    service_type = 'yellow'

    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{service_type}_tripdata_{year}-{month:02d}.parquet"

    start_time = time.time()

    logger.info(f"Loading Yellow Taxi data for {year}-{month:02d}")
    logger.info(f"Source URL: {url}")

    try:
        response = requests.head(url, timeout=30)
        if response.status_code != 200:
            raise Exception(f"File not available: {url} (Status: {response.status_code})")

        file_size_mb = int(response.headers.get('content-length', 0)) / (1024*1024)
        logger.info(f"File size: {file_size_mb:.1f} MB")

        df = pd.read_parquet(url, engine='pyarrow')

        if df.empty:
            raise Exception(f"Empty dataset from {url}")

        df['source_file'] = f"{service_type}_tripdata_{year}-{month:02d}.parquet"
        df['file_index'] = 0
        df['load_timestamp'] = datetime.now()
        df['batch_id'] = f"{service_type}_{year}_{month:02d}"
        df['ingestion_timestamp'] = datetime.now()
        df['service_type'] = service_type

        processing_time = time.time() - start_time
        rows_per_second = len(df) / processing_time if processing_time > 0 else 0

        logger.info(f"Loading completed successfully")
        logger.info(f"Rows loaded: {len(df):,}")
        logger.info(f"Columns: {len(df.columns)}")
        logger.info(f"Processing time: {processing_time:.2f}s")
        logger.info(f"Load rate: {rows_per_second:.0f} rows/sec")

        return df

    except Exception as e:
        logger.error(f"Failed to load {url}: {e}")
        raise

@test
def test_output(output, *args) -> None:
    assert output is not None, 'Output is undefined'
    assert len(output) > 0, 'No data was loaded'
    assert 'service_type' in output.columns, 'service_type column missing'
    assert 'batch_id' in output.columns, 'batch_id column missing'

    logger.info(f"Test passed: {len(output):,} rows loaded successfully")
    logger.info(f"Service type: {output['service_type'].iloc[0]}")

    if 'tpep_pickup_datetime' in output.columns:
        logger.info(f"Date range: {output['tpep_pickup_datetime'].min()} to {output['tpep_pickup_datetime'].max()}")