from mage_ai.io.snowflake import Snowflake
from mage_ai.data_preparation.shared.secrets import get_secret_value
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_snowflake(data: pd.DataFrame, **kwargs) -> None:

    # Crear conexión directa a Snowflake
    loader = Snowflake(
        account=get_secret_value('SNOWFLAKE_ACCOUNT'),
        user=get_secret_value('SNOWFLAKE_USER'),
        password=get_secret_value('SNOWFLAKE_PASSWORD'),
        warehouse=get_secret_value('SNOWFLAKE_WAREHOUSE'),
        database=get_secret_value('SNOWFLAKE_DATABASE'),
        schema='RAW',
        role=get_secret_value('SNOWFLAKE_ROLE'),
        insecure_mode=True,
    )

    table_name = "taxi_zones"

    with loader:
        # exportar
        loader.export(
            data,
            table_name,
            if_exists='replace'
        )

    print(f"Exportación finalizada")
