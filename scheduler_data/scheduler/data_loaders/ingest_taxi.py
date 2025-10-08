import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

    data = pd.read_csv(url)
    print("Cargando los datos...")
    data.columns = [c.strip().upper() for c in data.columns]

    return data


@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
