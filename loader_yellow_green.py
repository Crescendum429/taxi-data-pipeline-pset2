import pandas as pd
import requests
import uuid
import os
import pyarrow.parquet as pq

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def check_url(url: str):
    try:
        r = requests.head(url, timeout=10)
        return r.status_code == 200, r
    except Exception as e:
        print(f"Error verificando URL {url}: {e}")
        return False, None


@data_loader
def load_data(*args, **kwargs):
    start_year = kwargs.get("start_year", 2015)
    end_year = kwargs.get("end_year", 2025)
    start_month = kwargs.get("start_month", 1)

    all_meta = []

    # Procesar AMBOS servicios: yellow y green
    for servicio in ["yellow", "green"]:
        print(f"\n=== Procesando servicio: {servicio.upper()} ===\n")

        for anio in range(start_year, end_year + 1):
            mes_inicio = start_month if anio == start_year else 1

            for mes in range(mes_inicio, 13):
                URL = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{servicio}_tripdata_{anio}-{mes:02d}.parquet"
                run_id = f"{servicio}_{anio}_{mes:02d}"

                print(f"Procesando {run_id}")

                meta = {
                    "run_id": run_id,
                    "anio": anio,
                    "mes": mes,
                    "servicio": servicio,
                    "url": URL,
                    "status": "pendiente",
                    "conteo": 0,
                    "n_columns": 0,
                    "file_size_bytes": None,
                    "ingest_ts": pd.Timestamp.utcnow(),
                }

                ok, response = check_url(URL)
                if not ok:
                    meta["status"] = "brecha"
                    print(f"  Archivo no disponible")
                    all_meta.append(meta)
                    continue

                local_file = f"/tmp/{servicio}_{anio}_{mes:02d}.parquet"
                if not os.path.exists(local_file):
                    print(f"  Descargando...")
                    with requests.get(URL, stream=True, timeout=60) as r:
                        r.raise_for_status()
                        with open(local_file, "wb") as f:
                            for chunk in r.iter_content(chunk_size=8192):
                                f.write(chunk)

                parquet_file = pq.ParquetFile(local_file)
                meta["status"] = "ok"
                meta["conteo"] = parquet_file.metadata.num_rows
                meta["n_columns"] = len(parquet_file.schema_arrow.names)
                meta["file_size_bytes"] = os.path.getsize(local_file)

                print(f"  Listo: {meta['conteo']:,} filas")
                all_meta.append(meta)

    return pd.DataFrame(all_meta)


@test
def test_output(output, *args) -> None:
    assert output is not None, "El loader no devolvió nada"
    assert isinstance(output, pd.DataFrame), "El loader debe devolver un DataFrame"
    assert len(output) > 0, "El DataFrame está vacío"
    print(f"Test OK: {len(output)} archivos procesados")
