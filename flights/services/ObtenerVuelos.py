import json
import os
import requests
import pandas as pd
from datetime import datetime
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyarrow as pa  
# --- Funciones auxiliares ---


def load_json(path, default=None):
    """
    Carga un JSON desde archivo o devuelve default si no existe.
    Evita errores de FileNotFoundError.
    """
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default

def get_last_flight_timestamp(path: str) -> str | None:

    """Return the last timestamp from parrot file in format 'YYYY-MM-DD+HH:MM:SS' or ``None`` if unavailable."""
    try:
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            return None
        
        dataset = ds.dataset(path, format="parquet")
        max_time: datetime | None = None
        for times in dataset.to_batches(columns=["time"]):
            #print (times)
            time = times.column(0)
            max_str = pc.max(time).as_py()

            # Convertir a datetime
            time = datetime.strptime(max_str, "%Y-%m-%d %H:%M:%S")
            if max_time is None or time > max_time:
                max_time = time
        return (max_time.strftime("%Y-%m-%d %H:%M:%S"))
    except Exception:
        print(f"Error al obtener el último timestamp de {path}")
        return None

def get_now_timestamp():
    """Fecha/hora actual en formato 'YYYY-MM-DD+HH:MM:SS'."""
    now = datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S")

def fetch_flights(query: dict, cfg: dict):
    url   = "https://api.airdata.com/flights"
    limit = 100

    try:
        with requests.Session() as session:
            session.auth    = (cfg["api_key"], "")
            session.headers.update({"Content-Type": "application/json"})

            vuelos, offset = [], 0
            while True:
                resp = session.get(
                    url,
                    params={**query, "limit": limit, "offset": offset},
                    timeout=15
                )
                resp.raise_for_status()

                payload = resp.json()
                vuelos.extend(payload.get("data", []))

                if not payload.get("moreResultsAvailable"):
                    stats = {
                        "requested_range": (query.get("start"), query.get("end")),
                        "total": len(vuelos),
                        "fetched_at": datetime.now().isoformat(timespec="seconds"),
                    }
                    return vuelos, stats

                offset += limit
    except Exception as e:
        print(f"Error al consultar la api: {e}")
    return [], {}


def save_raw_parquet_pa(
    vuelos: list[dict],
    parquet_path: str,
    id_field: str = "id",
    compression: str = "zstd",
    )  -> pa.Table:
    """
    Guarda el histórico RAW en formato Parquet usando PyArrow puro.

    Parameters
    ----------
    vuelos       : list[dict]
        Payload crudo de la API.
    parquet_path  : str
        Ruta al archivo Parquet.
    id_field      : str, default "id"
        Nombre de la columna que identifica unívocamente cada vuelo.
    compression   : str, default "zstd"
        Codec de compresión al re-escribir el archivo.

    Returns
    -------
    pa.Table  (filas realmente añadidas)
    """
    # 1) Convertir el payload a Arrow Table
    tbl_in = pa.Table.from_pylist(vuelos)
    if tbl_in.num_rows == 0:
        return tbl_in                     # nada que guardar

    if id_field not in tbl_in.column_names:
        raise ValueError(f"'{id_field}' no está en el payload")

    # 2) Leer IDs existentes (solo esa columna → minimiza RAM)
    if os.path.exists(parquet_path) and os.path.getsize(parquet_path) > 0:
        ids_hist = pq.read_table(parquet_path, columns=[id_field])[id_field]
        existing_ids = set(ids_hist.to_pylist())          # Python set para lookup
        tbl_hist = pq.read_table(parquet_path)            # se usará luego al concatenar
    else:
        existing_ids = set()
        tbl_hist = None

    # 3) Filtrar solo los registros con ID nuevo
    # Cast to Arrow array to avoid "not a valid value set" errors
    id_set = pa.array(sorted(existing_ids))
    try:
        mask_new = pc.invert(pc.is_in(tbl_in[id_field], value_set=id_set))
    except Exception:
        mask_new = pc.invert(pc.is_in(tbl_in[id_field], value_set=list(existing_ids)))
    tbl_new = tbl_in.filter(mask_new)

    # 4) Concatenar y persistir si hay novedades
    if tbl_new.num_rows > 0:
        tbl_out = pa.concat_tables([tbl_hist, tbl_new]) if tbl_hist else tbl_new
        pq.write_table(tbl_out, parquet_path, compression=compression)

    return tbl_new

def save_flights_to_parquet(flights, output_path):
    """
    Guarda los vuelos especificados en un archivo Parquet.
    Extrae solo las columnas relevantes y crea un DataFrame.
    """
    rows = []
    for flight in flights:
        # Fecha ISO preferida, si no, campo 'time'
        date = flight.get("timeISO") or flight.get("time")
        # Buscamos el piloto con rol 'Pilot-in-Command'
        participants = flight.get("participants", {}).get("data", [])
        pilot = next(
            (
                p.get("name")
                for p in participants
                if p.get("role") == "Pilot-in-Command"
            ),
            "",
        )
        air_secs = flight.get("duration", {}).get("airDuration", "")
        log_secs = flight.get("duration", {}).get("logDuration", "")
        drone_name = flight.get("drone", {}).get("name", "")
        takeoff_pct = flight.get("batteryPercent", {}).get("takeOff", "")
        landing_pct = flight.get("batteryPercent", {}).get("landing", "")
        max_alt = flight.get("altitude", {}).get("max", "")
        max_dist = flight.get("distance", {}).get("max", "")
        mileage = flight.get("mileage", {}).get("total", "")
        latitude = flight.get("takeOffLatitude", "")
        longitude = flight.get("takeOffLongitude", "")
        rows.append(
            {
                "Flight/Service Date": date,
                "Pilot-in-Command": pilot,
                "Air Seconds": air_secs,
                "Air+Ground Seconds": log_secs,
                "Drone Name": drone_name,
                "Takeoff Bat %": takeoff_pct,
                "Landing Bat %": landing_pct,
                "Max Altitude (Meters)": max_alt,
                "Max Distance (Meters)": max_dist,
                "Total Mileage (Meters)": mileage,
                "Latitude": latitude,
                "Longitud": longitude,
            }
        )
    df = pd.DataFrame(rows)
    # Exportamos a Parquet sin índice para que Power BI lo lea limpio
    df.to_parquet(output_path, index=False)
    return df

def main(json_config, paquet_historico, parquet_api):
    cfg   = load_json(json_config, {})
    query = {
        "start": get_last_flight_timestamp(paquet_historico),
        "end": get_now_timestamp(),
        "detail_level": "comprehensive",
    }
    api_resp, stats = fetch_flights(query, cfg)
    print (api_resp)
    print (stats)
    nuevos = save_raw_parquet_pa(api_resp, paquet_historico)
    df_saved = save_flights_to_parquet(
        nuevos.to_pandas().to_dict("records"), parquet_api
    )
    return nuevos, df_saved, stats
json_config       = r"C:\Users\benpi\OneDrive\Escritorio\AutoRommexDjango\rommex\data\config.json"
paquet_historico  = r"C:\Users\benpi\OneDrive\Escritorio\AutoRommexDjango\rommex\data\historico.parquet"
parquet_api       = r"C:\Users\benpi\OneDrive\Escritorio\AutoRommexDjango\rommex\data\flights_api.parquet"
main(json_config, paquet_historico, parquet_api)