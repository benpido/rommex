# El objetivo principal es crear columnas calculadas a partir de un CSV de vuelos
# y guardar el resultado en un nuevo archivo CSV o Parquet.
import pandas as pd
from flights.services.Clean import filtrar_region_antofagasta

# Listas de pilotos por equipo
pilotos_turno_a = ["Marcelo Crosgrover", "Fernando Vargas"]
pilotos_turno_b = ["Luciano Erazo", "Carlos Farias"]


def calcular_turno(fecha_str):
    """
    Determina si un vuelo fue de día o de noche.
    Día: de 08:00 a 20:00 (inclusive de 08:00, exclusivo de 20:00).
    Noche: caso contrario.
    """
    dt = pd.to_datetime(fecha_str)
    hora = dt.hour
    return "Dia" if 8 <= hora < 20 else "Noche"


def calcular_uso_bat(takeoff_pct, landing_pct):
    """
    Calcula el uso porcentual de batería.
    Uso % Bat = Takeoff Bat % - Landing Bat %
    """
    try:
        return float(takeoff_pct) - float(landing_pct)
    except (ValueError, TypeError) as e:
        print(
            f"[ERROR] Uso % Bat: takeoff='{takeoff_pct}' ({type(takeoff_pct)}), "
            f"landing='{landing_pct}' ({type(landing_pct)}) — {e}"
        )
    return None


def calcular_ground_seconds(air_and_ground, air_secs):
    """
    Calcula los segundos en tierra y redondea a 2 decimales.
    Ground Seconds = Air+Ground Seconds - Air Seconds
    """
    try:
        val = float(air_and_ground) - float(air_secs)
        return round(val, 2)
    except (ValueError, TypeError):
        return None


def calcular_air_minutes(air_secs):
    """
    Calcula el tiempo de aire en minutos y redondea a 2 decimales.
    Air Minutes = Air Seconds / 60
    """
    try:
        val = float(air_secs) / 60
        return round(val, 2)
    except (ValueError, TypeError):
        return None


def calcular_air_hours(air_secs):
    """
    Calcula el tiempo de aire en horas y redondea a 2 decimales.
    Air Hours = Air Seconds / 3600
    """
    try:
        val = float(air_secs) / 3600
        return round(val, 2)
    except (ValueError, TypeError):
        return None


def calcular_km_recorridos(total_mileage_m):
    """
    Calcula los kilómetros recorridos y redondea a 2 decimales.
    Km Recorridos = Total Mileage (Meters) / 1000
    """
    try:
        val = float(total_mileage_m) / 1000
        return round(val, 2)
    except (ValueError, TypeError):
        return None


def determinar_equipo_piloto(pilot_name):
    """
    Asigna el equipo del piloto según listas predefinidas.
    Retorna "Pilotos Turno A", "Pilotos Turno B" o "Otro".
    """
    if pilot_name in pilotos_turno_a:
        return "Pilotos Turno A"
    elif pilot_name in pilotos_turno_b:
        return "Pilotos Turno B"
    else:
        return "Otro"


def formatear_decimal(x, precision=6):
    """
    Convierte un valor numérico o cadena a string con separador decimal punto
    y con la cantidad de decimales indicada.
    """
    try:
        val = float(str(x).replace(",", "."))
        return f"{val:.{precision}f}"
    except (ValueError, TypeError):
        return ""


def definir_tipos(df: pd.DataFrame) -> pd.DataFrame:
    # 1) Normalizar fecha (como ya lo tienes)
    df["Flight/Service Date"] = (
        pd.to_datetime(df["Flight/Service Date"], utc=True, errors="coerce")
        .dt.tz_convert("America/Santiago")
        .dt.tz_localize(None)
    )

    # 2) Limpieza previa de separadores decimales
    #    Ajusta esta lista a todas tus columnas numéricas
    float_cols = [
        "Max Altitude (Meters)",
        "Max Distance (Meters)",
        "Total Mileage (Meters)",
        "Uso % Bat",
        "Ground Seconds",
        "Air Minutes",
        "Air Hours",
        "Km Recorridos",
        "Longitud",
        "Latitude",
    ]
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", ".", regex=False), errors="coerce"
            )

    # 3) Ahora ya podemos castear con astype
    dtype_map = {
        "Air Seconds": "float64",
        "Air+Ground Seconds": "float64",
        "Takeoff Bat %": "float64",
        "Landing Bat %": "float64",
        # el resto de float ya los convertimos arriba
    }
    df = df.astype(dtype_map)

    return df


def procesar_datos(input_parquet: str, output_parquet: str):
    """
    Lee el histórico (output_parquet), lee el batch nuevo (input_parquet),
    aplica esquema y cálculos sólo al batch nuevo, concatena sin duplicados
    y reescribe el histórico completo.
    """
    # 1) Cargo el histórico completo
    df_existente = pd.read_parquet(output_parquet, engine="pyarrow")

    # 2) Cargo y tipifico el batch nuevo
    df_nuevo = pd.read_parquet(input_parquet, engine="pyarrow")
    df_nuevo = definir_tipos(df_nuevo)  # <-- antes era definir_tipos(df)

    # 3) Agrego columnas calculadas **sobre df_nuevo**
    df_nuevo["Turno"] = df_nuevo["Flight/Service Date"].apply(calcular_turno)
    df_nuevo["Uso % Bat"] = df_nuevo.apply(
        lambda x: calcular_uso_bat(x["Takeoff Bat %"], x["Landing Bat %"]), axis=1
    )
    df_nuevo["Ground Seconds"] = df_nuevo.apply(
        lambda x: calcular_ground_seconds(x["Air+Ground Seconds"], x["Air Seconds"]),
        axis=1,
    )
    df_nuevo["Air Minutes"] = df_nuevo["Air Seconds"].apply(calcular_air_minutes)
    df_nuevo["Air Hours"] = df_nuevo["Air Seconds"].apply(calcular_air_hours)
    df_nuevo["Km Recorridos"] = df_nuevo["Total Mileage (Meters)"].apply(
        calcular_km_recorridos
    )
    df_nuevo["Equipo Piloto"] = df_nuevo["Pilot-in-Command"].apply(
        determinar_equipo_piloto
    )

    # 4) Uno histórico + nuevo
    df = pd.concat([df_existente, df_nuevo], ignore_index=True)

    # 5) Reindexo y sobrescribo el Parquet de salida
    df = df.reset_index(drop=True)
    df.index.name = "ID"
    df.to_parquet(output_parquet, engine="pyarrow", index=True)
