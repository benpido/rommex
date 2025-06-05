# El objetivo principal de este script es procesar un archivo CSV que contiene datos de vuelos.
# Y descartar registros que cumplan con ciertas condiciones.
# Vuelos que esten fuera de la region de antofagasta
import pandas as pd
from typing import Optional, Tuple
def count_and_prune_duplicates(
    df: pd.DataFrame,
    subset: Optional[list[str]] = None,
    keep: str = "first"
) -> Tuple[int, pd.DataFrame, pd.DataFrame]:
    """
    Cuenta filas duplicadas y devuelve también el DataFrame depurado.

    Parameters
    ----------
    df : pd.DataFrame
        El DataFrame a revisar.
    subset : list[str] | None, default None
        Columnas que se considerarán para determinar duplicados.
        • None  ➜ se usan todas las columnas.
        • ['col1', 'col2'] ➜ solo se miran esas columnas.
    keep : {"first", "last", False}, default "first"
        Igual que en `pandas.DataFrame.duplicated` y `drop_duplicates`:
        • "first"  ➜ conserva la primera aparición.
        • "last"   ➜ conserva la última aparición.
        • False    ➜ elimina *todas* las ocurrencias duplicadas.

    Returns
    -------
    dup_count : int
        Número de filas marcadas como duplicadas según `subset` y `keep`.
    dup_rows : pd.DataFrame
        Las filas duplicadas (excluyendo la que se conserva).
    df_clean : pd.DataFrame
        El DataFrame sin duplicados.
        como usar : 
            #df = pd.read_parquet(settings.default_parquet, engine="pyarrow")
            #n_dups, dups_df, clean_df = count_and_prune_duplicates(df)
            #print(f"Duplicados encontrados en : {settings.default_parquet} de {n_dups}\n")
    """
    mask = df.duplicated(subset=subset, keep=keep if keep else False)
    dup_rows = df[mask]
    dup_count = int(mask.sum())

    # Si keep es False, eliminamos todas las ocurrencias; de lo contrario,
    # usamos drop_duplicates con el mismo criterio para conservar solo una.
    if keep:
        df_clean = df.drop_duplicates(subset=subset, keep=keep).reset_index(drop=True)
    else:
        df_clean = df[~mask].reset_index(drop=True)  # quita todas las repeticiones

    return dup_count, dup_rows, df_clean

# límites aproximados Región de Antofagasta
LAT_MIN, LAT_MAX = -26.5, -21.75   # sur / norte
LON_MIN, LON_MAX = -71.5, -67.0    # oeste / este

def filtrar_region_antofagasta(records):
    """
    Devuelve (kept_df, discarded_df) con los vuelos
    dentro / fuera de la Región de Antofagasta.

    Columnas esperadas: 'Longitud' y 'Latitude' (float).
    """
    df = pd.DataFrame(records).copy()

    # nos aseguramos de que sean numéricos
    df['Longitud'] = pd.to_numeric(df['Longitud'], errors='coerce')
    df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')

    mask = (
        df['Latitude'].between(LAT_MIN, LAT_MAX)
        & df['Longitud'].between(LON_MIN, LON_MAX)
    )
    kept      = df[mask]
    discarded = df[~mask]

    print(f"Antofagasta ✓ {len(kept)} | Fuera ✗ {len(discarded)}")
    return kept, discarded
