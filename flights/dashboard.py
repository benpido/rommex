from django.conf import settings
from .services.ObtenerVuelos import main
from .services.Procesar import procesar_datos
from .services.Clean import filtrar_region_antofagasta

def run_etl():
    """
    Ejecuta la secuencia completa de ETL y procesamiento.
    """
    # 1) Obtener y guardar vuelos crudos
    new_tbl, df_new, stats = main(
        settings.JSON_CONFIG,
        settings.PARQUET_HISTORICO,
        settings.PARQUET_API,
    )
    # 'df_new' contiene las columnas limpias del batch recien descargado
    records = df_new.to_dict("records") if not df_new.empty else []
    kept, discarded = filtrar_region_antofagasta(records)
    print(
        f"Nuevos vuelos: {new_tbl.num_rows} | "
        f"En Antofagasta: {len(kept)} | "
        f"Descartados fuera de la región: {len(discarded)}"
    )

    # 2) Procesamiento y escritura final
    procesar_datos(settings.PARQUET_API, settings.PARQUET_FINAL)

    return {
        "fetched": new_tbl.num_rows,
        "kept": len(kept),
        "discarded": len(discarded),
        "api_total": stats.get("total") if isinstance(stats, dict) else None,
        "range": stats.get("requested_range") if isinstance(stats, dict) else None,
    }