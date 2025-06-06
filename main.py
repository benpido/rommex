# main.py
import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "rommex.settings")
from django.conf import settings
from flights.services.ObtenerVuelos import main as obtener_vuelos
from flights.services.Procesar import procesar_datos
from flights.services.Clean import filtrar_region_antofagasta

def main() -> None:
    """Execute the full ETL process using project settings."""

    new_tbl, stats = obtener_vuelos(
        settings.JSON_CONFIG,
        settings.PARQUET_HISTORICO,
        settings.PARQUET_API,
    )
    records = new_tbl.to_pandas().to_dict("records") if new_tbl.num_rows else []
    kept, discarded = filtrar_region_antofagasta(records)
    print(
        f"Nuevos vuelos: {new_tbl.num_rows} | "
        f"En Antofagasta: {len(kept)} | "
        f"Descartados fuera de la región: {len(discarded)}"
    )

    procesar_datos(settings.PARQUET_API, settings.PARQUET_FINAL)

    total = stats.get("total") if isinstance(stats, dict) else "?"
    print(f"ETL completado. Vuelos procesados: {total}")


if __name__ == "__main__":
    main()
