# main.py
import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "rommex.settings")
from django.conf import settings
from flights.services.ObtenerVuelos import main as obtener_vuelos
from flights.services.Procesar import procesar_datos


def main() -> None:
    """Execute the full ETL process using project settings."""

    stats = obtener_vuelos(
        settings.JSON_CONFIG,
        settings.PARQUET_HISTORICO,
        settings.PARQUET_API,
    )

    procesar_datos(settings.PARQUET_API, settings.PARQUET_FINAL)

    total = stats.get("total") if isinstance(stats, dict) else "?"
    print(f"ETL completado. Vuelos procesados: {total}")


if __name__ == "__main__":
    main()
    