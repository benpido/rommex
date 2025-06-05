from django.conf import settings
from .services.ObtenerVuelos import main
from .services.Procesar import procesar_datos

def run_etl():
    """
    Ejecuta la secuencia completa de ETL y procesamiento.
    """
    # 1) Obtener y guardar vuelos crudos
    main(
        settings.JSON_CONFIG,
        settings.PARQUET_HISTORICO,
        settings.PARQUET_API,
    )

    # 2) Procesamiento y escritura final
    procesar_datos(settings.PARQUET_API, settings.PARQUET_FINAL)