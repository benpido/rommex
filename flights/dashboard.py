import os
from django.conf import settings
from .services.ObtenerVuelos import fetch_flights
from .services.Clean import filtrar_region_antofagasta
from .services.Procesar import procesar_datos

PARQUET_FILE = settings.PARQUET_FINAL


def run_etl():
    """
    Ejecuta la secuencia: obtener vuelos, filtrar, procesar y guardar parquet.
    """
    # 1) Obtener datos
    vuelos = fetch_flights()
    # 2) Limpieza
    #vuelos_limpios = filtrar_region_antofagasta(vuelos)
    # 3) Procesamiento y escritura
    procesar_datos(vuelos, PARQUET_FILE)