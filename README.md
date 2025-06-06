# Rommex
Aplicación Django para descargar y procesar datos de vuelos de drones. Incluye un
sencillo dashboard que permite ver informacion sobre los nuevos datos obtenidos y actualizar la información 
obteniendo el archivo parquet resultante.

## Descripción
Los módulos dentro de `flights/services` implementan un flujo ETL completo:
- Tras cada consulta a la API se muestran cuántos vuelos son nuevos,
  cuántos pertenecen a la Región de Antofagasta y cuántos se descartan por
  estar fuera de dicha zona.
  
- **ObtenerVuelos** descarga registros de vuelos desde la API externa de AirDatta y mantiene un
  histórico en `data/historico.parquet`.
- **Clean** y **Procesar** normalizan la información y calculan métricas
  adicionales, generando `data/FlightsFinal.parquet`.
- El dashboard web (vistas en `flights/views.py`) permite ejecutar el ETL y
descargar el archivo procesado.

## Instalacion
- pip install -r requirements.txt

## Configuración
- Crea una carpeta `data/` (excluida del repositorio) con los siguientes
archivos:

- Define la variable de entorno `DJANGO_SECRET_KEY` con una cadena aleatoria antes de ejecutar la aplicación.

- en ps --> $env:DJANGO_SECRET_KEY = "letraslargas"

- `config.json` con las credenciales y URL de la API.
    `
     {
   "base_url": "https://api.airdata.com",
   "api_key": "apiKey",
   "endpoint": "/flights",
   "page_size": 100
    }
    `
- `historico.parquet` 
    - almacenara el histórico completo de la empresa
- `flights_api.parquet`
    - archivo temporal generado en cada consulta a la API
- `FlightsFinal.parquet`
    - resultado final del procesamiento, con estructura definida
    Air Hours,Air Minutes,Air Seconds,Air+Ground Seconds,Drone Name,Equipo Piloto,Flight/Service Date,Ground Seconds,ID,Km Recorridos,Landing Bat %,Latitude,Longitude,Max Altitude (Meters),Max Distance (Meters),Pilot-in-Command,Takeoff Bat %,Total Mileage (Meters),Turno,Uso % Bat
    - el archivo se actualiza con cada ejecución para mantener el histórico procesado
Los archivos `historico.parquet`, `flights_api.parquet` y `FlightsFinal.parquet` deben ser
archivos Parquet válidos o simplemente no existir. Si están presentes pero vacíos (tamaño
0&nbsp;bytes) la lectura fallará; elimínalos para que el sistema los regenere.

## Uso
1. Ejecuta las migraciones y crea un usuario administrador:
- python manage.py migrate
- python manage.py runserver

## Estructura
- `flights/services/` – obtención y procesamiento de vuelos.
- `flights/templates/` – plantillas del dashboard y autenticación.
- `rommex/` – configuración principal de Django.
- `manage.py` – utilitario de administración.
