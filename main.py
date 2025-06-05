# main.py
import os
import flights.services.ObtenerVuelos as ObtenerVuelos
import flights.services.Procesar as Procesar

if __name__ == "__main__":
    # Llama a la función principal 'main' de ObtenerVuelos.py:
    #  - Carga configuración y rutas.
    #  - Recupera vuelos nuevos de la API (sincronización incremental).
    #  - Almacena histórico y genera CSV de vuelos incrementales.
    nuevos = ObtenerVuelos.main()  # <-- Necesita devolver 'nuevos'

    if nuevos:
        env_dir = os.path.dirname(__file__)
        input_csv = os.path.join(env_dir, "flights_api.parquet")
        output = os.path.join(env_dir, "FlightsFinal.parquet")
        Procesar.main(input_csv, output)
    else:
        print("No hay vuelos nuevos → salto el procesamiento de Parquet.")
