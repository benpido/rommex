import pandas as pd
from flights.services.Procesar import procesar_datos


def test_procesar_datos_generates_parquet(tmp_path):
    df = pd.DataFrame({
        "Flight/Service Date": ["2024-06-01T16:00:00Z"],
        "Air Seconds": [60],
        "Air+Ground Seconds": [120],
        "Takeoff Bat %": [100],
        "Landing Bat %": [80],
        "Total Mileage (Meters)": [1500],
        "Pilot-in-Command": ["Marcelo Crosgrover"],
        "Max Altitude (Meters)": [10],
        "Max Distance (Meters)": [20],
        "Longitud": [-69.0],
        "Latitude": [-23.0],
        "Drone Name": ["DroneX"],
    })

    input_pq = tmp_path / "api.parquet"
    output_pq = tmp_path / "final.parquet"
    df.to_parquet(input_pq, index=False, engine="pyarrow")

    procesar_datos(str(input_pq), str(output_pq))

    result = pd.read_parquet(output_pq, engine="pyarrow")
    row = result.iloc[0]

    assert result.index.name == "ID"
    assert row["Uso % Bat"] == 20
    assert row["Ground Seconds"] == 60
    assert row["Air Minutes"] == 1
    assert row["Air Hours"] == 0.02
    assert row["Km Recorridos"] == 1.5
    assert row["Equipo Piloto"] == "Pilotos Turno A"
    assert row["Turno"] == "Dia"
