import pandas as pd
import os 

def cargar_dataset(path, nrows=1000):
    """
    Carga un subconjunto del dataset de mempool y selecciona únicamente
    las columnas necesarias para la simulación de construcción de bloques.

    Parámetros:
        path (str): Ruta al archivo .csv de mempool (formato Flashbots).
        nrows (int): Número de filas a cargar (default: 1000).

    Retorna:
        pd.DataFrame: Con columnas ['hash', 'from', 'to', 'gas', 'gas_fee_cap', 'timestamp_ms']
    """
    df = pd.read_csv(path, nrows=nrows)
    columnas_necesarias = ["hash", "from", "to", "gas", "gas_fee_cap", "timestamp_ms"]
    return df[columnas_necesarias].copy()

def guardar_log_csv(resumen, path="logs/logs.csv"):
    df_log = pd.DataFrame([resumen])

    if os.path.exists(path):
        df_log.to_csv(path, mode='a', header=False, index=False)
    else:
        df_log.to_csv(path, mode='w', header=True, index=False)
