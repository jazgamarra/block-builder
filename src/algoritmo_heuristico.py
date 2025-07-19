import pandas as pd
import time
from itertools import combinations
from utils import cargar_dataset, guardar_log_csv

def calcular_T_simulado(df, delay_ms=6000):
    """
    Calcula el instante simulado de inclusión del bloque.

    Parámetros:
        df (pd.DataFrame): DataFrame con columna 'timestamp_ms' de las transacciones.
        delay_ms (int): Retardo simulado (en milisegundos) respecto al promedio de llegada.

    Retorna:
        int: Timestamp simulado en milisegundos.
    """
    return int(df["timestamp_ms"].mean()) + delay_ms

def calcular_utilidad(ti, tj, gas_limit=30_000_000, penalties=None, bonuses=None):
    penalties = penalties or {
        "conflicto": 999,
        "dependencia_mal_ordenada": 100,
        "gas_alto": 10
    }
    bonuses = bonuses or {
        "contrato_comun": 50,
        "orden_correcto": 30,
        "mev_detectado": 100
    }

    tarifa_ti = ti["gas"] * ti["gas_fee_cap"]
    tarifa_tj = tj["gas"] * tj["gas_fee_cap"]

    reglas = {
        "conflicto_nonce": ti["from"] == tj["from"] and ti.get("nonce") == tj.get("nonce"),
        "conflicto_destino": ti["to"] == tj["to"],
        "gas_excesivo": (ti["gas"] + tj["gas"]) > gas_limit,
        "contrato_comun": ti["to"] == tj["to"],
        "orden_valido": (
            ti["from"] == tj["from"]
            and ti.get("nonce") is not None
            and tj.get("nonce") is not None
            and ti["nonce"] + 1 == tj["nonce"]
        ),
    }

    penalizacion = 0
    bonificacion = 0

    if reglas["conflicto_nonce"] or reglas["conflicto_destino"]:
        penalizacion += penalties["conflicto"]
    if reglas["gas_excesivo"]:
        penalizacion += penalties["gas_alto"]
    if reglas["contrato_comun"]:
        bonificacion += bonuses["contrato_comun"]
    if reglas["orden_valido"]:
        bonificacion += bonuses["orden_correcto"]

    return tarifa_ti + tarifa_tj + bonificacion - penalizacion

def construir_bloque(df, T_simulado, gas_limit=30_000_000):
    """
    Construye un bloque optimizado por utilidad secuencial (modelo de secuenciación de trabajos).

    Parámetros:
        df (pd.DataFrame): Con columnas ['hash', 'from', 'to', 'gas', 'gas_fee_cap', 'timestamp_ms', 'nonce']
        T_simulado (int): Timestamp del bloque simulado
        gas_limit (int): Límite máximo de gas

    Retorna:
        resumen (dict), bloque_df (pd.DataFrame)
    """
    inicio = time.time()
    df["fee"] = df["gas_fee_cap"] * df["gas"]
    txs = df.to_dict("records")  

    combinaciones_validas = []
    for i, j in combinations(range(len(txs)), 2):
        ti = txs[i]
        tj = txs[j]

        gas_total = ti["gas"] + tj["gas"]
        if gas_total > gas_limit:
            continue  # Si la combinación excede el límite de gas, la descartamos 

        utilidad = calcular_utilidad(ti, tj, gas_limit=gas_limit)
        addrs = {ti["from"], ti["to"], tj["from"], tj["to"]}

        combinaciones_validas.append({
            "i": i,
            "j": j,
            "utilidad_total": utilidad,
            "gas_total": gas_total,
            "addrs": addrs
        })

    combinaciones_ordenadas = sorted(combinaciones_validas, key=lambda x: x["utilidad_total"], reverse=True)

    bloque_idx = set()
    direcciones_ocupadas = set()
    gas_usado = 0

    for combo in combinaciones_ordenadas:
        if combo["addrs"] & direcciones_ocupadas:
            continue
        if gas_usado + combo["gas_total"] > gas_limit:
            continue
        bloque_idx.update([combo["i"], combo["j"]])
        gas_usado += combo["gas_total"]
        direcciones_ocupadas |= combo["addrs"]

    fin = time.time() 
    bloque_df = pd.DataFrame([txs[i] for i in bloque_idx])
    bloque_df["fee"] = bloque_df["gas"] * bloque_df["gas_fee_cap"]
    bloque_df["lead_time_ms"] = T_simulado - bloque_df["timestamp_ms"]

    resumen = {
    "algoritmo": "heuristico",
    "timestamp_simulado": T_simulado,
    "total_transacciones": len(df),
    "tx_incluidas": len(bloque_df),
    "gas_usado": int(bloque_df["gas"].sum()),
    "utilidad_total": int(bloque_df["fee"].sum()),
    "fragmentacion": int(gas_limit - bloque_df["gas"].sum()),
    "lead_time_promedio_s": round(bloque_df["lead_time_ms"].mean() / 1000, 3),
    "tiempo_ejecucion_s": round(fin - inicio, 4)
    }

    guardar_log_csv(resumen)

    return resumen, bloque_df
