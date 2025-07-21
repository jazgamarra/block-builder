import pandas as pd
import time
from itertools import combinations
from utils import  guardar_log_csv, calcular_utilidad

def construir_bloque(df, T_simulado, gas_limit=30_000_000, top_n=500, max_trios=10000, max_pares=20000):
    """
    Construye un bloque heurístico combinando evaluación de tríos, pares y relleno greedy final.

    Parámetros:
        df (pd.DataFrame): Dataset completo con columnas ['from', 'to', 'gas', 'gas_fee_cap', 'timestamp_ms']
        T_simulado (int): Timestamp simulado
        gas_limit (int): Límite de gas por bloque
        top_n (int): N transacciones top por fee para evaluar combinaciones
        max_trios (int): Cantidad máxima de tríos a considerar
        max_pares (int): Cantidad máxima de pares a considerar

    Retorna:
        resumen (dict), bloque_df (pd.DataFrame)
    """
    inicio = time.perf_counter()
    df["fee"] = df["gas"] * df["gas_fee_cap"]
    txs_top = df.sort_values("fee", ascending=False).head(top_n).reset_index(drop=True)
    txs = txs_top.to_dict("records")
    n = len(txs)

    bloque_idx = set()
    direcciones_ocupadas = set()
    gas_usado = 0

    # --- TRIOS ---
    trios = []
    for i, j, k in combinations(range(n), 3):
        ti, tj, tk = txs[i], txs[j], txs[k]
        gas_total = ti["gas"] + tj["gas"] + tk["gas"]
        if gas_total > gas_limit:
            continue

        utilidad = (
            calcular_utilidad(ti, tj, gas_limit=gas_limit) +
            calcular_utilidad(ti, tk, gas_limit=gas_limit) +
            calcular_utilidad(tj, tk, gas_limit=gas_limit)
        ) / 3

        addrs = {ti["from"], ti["to"], tj["from"], tj["to"], tk["from"], tk["to"]}
        trios.append({
            "idx": [i, j, k],
            "utilidad": utilidad,
            "gas_total": gas_total,
            "addrs": addrs
        })
        if len(trios) >= max_trios:
            break

    trios.sort(key=lambda x: x["utilidad"], reverse=True)

    for t in trios:
        if t["addrs"] & direcciones_ocupadas:
            continue
        if gas_usado + t["gas_total"] > gas_limit:
            continue
        bloque_idx.update(t["idx"])
        gas_usado += t["gas_total"]
        direcciones_ocupadas |= t["addrs"]

    # --- PARES ---
    pares = []
    for i, j in combinations(range(n), 2):
        if i in bloque_idx or j in bloque_idx:
            continue
        ti, tj = txs[i], txs[j]
        gas_total = ti["gas"] + tj["gas"]
        if gas_total > gas_limit:
            continue

        utilidad = calcular_utilidad(ti, tj, gas_limit=gas_limit)
        addrs = {ti["from"], ti["to"], tj["from"], tj["to"]}
        pares.append({
            "idx": [i, j],
            "utilidad": utilidad,
            "gas_total": gas_total,
            "addrs": addrs
        })
        if len(pares) >= max_pares:
            break

    pares.sort(key=lambda x: x["utilidad"], reverse=True)

    for p in pares:
        if any(idx in bloque_idx for idx in p["idx"]):
            continue
        if p["addrs"] & direcciones_ocupadas:
            continue
        if gas_usado + p["gas_total"] > gas_limit:
            continue
        bloque_idx.update(p["idx"])
        gas_usado += p["gas_total"]
        direcciones_ocupadas |= p["addrs"]

    # --- Finalizar ---
    bloque_df = df.loc[list(bloque_idx)].copy()
    bloque_df["lead_time_ms"] = T_simulado - bloque_df["timestamp_ms"]

    fin = time.perf_counter()

    resumen = {
        "algoritmo": f"hibrido_top{top_n}",
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
