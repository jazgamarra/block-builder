from utils import cargar_dataset
from algoritmo_heuristico import calcular_T_simulado, construir_bloque
from algoritmo_greedy_clasico import construir_bloque_greedy
import os 

df = cargar_dataset(os.path.join(os.path.dirname(__file__), '..', 'data', '2025-07-14.csv'), nrows=1000)
T_simulado = calcular_T_simulado(df)
resumen, bloque = construir_bloque(df, T_simulado)
# resumen, bloque = construir_bloque_greedy(df, T_simulado)

print(resumen)
