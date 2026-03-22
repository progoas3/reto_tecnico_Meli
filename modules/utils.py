import pandas as pd
from datetime import datetime
import os
from functools import wraps
import time

LOG_TIEMPOS = []

def registrar_auditoria(tabla, regla, campo, n_entrada, n_salida, ruta_logs):
    """
    Registra el balance de calidad.
    MEJORA: Limpia el nombre de la regla y asegura que el campo sea descriptivo.
    """
    ruta_sub_auditoria = os.path.join(ruta_logs, "auditoria")
    os.makedirs(ruta_sub_auditoria, exist_ok=True)
    ruta_auditoria = os.path.join(ruta_sub_auditoria, "log_auditoria_calidad.csv")

    cuarentena = n_entrada - n_salida
    ahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # --- MEJORA DE LÓGICA DE NOMBRES ---
    # Si el campo viene genérico, lo hacemos descriptivo según la regla
    nombres_dict = {
        'R2_Nulos': f"Validación de Nulos en campos críticos",
        'R3_Email': f"Validación de formato en columna: {campo}",
        'R8_Hashing_PII': f"Anonimización de datos sensibles",
        'R4_Moneda': f"Validación de integridad financiera ({campo})"
    }

    # Si la regla está en nuestro dict, usamos la descripción pro, si no, el campo que venga
    campo_final = nombres_dict.get(regla, campo)

    registro = pd.DataFrame([{
        'timestamp': ahora,
        'tabla': tabla.upper(),
        'regla': regla,
        'campo_evaluado': campo_final,
        'total_recibidos': n_entrada,
        'pasan_a_calidad': n_salida,
        'enviados_a_cuarentena': cuarentena,
        'tasa_error_pct': f"{(cuarentena / n_entrada) * 100:.2f}%" if n_entrada > 0 else "0.00%"
    }])

    header_bool = not os.path.exists(ruta_auditoria)
    registro.to_csv(ruta_auditoria, index=False, mode='a', header=header_bool, encoding='utf-8')


def registrar_error_y_log(tabla, regla, df_errores, campo_afectado, ruta_quarantine, ruta_logs):
    """
    Registra por qué se fue a cuarentena.
    """
    if df_errores.empty:
        return

    os.makedirs(ruta_quarantine, exist_ok=True)
    # Nombre de archivo más limpio
    nombre_archivo = f"{tabla}_fallo_{regla.lower()}.csv"
    ruta_csv = os.path.join(ruta_quarantine, nombre_archivo)
    df_errores.to_csv(ruta_csv, index=False, encoding='utf-8')

    ruta_sub_log_quarantine = os.path.join(ruta_logs, "quarantine")
    os.makedirs(ruta_sub_log_quarantine, exist_ok=True)

    ruta_log_trans = os.path.join(ruta_sub_log_quarantine, "log_cuarentena.csv")
    ahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Aquí también enriquecemos el mensaje
    registro_log = pd.DataFrame([{
        'timestamp': ahora,
        'tabla': tabla.upper(),
        'regla_incumplida': regla,
        'campo_detectado': campo_afectado,
        'cantidad_rechazados': len(df_errores),
        'ruta_evidencia': f"quarantine/{nombre_archivo}"
    }])

    header_bool = not os.path.exists(ruta_log_trans)
    registro_log.to_csv(ruta_log_trans, index=False, mode='a', header=header_bool, encoding='utf-8')