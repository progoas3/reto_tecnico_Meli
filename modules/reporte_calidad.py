import pandas as pd
import os


def generar_reporte_ejecutivo():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    logs_root = os.path.join(base_dir, "logs")
    outputs_dir = os.path.join(base_dir, "outputs")

    if not os.path.exists(outputs_dir):
        os.makedirs(outputs_dir)

    try:
        ejecuciones = sorted([d for d in os.listdir(logs_root) if os.path.isdir(os.path.join(logs_root, d))])
        if not ejecuciones: return

        ultima_ejecucion = ejecuciones[-1]
        folder_auditoria = os.path.join(logs_root, ultima_ejecucion, "auditoria")
        ruta_csv = os.path.join(folder_auditoria, "log_auditoria_calidad.csv")

        if not os.path.exists(ruta_csv): return

        print(f"Procesando ejecución: {ultima_ejecucion}")

        df_log = pd.read_csv(ruta_csv)
        df_log.columns = df_log.columns.str.strip()

        # Ajuste de tasa para cálculos
        if 'tasa_error_pct' in df_log.columns:
            df_log['tasa_error_pct_num'] = df_log['tasa_error_pct'].astype(str).str.replace('%', '').astype(float)

        # --- INFORMACIÓN DE VERDAD ---
        # El total real de la tabla es lo que entró en la primera regla (R1)
        # El total limpio es lo que quedó después de la última regla (R8)
        resumen = df_log.groupby('tabla').agg({
            'total_recibidos': 'first',
            'pasan_a_calidad': 'last'
        }).reset_index()

        # La cuarentena real es la diferencia entre el inicio y el fin
        resumen['enviados_a_cuarentena'] = resumen['total_recibidos'] - resumen['pasan_a_calidad']
        resumen['tasa_error_total'] = (
                                              (resumen['enviados_a_cuarentena'] / resumen['total_recibidos']) * 100
                                      ).round(2).astype(str) + '%'

        nombre_reporte = f"reporte_calidad_{ultima_ejecucion}.md"
        ruta_salida = os.path.join(outputs_dir, nombre_reporte)

        with open(ruta_salida, "w", encoding="utf-8") as f:
            f.write(f"# 📊 Reporte de Calidad de Datos - {ultima_ejecucion}\n\n")
            f.write(f"**Meli Technical Challenge - Auditoría de Datos Capa Silver**\n\n")

            f.write("## 1. Resumen Consolidado (Total Real)\n")
            try:
                f.write(resumen.to_markdown(index=False) + "\n\n")
            except:
                f.write(resumen.to_string(index=False) + "\n\n")

            f.write("## 2. Detalle de Impacto por Regla\n")
            f.write("Desglose de cuántos registros filtró cada regla individualmente:\n\n")
            detalle_reglas = df_log[['tabla', 'regla', 'campo_evaluado', 'enviados_a_cuarentena', 'tasa_error_pct']]
            try:
                f.write(detalle_reglas.to_markdown(index=False) + "\n\n")
            except:
                f.write(detalle_reglas.to_string(index=False) + "\n\n")

            f.write("--- \n")
            f.write(f"- **Carpeta de Ejecución:** `{ultima_ejecucion}`\n")
            f.write(f"- **Capa Destino:** Clean (Silver Area)\n")

        print(f"Reporte con datos reales guardado como: {nombre_reporte}")

    except Exception as e:
        print(f"Error crítico: {e}")
