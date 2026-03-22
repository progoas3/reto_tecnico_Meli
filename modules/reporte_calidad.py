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

        df_log = pd.read_csv(ruta_csv)
        df_log.columns = df_log.columns.str.strip()

        # --- SECCIÓN 1: PROCESAMIENTO DE TOTALES ---
        resumen_tablas = df_log.groupby('tabla').agg({
            'total_recibidos': 'first',
            'enviados_a_cuarentena': 'sum'  # Suma de todos los deltas reales
        }).reset_index()

        nombre_reporte = f"reporte_calidad_{ultima_ejecucion}.md"
        ruta_salida = os.path.join(outputs_dir, nombre_reporte)

        with open(ruta_salida, "w", encoding="utf-8") as f:
            f.write(f"# 📊 Reporte Ejecutivo de Calidad de Datos\n")
            f.write(f"> **Ejecución:** `{ultima_ejecucion}` | **Capa:** Silver (Clean)\n\n")

            # --- SECCIÓN 1: PERFILADO (DIAGNÓSTICO) ---
            f.write("## 1. Perfilado de Datos Inicial (Diagnóstico)\n")
            f.write("| Tabla | Registros | Errores Detectados | % Calidad Inicial | Estado |\n")
            f.write("| :--- | :---: | :---: | :---: | :--- |\n")

            for _, row in resumen_tablas.iterrows():
                total = row['total_recibidos']
                errores = row['enviados_a_cuarentena']
                calidad_pct = round(((total - errores) / total) * 100, 2)
                status = "✅ Óptimo" if calidad_pct > 90 else "🚨 Crítico"
                f.write(f"| **{row['tabla']}** | {total} | {errores} | {calidad_pct}% | {status} |\n")

            # --- SECCIÓN 2: DETALLE REAL POR REGLA ---
            f.write("\n## 2. Ejecución de Reglas de Calidad (Impacto Real)\n")
            f.write("A continuación se detalla cuántos registros filtró cada regla de forma independiente:\n\n")

            # Filtramos para no mostrar ruido: si la regla no afectó a nada, se ve más limpio
            df_detalle = df_log[['tabla', 'regla', 'campo_evaluado', 'enviados_a_cuarentena', 'tasa_error_pct']]

            f.write(df_detalle.to_markdown(index=False) + "\n\n")

            # --- SECCIÓN 3: TRANSFORMACIONES ---
            f.write("## 3. Transformaciones Críticas y Seguridad\n")
            f.write("* **Anonimización (PII):** Hashing SHA-256 aplicado (R8).\n")
            f.write("* **Imputación (R9):** Solo aplicada a la tabla PRODUCTOS para corregir stock `NaN`.\n")
            f.write("* **Integridad:** Eliminación de duplicados por PK (R1).\n\n")

            f.write("---\n")
            f.write(f"**Resultado:** Procesamiento finalizado. Datos persistidos en DuckDB.\n")

        print(f"Reporte corregido generado en: {ruta_salida}")

    except Exception as e:
        print(f"Error: {e}")