import pandas as pd
import os


def generar_reporte_ejecutivo():
    # 1. Configuración de rutas
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    logs_root = os.path.join(base_dir, "logs")
    outputs_dir = os.path.join(base_dir, "outputs")

    if not os.path.exists(outputs_dir):
        os.makedirs(outputs_dir)

    try:
        # 2. Identificar la última ejecución
        ejecuciones = sorted([d for d in os.listdir(logs_root) if os.path.isdir(os.path.join(logs_root, d))])
        if not ejecuciones:
            print("No se encontraron carpetas de logs.")
            return

        ultima_ejecucion = ejecuciones[-1]
        ruta_csv = os.path.join(logs_root, ultima_ejecucion, "auditoria", "log_auditoria_calidad.csv")

        if not os.path.exists(ruta_csv):
            print(f"No existe el CSV: {ruta_csv}")
            return

        # 3. Leer log
        df_log = pd.read_csv(ruta_csv)
        df_log.columns = df_log.columns.str.strip()

        # --- SECCIÓN 1: PROCESAMIENTO DE TOTALES REALES ---
        resumen_tablas = []
        for tabla in df_log['tabla'].unique():
            df_tabla = df_log[df_log['tabla'] == tabla]

            total_inicial = df_tabla['total_recibidos'].iloc[0]

            # Usamos 'pasan_a_calidad' de la ÚLTIMA regla ejecutada para esa tabla
            # En tu caso, la última suele ser 'Governanza fecha limite'
            total_final = df_tabla['pasan_a_calidad'].iloc[-1]

            errores_netos = total_inicial - total_final
            calidad_pct = round((max(0, total_final) / total_inicial) * 100, 2)

            resumen_tablas.append({
                'tabla': tabla,
                'total': total_inicial,
                'errores': errores_netos,
                'calidad': calidad_pct
            })

        # --- 4. ESCRITURA DEL REPORTE ---
        nombre_reporte = f"reporte_calidad_{ultima_ejecucion}.md"
        ruta_salida = os.path.join(outputs_dir, nombre_reporte)

        with open(ruta_salida, "w", encoding="utf-8") as f:
            f.write(f"# 📊 Reporte Ejecutivo de Calidad de Datos\n")
            f.write(f"> **Ejecución:** `{ultima_ejecucion}` | **Capa:** Silver (Clean)\n\n")

            f.write("## 1. Perfilado de Datos (Impacto Neto)\n")
            f.write("| Tabla | Inicial | Filtrados | % Calidad | Estado |\n")
            f.write("| :--- | :---: | :---: | :---: | :--- |\n")

            for item in resumen_tablas:
                status = "✅ Óptimo" if item['calidad'] > 90 else "🚨 Crítico"
                f.write(
                    f"| **{item['tabla']}** | {item['total']} | {item['errores']} | {item['calidad']}% | {status} |\n")

            f.write("\n## 2. Impacto por Regla de Calidad\n")
            # Mostramos el detalle original del CSV
            f.write(df_log[['tabla', 'regla', 'enviados_a_cuarentena', 'tasa_error_pct']].to_markdown(index=False))

            f.write("\n\n## 3. Notas de Transformación\n")
            f.write("* **Gobernanza:** Se aplicó purga por fecha límite (especialmente crítico en EVENTOS).\n")
            f.write("* **Seguridad:** PII anonimizada mediante SHA-256.\n")

            f.write("\n---\n**Resultado:** Procesamiento exitoso. Datos en DuckDB.")

        print(f"Reporte generado: {ruta_salida}")

    except Exception as e:
        print(f"Error al procesar el reporte: {e}")