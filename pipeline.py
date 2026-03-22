from modules.carga import cargar_y_presentar_datos
from modules.reglas_calidad import *
from modules.utils import *
from modules.reporte_calidad import *


def ejecutar_full_pipeline():
    # --- 0. INICIO DE BITÁCORA ---
    log_pasos = []
    inicio_total = time.time()

    def anotar_paso(tabla, paso, t_inicio):
        """Calcula el tiempo y guarda el registro en la bitácora"""
        t_fin = time.time()
        log_pasos.append({
            "tabla": tabla,
            "paso": paso,
            "duracion_seg": round(t_fin - t_inicio, 5),
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        return time.time()  # Reinicia el segundero para el siguiente paso

    # --- 1. CARGA DE DATOS ---
    t_ref = time.time()
    datasets = cargar_y_presentar_datos()
    t_ref = anotar_paso("SISTEMA", "Carga_Raw_Data", t_ref)

    # --- 2. CONFIGURACIONES ---
    pk_map = {
        'CLIENTES': 'cliente_id', 'PEDIDOS': 'pedido_id',
        'PRODUCTOS': 'producto_id', 'EVENTOS': 'evento_id',
        'DETALLE_PEDIDOS': 'item_id'
    }

    config_nulos = {
        'clientes': ['cliente_id', 'email', 'fecha_registro'],
        'pedidos': ['pedido_id', 'cliente_id', 'fecha_pedido'],
        'productos': ['producto_id', 'nombre_producto', 'precio_venta'],
        'eventos': ['evento_id', 'session_id', 'tipo_evento', 'duracion_seg'],
        'detalle_pedidos': ['item_id', 'pedido_id', 'producto_id']
    }

    timestamp_ejecucion = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    carpeta_quarantine = os.path.join("./data/quarantine", timestamp_ejecucion)
    carpeta_logs = os.path.join("./logs", timestamp_ejecucion)

    os.makedirs(carpeta_quarantine, exist_ok=True)
    os.makedirs(carpeta_logs, exist_ok=True)
    os.makedirs("./data/clean", exist_ok=True)
    os.makedirs("./outputs", exist_ok=True)

    print(f"\nINICIANDO PIPELINE DE CALIDAD - {timestamp_ejecucion}")
    print("=" * 80)

    # --- 3. BUCLE DE PROCESAMIENTO (LA OBRA) ---
    for tabla, pk in pk_map.items():
        nombre_key = tabla.lower()
        if nombre_key in datasets:
            df_work = datasets[nombre_key].copy()
            n_entrada = len(df_work)

            # Iniciamos cronómetro para la tabla actual
            t_ref = time.time()

            # --- R1: DUPLICADOS ---
            df_work, err1 = validacion_duplicados(df_work, pk)
            registrar_auditoria(nombre_key, "R1_Duplicados", pk, n_entrada, len(df_work), carpeta_logs)
            registrar_error_y_log(nombre_key, "R1_Duplicados", err1, pk, carpeta_quarantine, carpeta_logs)
            t_ref = anotar_paso(nombre_key, "R1_Duplicados", t_ref)

            # --- R2: NULOS ---
            cols_nulos = config_nulos.get(nombre_key, [pk])
            df_work, err2 = validacion_nulos_criticos(df_work, cols_nulos)
            registrar_auditoria(nombre_key, "R2_Nulos", ", ".join(cols_nulos), n_entrada, len(df_work), carpeta_logs)
            registrar_error_y_log(nombre_key, "R2_Nulos", err2, ", ".join(cols_nulos), carpeta_quarantine, carpeta_logs)
            t_ref = anotar_paso(nombre_key, "R2_Nulos", t_ref)

            # --- R3: EMAILS ---
            df_work, err3 = validacion_formato_email(df_work)
            t_ref = anotar_paso(nombre_key, "R3_Emails", t_ref)

            # --- R4: MONEDA ---
            df_work, err4 = validacion_valores_positivos(df_work)
            t_ref = anotar_paso(nombre_key, "R4_Moneda", t_ref)

            # --- R5: FECHAS ---
            df_work, err5 = validacion_fechas_dinamica(df_work)
            t_ref = anotar_paso(nombre_key, "R5_Fechas", t_ref)

            # --- R6: CATEGÓRICOS ---
            df_work, err6 = validacion_categoricos_dinamica(df_work, nombre_key)
            t_ref = anotar_paso(nombre_key, "R6_Categoricos", t_ref)

            # --- R7: FORMATO IDS ---
            df_work, err7 = validacion_formato_ids(df_work, pk)
            t_ref = anotar_paso(nombre_key, "R7_Formato_ID", t_ref)

            # --- R8: HASHING PII ---
            df_work, _ = aplicar_hashing_pii(df_work)
            t_ref = anotar_paso(nombre_key, "R8_Hashing_PII", t_ref)

            # --- GUARDAR RESULTADOS CLEAN ---
            ruta_clean = os.path.join("./data/clean", f"{nombre_key}_clean.csv")
            df_work.to_csv(ruta_clean, index=False)
            t_ref = anotar_paso(nombre_key, "Escritura_CSV_Clean", t_ref)

            print(f"{tabla.upper()}: Procesada correctamente.")

    # --- 4. REPORTES Y CIERRE ---
    t_ref = time.time()
    generar_reporte_ejecutivo()
    anotar_paso("SISTEMA", "Generacion_Reporte_Calidad", t_ref)

    # --- 5. ESCRITURA FINAL DE LOGS DE TIEMPO EN OUTPUTS ---
    df_tiempos = pd.DataFrame(log_pasos)
    df_tiempos.to_csv("./outputs/log_tiempos_pasos.csv", index=False)

    duracion_total = round(time.time() - inicio_total, 2)
    print("=" * 80)
    print(f"PIPELINE FINALIZADO EN {duracion_total} SEGUNDOS.")
    print(f"Bitácora de tiempos guardada en: ./outputs/log_tiempos_pasos.csv")


if __name__ == "__main__":
    ejecutar_full_pipeline()