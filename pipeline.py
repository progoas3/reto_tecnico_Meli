from modules.carga import cargar_y_presentar_datos
from modules.reglas_calidad import *
from modules.utils import *
from modules.reporte_calidad import *
from datetime import datetime
import os

# 1. Cargar los datos (Capa Raw/Bronze)
datasets = cargar_y_presentar_datos()
datasets_finales = {}

# 2. Configuraciones
pk_map = {
    'CLIENTES': 'cliente_id',
    'PEDIDOS': 'pedido_id',
    'PRODUCTOS': 'producto_id',
    'EVENTOS': 'evento_id',
    'DETALLE_PEDIDOS': 'item_id'
}

config_nulos = {
    'clientes': ['cliente_id', 'email', 'fecha_registro'],
    'pedidos': ['pedido_id', 'cliente_id', 'fecha_pedido'],
    'productos': ['producto_id', 'nombre_producto', 'precio_venta'],
    'eventos': ['evento_id', 'session_id', 'tipo_evento', 'duracion_seg'],
    'detalle_pedidos': ['item_id', 'pedido_id', 'producto_id']
}

# --- AJUSTE DE CARPETAS ---
timestamp_ejecucion = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

# Definimos las dos rutas que pide la nueva función
carpeta_quarantine = os.path.join("./data/quarantine", timestamp_ejecucion)
carpeta_logs = os.path.join("./logs", timestamp_ejecucion)

os.makedirs(carpeta_quarantine, exist_ok=True)
os.makedirs(carpeta_logs, exist_ok=True)
os.makedirs("./data/clean", exist_ok=True)

print(f"\nINICIANDO PIPELINE DE CALIDAD INTEGRAL - {timestamp_ejecucion}")
print("=" * 80)

for tabla, pk in pk_map.items():
    nombre_key = tabla.lower()
    if nombre_key in datasets:
        df_work = datasets[nombre_key].copy()

        # --- R1: DUPLICADOS ---
        n_entrada = len(df_work)
        df_work, err1 = validacion_duplicados(df_work, pk)
        registrar_auditoria(nombre_key, "R1_Duplicados", pk, n_entrada, len(df_work), carpeta_logs)
        registrar_error_y_log(nombre_key, "R1_Duplicados", err1, pk, carpeta_quarantine, carpeta_logs)

        # --- R2: NULOS CRÍTICOS ---
        n_entrada = len(df_work)
        cols_nulos = config_nulos.get(nombre_key, [pk])
        # Convertimos la lista de nulos a un string para el log
        nombres_cols_nulos = ", ".join(cols_nulos)
        df_work, err2 = validacion_nulos_criticos(df_work, cols_nulos)
        registrar_auditoria(nombre_key, "R2_Nulos", nombres_cols_nulos, n_entrada, len(df_work), carpeta_logs)
        registrar_error_y_log(nombre_key, "R2_Nulos", err2, nombres_cols_nulos, carpeta_quarantine, carpeta_logs)

        # --- R3: EMAILS Y OWNERS ---
        n_entrada = len(df_work)
        campo_email = "email" if nombre_key == "clientes" else "owner_email" # Ajuste según tabla
        df_work, err3 = validacion_formato_email(df_work)
        registrar_auditoria(nombre_key, "R3_Email", campo_email, n_entrada, len(df_work), carpeta_logs)
        registrar_error_y_log(nombre_key, "R3_Email_Invalido", err3, campo_email, carpeta_quarantine, carpeta_logs)

        # --- R4: VALORES POSITIVOS (MONEDA) ---
        n_entrada = len(df_work)
        # Identificamos el campo de dinero según la tabla
        campo_moneda = "precio_venta" if nombre_key == "productos" else "monto_total"
        df_work, err4 = validacion_valores_positivos(df_work)
        registrar_auditoria(nombre_key, "R4_Moneda", campo_moneda, n_entrada, len(df_work), carpeta_logs)
        registrar_error_y_log(nombre_key, "R4_Moneda_Invalida", err4, campo_moneda, carpeta_quarantine, carpeta_logs)

        # --- R5: FECHAS COHERENTES ---
        n_entrada = len(df_work)
        campo_fecha = "fecha_pedido" if "pedido" in nombre_key else "fecha_registro"
        df_work, err5 = validacion_fechas_dinamica(df_work)
        registrar_auditoria(nombre_key, "R5_Fechas", campo_fecha, n_entrada, len(df_work), carpeta_logs)
        registrar_error_y_log(nombre_key, "R5_Fecha_Invalida", err5, campo_fecha, carpeta_quarantine, carpeta_logs)

        # --- R6: CATEGORÍAS Y ESTADOS ---
        n_entrada = len(df_work)
        campo_cat = "tipo_evento" if nombre_key == "eventos" else "categoria"
        df_work, err6 = validacion_categoricos_dinamica(df_work, nombre_key)
        registrar_auditoria(nombre_key, "R6_Categoricos", campo_cat, n_entrada, len(df_work), carpeta_logs)
        registrar_error_y_log(nombre_key, "R6_Categoria_Invalida", err6, campo_cat, carpeta_quarantine, carpeta_logs)

        # --- R7: FORMATO ESTRUCTURAL DE IDS ---
        n_entrada = len(df_work)
        df_work, err7 = validacion_formato_ids(df_work, pk)
        # OJO: Aquí corregí len(err7) por len(df_work) para que el log de auditoría cuadre el balance
        registrar_auditoria(nombre_key, "R7_Formato_ID", pk, n_entrada, len(df_work), carpeta_logs)
        registrar_error_y_log(nombre_key, "R7_ID_Mal_Formato", err7, pk, carpeta_quarantine, carpeta_logs)

        # --- R8: HASHING PII (DATOS SENSIBLES) ---
        n_entrada = len(df_work)
        campo_pii = "email, nombre, telefono" # Campos que usualmente hasheas
        df_work, _ = aplicar_hashing_pii(df_work)
        registrar_auditoria(nombre_key, "R8_Hashing_PII", campo_pii, n_entrada, len(df_work), carpeta_logs)

        # --- GUARDAR RESULTADOS ---
        datasets_finales[nombre_key] = df_work
        ruta_clean = os.path.join("./data/clean", f"{nombre_key}_clean.csv")
        df_work.to_csv(ruta_clean, index=False)

        print(f"{nombre_key.upper()}: Procesada (Original: {n_entrada} | Final: {len(df_work)})")


generar_reporte_ejecutivo()

print("=" * 80)
print(f"Pipeline finalizado con éxito.")
print(f"Archivos limpios en: ./data/clean/")
print(f"Reportes y Logs en: {carpeta_logs}")
print(f"Evidencia de errores en: {carpeta_quarantine}")