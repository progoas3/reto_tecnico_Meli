import pandas as pd
import hashlib


# --- REGLAS EXISTENTES (1 al 8) ---

def validacion_duplicados(df, pk):
    es_duplicado = df[pk].duplicated(keep='first')
    df_unicos = df[~es_duplicado].copy()
    df_duplicados = df[es_duplicado].copy()
    if not df_duplicados.empty:
        df_duplicados['motivo_error'] = f"ID duplicado en {pk}"
    return df_unicos, df_duplicados

def validacion_nulos_criticos(df, columnas_criticas):
    tiene_nulos = df[columnas_criticas].isnull().any(axis=1)
    df_limpio = df[~tiene_nulos].copy()
    df_nulos = df[tiene_nulos].copy()
    if not df_nulos.empty:
        columna_fallida = df_nulos[columnas_criticas].isnull().idxmax(axis=1)
        df_nulos['motivo_error'] = "Nulo crítico en: " + columna_fallida
    return df_limpio, df_nulos


def validacion_formato_email(df):
    """Valida correos en columnas específicas de email y dueños de datos."""
    # Buscamos exactamente 'email' o 'data_owner' en los nombres de las columnas
    cols_a_validar = [c for c in df.columns if any(x in c.lower() for x in ['email', 'data_owner'])]

    if not cols_a_validar:
        return df, pd.DataFrame()

    regex_email = r'^[\w\.-]+@[\w\.-]+\.\w{2,4}$'
    df_limpio = df.copy()
    errores = []

    for col in cols_a_validar:
        # Importante: astype(str) para evitar errores con posibles nulos remanentes
        es_invalido = ~df_limpio[col].astype(str).str.contains(regex_email, regex=True, na=False)
        df_err = df_limpio[es_invalido].copy()
        if not df_err.empty:
            df_err['motivo_error'] = f"Formato de correo inválido en {col}"
            errores.append(df_err)
        df_limpio = df_limpio[~es_invalido]

    df_errores_final = pd.concat(errores) if errores else pd.DataFrame()
    return df_limpio, df_errores_final


def validacion_valores_positivos(df):
    """
    Regla 4: Detecta columnas de dinero y valida que sean > 0.
    Específicamente para: precio_venta, costo, precio_unitario, subtotal, total_bruto, total_neto.
    """
    # Lista refinada basada en tus archivos para NO tocar 'stock' o 'descuento' que sí pueden ser 0
    palabras_clave = ['precio', 'costo', 'total_bruto', 'total_neto', 'subtotal', 'precio_unitario']
    cols_monetarias = [c for c in df.columns if any(p == c.lower() or p in c.lower() for p in palabras_clave)]

    if not cols_monetarias:
        return df, pd.DataFrame()

    df_limpio = df.copy()
    errores_acumulados = []

    for col in cols_monetarias:
        # Convertimos a numérico (maneja los 'N/A' que vi en tus archivos)
        valores_num = pd.to_numeric(df_limpio[col], errors='coerce')

        # OJO: En retail, el precio o costo NUNCA debe ser <= 0.
        # Si es NaN (N/A) también es un error de integridad.
        es_invalido = (valores_num <= 0) | (valores_num.isna())

        df_error = df_limpio[es_invalido].copy()
        if not df_error.empty:
            df_error['motivo_error'] = f"Valor monetario crítico inválido (<=0 o N/A) en: {col}"
            errores_acumulados.append(df_error)

        df_limpio = df_limpio[~es_invalido]

    df_errores_final = pd.concat(errores_acumulados) if errores_acumulados else pd.DataFrame()

    return df_limpio, df_errores_final


def validacion_fechas_dinamica(df):
    """
    Regla 5: Detecta columnas de fecha/tiempo y valida:
    1. Que no sean fechas futuras.
    2. Coherencia lógica (Entrega >= Pedido) si ambas existen.
    """
    # 1. Identificar columnas que representen tiempo
    palabras_clave = ['fecha', 'timestamp', 'created_at', 'updated_at']
    cols_fecha = [c for c in df.columns if any(p in c.lower() for p in palabras_clave)]

    if not cols_fecha:
        return df, pd.DataFrame()

    df_limpio = df.copy()
    errores_acumulados = []
    ahora = pd.Timestamp.now()

    for col in cols_fecha:
        # Convertimos a datetime (maneja formatos variados de tus CSV)
        df_limpio[col] = pd.to_datetime(df_limpio[col], errors='coerce')

        # --- Validación A: Fechas Futuras ---
        es_futura = df_limpio[col] > ahora

        df_error_futuro = df_limpio[es_futura].copy()
        if not df_error_futuro.empty:
            df_error_futuro['motivo_error'] = f"Fecha futura detectada en: {col}"
            errores_acumulados.append(df_error_futuro)

        # Limpiamos el dataframe principal
        df_limpio = df_limpio[~es_futura]

    # --- Validación B: Coherencia Pedido vs Entrega (Específico para pedidos.csv) ---
    if 'fecha_pedido' in df_limpio.columns and 'fecha_entrega' in df_limpio.columns:
        # Solo comparamos donde ambos tengan dato (entrega no sea nula)
        incoherente = (df_limpio['fecha_entrega'] < df_limpio['fecha_pedido']) & df_limpio['fecha_entrega'].notnull()

        df_error_logica = df_limpio[incoherente].copy()
        if not df_error_logica.empty:
            df_error_logica['motivo_error'] = "Fecha de entrega anterior a la fecha de pedido"
            errores_acumulados.append(df_error_logica)

        df_limpio = df_limpio[~incoherente]

    df_errores_final = pd.concat(errores_acumulados) if errores_acumulados else pd.DataFrame()

    return df_limpio, df_errores_final


def validacion_categoricos_dinamica(df, nombre_tabla):
    """
    Regla 6: Valida categorías y estados.
    Ajustada para normalizar texto y validar según el nombre de la tabla.
    """
    df_limpio = df.copy()
    errores_acumulados = []

    # 1. Diccionario Maestro de Catálogos (Basado en tus archivos reales)
    catalogos = {
        'eventos': {
            'tipo_evento': [
                'view', 'click', 'purchase', 'search',
                'add_to_cart', 'remove_from_cart',
                'checkout', 'page_view'
            ],
            'dispositivo': ['mobile', 'desktop', 'tablet'],
            'pais': ['Colombia', 'México', 'Argentina', 'Chile', 'Perú', 'Ecuador']
        },
        'pedidos': {
            'estado': ['entregado', 'pendiente', 'cancelado', 'en_camino', 'enviado', 'devuelto'],
            'canal': ['web', 'mobile', 'tienda_fisica', 'marketplace'],
            'metodo_pago': ['pse', 'efectivo', 'transferencia', 'credito_tienda', 'tarjeta_credito'],
            'pais_envio': ['Colombia', 'México', 'Argentina', 'Chile', 'Perú', 'Ecuador']
        },
        'productos': {
            'categoria': ['Deportes', 'Ropa', 'Hogar', 'Alimentos', 'Electrónica']
        }
    }

    # 2. VALIDACIÓN DE ENTRADA: ¿La tabla existe en el catálogo?
    # Forzamos minúsculas para evitar errores de "Eventos" vs "eventos"
    tabla_busqueda = nombre_tabla.lower().strip()

    if tabla_busqueda not in catalogos:
        # Si la tabla no requiere validación categórica, se retorna tal cual
        return df_limpio, pd.DataFrame()

    # 3. PROCESAMIENTO DE REGLAS
    config_tabla = catalogos[tabla_busqueda]

    for col, valores_validos in config_tabla.items():
        if col in df_limpio.columns:
            # NORMALIZACIÓN PARA LA COMPARACIÓN:
            # - Pasamos la lista de permitidos a minúsculas y quitamos espacios
            validos_norm = [str(v).lower().strip() for v in valores_validos]

            # - Creamos una serie temporal normalizada del DataFrame
            serie_comparar = df_limpio[col].astype(str).str.lower().str.strip()

            # Identificamos registros que NO están en el catálogo
            es_invalido = ~serie_comparar.isin(validos_norm)

            # Capturamos los errores
            df_error = df_limpio[es_invalido].copy()
            if not df_error.empty:
                df_error['motivo_error'] = (
                        f"Valor no permitido en '{col}'. "
                        f"Encontrado: " + df_error[col].astype(str) +
                        f" | Esperados: {valores_validos}"
                )
                errores_acumulados.append(df_error)

            # Filtramos el DataFrame original para dejar solo lo que pasó la calidad
            df_limpio = df_limpio[~es_invalido]

    # 4. CONSOLIDACIÓN DE ERRORES
    df_errores_final = pd.concat(errores_acumulados) if errores_acumulados else pd.DataFrame()

    return df_limpio, df_errores_final


def validacion_formato_ids(df, pk):
    """
    Regla 7: Valida que la PK cumpla el patrón estructural del negocio.
    Patrón esperado: 3-4 letras mayúsculas, un guion y números.
    Ejemplos válidos: CLI-001, PROD-0075, ITEM-0004167.
    """
    # Explicación del Regex:
    # ^[A-Z]{3,4} -> Inicia con 3 o 4 letras mayúsculas
    # -           -> Seguido de un guion obligatorio
    # \d+         -> Seguido de uno o más dígitos
    # $           -> Fin de la cadena
    patron_id = r'^[A-Z]{3,4}-\d+$'

    # Convertimos a string por si hay algún dato corrupto tipo numérico
    es_invalido = ~df[pk].astype(str).str.contains(patron_id, regex=True)

    df_limpio = df[~es_invalido].copy()
    df_error = df[es_invalido].copy()

    if not df_error.empty:
        df_error['motivo_error'] = f"ID con formato inválido en {pk}. Se esperaba prefijo-número."

    return df_limpio, df_error


def aplicar_hashing_pii(df):
    """
    Regla 8: Anonimiza datos sensibles (PII).
    Busca dinámicamente columnas de contacto y aplica SHA-256.
    """
    # Identificamos columnas PII comunes en tus CSV
    palabras_pii = ['nombre', 'apellido', 'email', 'telefono', 'documento']
    cols_a_hashear = [c for c in df.columns if any(p in c.lower() for p in palabras_pii)]

    # Excepción: No queremos hashear 'nombre_producto' o 'nombre_proveedor'
    # porque no son datos personales del cliente
    cols_a_hashear = [c for c in cols_a_hashear if 'producto' not in c and 'proveedor' not in c]

    if not cols_a_hashear:
        return df, pd.DataFrame()

    df_anon = df.copy()

    for col in cols_a_hashear:
        # Aplicar SHA-256
        df_anon[col] = df_anon[col].apply(
            lambda x: hashlib.sha256(str(x).strip().lower().encode()).hexdigest()
            if pd.notnull(x) and str(x).lower() != 'n/a' else x
        )


    return df_anon, pd.DataFrame()