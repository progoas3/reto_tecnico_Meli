import duckdb
import os

def cargar_tabla_a_db(df, nombre_tabla, db_path="./data/analitycs/mercado_libre.db"):
    """
    Carga un DataFrame en DuckDB filtrando automáticamente las columnas
    para que coincidan con el esquema técnico.
    """
    # 1. Asegurar carpeta
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # 2. Conectar
    con = duckdb.connect(db_path)

    try:
        # --- ESQUEMAS TÉCNICOS ---
        schemas = {
            "clientes": """
                CREATE TABLE IF NOT EXISTS clientes (
                    cliente_id VARCHAR(10) PRIMARY KEY,
                    nombre VARCHAR(100),
                    apellido VARCHAR(100),
                    email VARCHAR(200),
                    telefono VARCHAR(30),
                    ciudad VARCHAR(100),
                    pais VARCHAR(50),
                    segmento VARCHAR(20),
                    fecha_registro DATE,
                    fecha_consentimiento DATE,
                    activo BOOLEAN,
                    data_owner VARCHAR(100),
                    clasificacion_dato VARCHAR(20)
                )""",
            "productos": """
                CREATE TABLE IF NOT EXISTS productos (
                    producto_id VARCHAR(10) PRIMARY KEY,
                    nombre_producto VARCHAR(200),
                    categoria VARCHAR(50),
                    subcategoria VARCHAR(50),
                    precio_venta DECIMAL(12,2),
                    costo DECIMAL(12,2),
                    stock_disponible INTEGER,
                    proveedor_id VARCHAR(10),
                    nombre_proveedor VARCHAR(200),
                    fecha_creacion DATE,
                    activo BOOLEAN
                )""",
            "pedidos": """
                CREATE TABLE IF NOT EXISTS pedidos (
                    pedido_id VARCHAR(12) PRIMARY KEY,
                    cliente_id VARCHAR(10),
                    fecha_pedido DATE,
                    fecha_entrega DATE,
                    estado VARCHAR(20),
                    canal VARCHAR(20),
                    metodo_pago VARCHAR(30),
                    pais_envio VARCHAR(50),
                    total_bruto DECIMAL(14,2),
                    descuento_pct DECIMAL(5,2),
                    total_neto DECIMAL(14,2)
                )""",
            "detalle_pedidos": """
                CREATE TABLE IF NOT EXISTS detalle_pedidos (
                    item_id VARCHAR(13) PRIMARY KEY,
                    pedido_id VARCHAR(12),
                    producto_id VARCHAR(10),
                    cantidad INTEGER,
                    precio_unitario DECIMAL(12,2),
                    descuento_pct DECIMAL(5,2),
                    subtotal DECIMAL(14,2)
                )""",
            "eventos": """
                CREATE TABLE IF NOT EXISTS eventos (
                    evento_id VARCHAR(12) PRIMARY KEY,
                    cliente_id VARCHAR(10),
                    session_id VARCHAR(14),
                    tipo_evento VARCHAR(30),
                    timestamp TIMESTAMP,
                    producto_id VARCHAR(10),
                    dispositivo VARCHAR(20),
                    pais VARCHAR(50),
                    duracion_seg INTEGER
                )"""
        }

        # 3. Reiniciar tabla (Idempotencia)
        con.execute(f"DROP TABLE IF EXISTS {nombre_tabla} CASCADE")

        if nombre_tabla in schemas:
            # Crear con esquema oficial
            con.execute(schemas[nombre_tabla])

            # --- SOLUCIÓN AL ERROR DE COLUMNAS ---
            # Obtenemos las columnas exactas que DuckDB espera para esta tabla
            info_tabla = con.execute(f"PRAGMA table_info({nombre_tabla})").fetchall()
            columnas_que_quiere_db = [col[1] for col in info_tabla]

            # Filtramos el DataFrame para que solo tenga esas columnas y en ese orden
            # Esto ignora automáticamente 'data_owner', 'clasificacion_dato' o cualquier otra sobra.
            df_para_insertar = df[columnas_que_quiere_db].copy()

            # Registrar e insertar
            con.register('df_view', df_para_insertar)
            con.execute(f"INSERT INTO {nombre_tabla} SELECT * FROM df_view")
            con.unregister('df_view')
            print(f"   [DB] Tabla '{nombre_tabla}' cargada exitosamente.")
        else:
            # Si no hay esquema definido, carga normal (esquema automático)
            con.execute(f"CREATE TABLE {nombre_tabla} AS SELECT * FROM df")
            print(f"   [DB] Tabla '{nombre_tabla}' cargada con esquema automático.")

    except Exception as e:
        print(f"   [ERROR DB] Fallo en {nombre_tabla}: {str(e)}")
        raise e
    finally:
        con.close()