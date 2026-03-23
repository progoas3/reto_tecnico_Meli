import os
import duckdb
import pandas as pd
import re
from langchain_groq import ChatGroq
import yaml


# CARGA DE CONFIGURACIÓN YAML
def cargar_configuracion():
    ruta = "config.yaml"
    if not os.path.exists(ruta):
        # Si el archivo no existe, podrías crear uno por defecto o lanzar error
        raise FileNotFoundError(f"No se encontró el archivo {ruta}")

    with open(ruta, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

config = cargar_configuracion()

# 1. CONFIGURACIÓN
os.environ["GROQ_API_KEY"] = config['credentials']['groq_api_key']
DB_PATH = "./data/analitycs/mercado_libre.db"
CATALOGOS_PATH = "./data/catalogos/"


# 2. HERRAMIENTAS

# Mecanismo de validación de output
def validar_pii(texto: str) -> bool:
    """
    Verifica si el texto contiene patrones de PII (emails o teléfonos).
    Retorna True si es SEGURO, False si contiene datos sensibles.
    """
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    # Bloquea números de más de 7 dígitos que parezcan teléfonos
    phone_pattern = r'\b\d{7,15}\b'

    if re.search(email_pattern, texto) or re.search(phone_pattern, texto):
        return False
    return True


def obtener_schemas_desde_csv():
    if not os.path.exists(CATALOGOS_PATH):
        return "Error: No se encuentra la carpeta de catálogos."

    schemas_detallados = []

    for archivo in os.listdir(CATALOGOS_PATH):
        if archivo.endswith(".csv"):
            try:
                df = pd.read_csv(os.path.join(CATALOGOS_PATH, archivo))

                # [SEGURIDAD] El agente no debe conocer la existencia de columnas PII
                if 'es_pii' in df.columns:
                    df = df[df['es_pii'].str.upper() != 'SÍ']

                nombre_tabla = archivo.replace("catalogo_", "").replace(".csv", "").upper()
                header = f"\n📖 TABLA: {nombre_tabla}\n" + "—" * 60
                filas = []

                for _, row in df.iterrows():
                    is_pk = "🔑 " if str(row.get('constraint_tipo', '')).upper() == 'PK' else "  "
                    col = row['columna'].ljust(20)
                    tipo = f"[{row['tipo_dato']}]".ljust(15)
                    desc = f"| {row['descripcion']}"
                    filas.append(f"{is_pk}{col} {tipo} {desc}")

                tabla_str = header + "\n" + "\n".join(filas)
                schemas_detallados.append(tabla_str)
            except Exception:
                continue
    return "\n".join(schemas_detallados)


def ejecutar_sql(query: str):
    # [SEGURIDAD] Bloqueo preventivo si el LLM intenta adivinar nombres de columnas sensibles
    prohibidos = ['nombre', 'apellido', 'email', 'telefono']
    if any(p in query.lower() for p in prohibidos):
        return "ERROR DE SEGURIDAD: Acceso denegado a datos PII (información personal)."

    if not os.path.exists(DB_PATH):
        return f"Error: No se encuentra la base de datos en {DB_PATH}"

    con = duckdb.connect(DB_PATH)
    try:
        sql = query.split("Action Input:")[-1].strip().replace("`", "").replace("sql", "").split(";")[0]
        df = con.execute(sql).df()
        return df.to_string()
    except Exception as e:
        return f"Error en la consulta: {e}"
    finally:
        con.close()


# 3. LÓGICA DEL AGENTE
llm = ChatGroq(model_name="llama-3.3-70b-versatile", temperature=0)

SYSTEM_PROMPT = """Eres un Analista de Datos Senior de MeLi. Responde en español.
Tus respuestas DEBEN basarse en las siguientes definiciones técnicas de negocio:

REGLAS DE NEGOCIO (Provenientes de analisisSQL.py):
- Top Clientes: Sumar 'total_neto' de pedidos por cliente.
- Ventas por País: Agrupar por 'pais_envio' y sumar 'total_neto'.
- Productos más vendidos: Join entre 'detalle_pedidos' y 'productos' sumando 'cantidad'.
- Conversión: Contar registros por 'tipo_evento' en la tabla eventos.
- Stock Crítico: Productos con 'stock_disponible' < 10.
- Ticket Promedio: AVG de 'total_neto' en la tabla pedidos.
- Activación: (Clientes con pedido / Total clientes) * 100.
- Sesiones: AVG de 'duracion_seg' por 'dispositivo'.

TABLAS DISPONIBLES:
- clientes (cliente_id, nombre, apellido, ciudad)
- pedidos (pedido_id, cliente_id, fecha_pedido, total_neto, pais_envio, canal)
- productos (producto_id, nombre_producto, categoria, precio_venta, costo, stock_disponible)
- detalle_pedidos (pedido_id, producto_id, cantidad)
- eventos (evento_id, cliente_id, dispositivo, duracion_seg, tipo_evento)

FORMATO DE RESPUESTA:
Thought: Razonamiento lógico.
Action Input: Query SQL pura.
Final Answer: Explicación amigable de los datos.
"""


def agente_meli(pregunta_usuario):
    schemas_reales = obtener_schemas_desde_csv()
    prompt_con_contexto = f"{SYSTEM_PROMPT}\n\nESQUEMAS REALES (Usa estos campos prioritariamente):\n{schemas_reales}\n\nPregunta: {pregunta_usuario}\nThought:"

    try:
        respuesta_llm = llm.invoke(prompt_con_contexto).content
        resultado_final = ""

        if "Action Input:" in respuesta_llm:
            query_part = respuesta_llm.split("Action Input:")[1].split("Final Answer:")[0].strip()
            observacion = ejecutar_sql(query_part)
            segundo_prompt = f"{prompt_con_contexto}\n{respuesta_llm}\nObservation: {observacion}\nFinal Answer:"
            resultado_final = llm.invoke(segundo_prompt).content
        else:
            resultado_final = respuesta_llm

        # [SEGURIDAD] Validación de output final
        if not validar_pii(resultado_final):
            return "BLOQUEO DE SEGURIDAD: La respuesta contenía datos personales (PII) y ha sido interceptada."

        return resultado_final
    except Exception as e:
        return f"Error: {e}"


if __name__ == "__main__":
    print("\n" + "=" * 50 + "\n🛒 AGENTE ANALISTA MELI (MODO SEGURO PII)\n" + "=" * 50)
    while True:
        pregunta = input("🤔 Tu pregunta o salir: ")
        if pregunta.lower() in ["salir", "exit"]: break
        print("\n🔍 Analizando con lógica de negocio...")
        print(f"\n💡 RESPUESTA:\n{agente_meli(pregunta)}\n" + "-" * 50)