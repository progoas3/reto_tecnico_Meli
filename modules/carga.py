import os
import pandas as pd


def cargar_y_presentar_datos():
    # 1. Obtener la ruta base del proyecto (donde está pipeline.py)
    # Subimos un nivel desde 'modules' para llegar a la raíz
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path_raw = os.path.join(base_path, "data", "raw")

    print(f"Buscando datos en: {path_raw}")
    print("=" * 60)

    if not os.path.exists(path_raw):
        raise FileNotFoundError(f"No se encontró la carpeta: {path_raw}")

    archivos = [f for f in os.listdir(path_raw) if f.endswith('.csv')]
    datasets = {}

    for archivo in archivos:
        nombre_tabla = archivo.replace('.csv', '')
        path_completo = os.path.join(path_raw, archivo)

        df = pd.read_csv(path_completo)
        datasets[nombre_tabla] = df

        print(f"Tabla: {nombre_tabla.upper()} | Registros: {len(df)}")

    return datasets


def cargar_y_presentar_datos_catalogo():
    # 1. Obtener la ruta base del proyecto (donde está pipeline.py)
    # Subimos un nivel desde 'modules' para llegar a la raíz
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path_catalogos = os.path.join(base_path, "data", "catalogos")

    print(f"Buscando datos en: {path_catalogos}")
    print("=" * 60)

    if not os.path.exists(path_catalogos):
        raise FileNotFoundError(f"No se encontró la carpeta: {path_catalogos}")

    archivos = [f for f in os.listdir(path_catalogos) if f.endswith('.csv')]
    datasets = {}

    for archivo in archivos:
        nombre_tabla = archivo.replace('.csv', '')
        path_completo = os.path.join(path_catalogos, archivo)

        df = pd.read_csv(path_completo)
        datasets[nombre_tabla] = df

        print(f"Tabla: {nombre_tabla.upper()} | Registros: {len(df)}")

    return datasets