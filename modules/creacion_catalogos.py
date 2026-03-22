import pandas as pd
import os


def generar_catalogos_y_portal():
    # 1. Configuración de Rutas
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ruta_diccionario = os.path.join(base_dir, "data", "raw", "diccionario_datos.csv")
    ruta_salida = os.path.join(base_dir, "data", "catalogos")

    if not os.path.exists(ruta_diccionario):
        print(f"Error: No se encontró {ruta_diccionario}")
        return

    # 2. Cargar y Generar CSVs
    df_dict = pd.read_csv(ruta_diccionario)
    os.makedirs(ruta_salida, exist_ok=True)
    tablas = sorted(df_dict['tabla'].unique())

    # Guardaremos el HTML de las tablas aquí
    html_sections = ""
    sidebar_links = ""

    print(f"Generando catálogos en: {ruta_salida}")

    for tabla in tablas:
        df_tabla = df_dict[df_dict['tabla'] == tabla].copy()

        # Exportar CSV
        nombre_csv = f"catalogo_{tabla}.csv"
        df_tabla.to_csv(os.path.join(ruta_salida, nombre_csv), index=False)

        # --- Lógica para el HTML ---
        id_tabla = tabla.replace(" ", "_")
        sidebar_links += f'<a class="nav-link" href="#{id_tabla}">{tabla.upper()}</a>\n'

        # Convertimos el DataFrame a tabla HTML de Bootstrap
        tabla_html = df_tabla.to_html(classes='table table-striped table-hover table-sm', index=False, border=0)

        html_sections += f"""
        <div id="{id_tabla}" class="mb-5 mt-4">
            <h3 class="border-bottom pb-2 text-primary">{tabla.upper()}</h3>
            <p class="text-muted small">Metadata técnica y de gobierno para la entidad {tabla}.</p>
            <div class="table-responsive">
                {tabla_html}
            </div>
            <a href="{nombre_csv}" class="btn btn-outline-success btn-sm" download>📥 Descargar CSV</a>
        </div>
        """

    # 3. Construir el archivo INDEX.HTML
    html_final = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Data Governance Portal - Meli Challenge</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>
            body {{ font-size: 0.85rem; background-color: #f8f9fa; }}
            .sidebar {{ position: fixed; top: 0; left: 0; height: 100vh; width: 200px; background: #343a40; padding-top: 20px; }}
            .sidebar .nav-link {{ color: #adb5bd; }}
            .sidebar .nav-link:hover {{ color: white; background: #495057; }}
            .sidebar .nav-link.active {{ color: white; background: #0d6efd; }}
            .content {{ margin-left: 220px; padding: 20px; }}
            .table-responsive {{ background: white; padding: 15px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }}
            th {{ background-color: #f1f3f5 !important; }}
        </style>
    </head>
    <body>
        <div class="sidebar d-flex flex-column p-3">
            <h5 class="text-white border-bottom pb-2">📦 Tablas</h5>
            <nav class="nav flex-column">
                {sidebar_links}
            </nav>
        </div>
        <div class="content">
            <div class="d-flex justify-content-between align-items-center border-bottom mb-4">
                <h2>📊 Portal de Gobierno de Datos</h2>
                <span class="badge bg-info text-dark">Versión 1.0 - Meli Challenge</span>
            </div>
            <div class="alert alert-light border">
                <strong>Instrucciones:</strong> Selecciona una tabla en el menú lateral para ver su diccionario de datos, clasificación de PII y políticas de retención.
            </div>
            {html_sections}
        </div>
    </body>
    </html>
    """

    ruta_html = os.path.join('../', "governance", "governance.html")
    with open(ruta_html, "w", encoding="utf-8") as f:
        f.write(html_final)

    print("-" * 50)
    print(f"Proceso completado.")
    print(f"Portal generado en: {ruta_html}")


if __name__ == "__main__":
    generar_catalogos_y_portal()