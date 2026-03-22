from modules.carga import cargar_y_presentar_datos_catalogo
import pandas as pd


def aplicar_politicas_gobierno_autonoma(df_work, nombre_tabla):
    """
    Retorna:
    - df_work: DataFrame con los datos vigentes.
    - df_eliminados: DataFrame con los registros que superaron la fecha de retención.
    """
    # Creamos un DataFrame vacío por defecto para los eliminados
    df_eliminados = pd.DataFrame()

    catalogos = cargar_y_presentar_datos_catalogo()
    nombre_cat = f"catalogo_{nombre_tabla.lower()}"
    df_cat = catalogos.get(nombre_cat)

    if df_cat is None:
        print(f"Catálogo {nombre_cat} no encontrado.")
        return df_work, df_eliminados

    try:
        clasificacion = str(df_cat['clasificacion'].iloc[0]).strip()
        retencion_valor = int(df_cat['retencion_dias'].iloc[0])

        # FILTRO DE RETENCIÓN
        cols_fecha = [c for c in df_work.columns if 'fecha' in c.lower() or 'timestamp' in c.lower()]

        if cols_fecha:
            col_ref = cols_fecha[0]
            df_work[col_ref] = pd.to_datetime(df_work[col_ref])

            # Cálculo de fecha límite
            fecha_corte = pd.Timestamp.now() - pd.Timedelta(days=retencion_valor)

            # Identificamos registros vencidos
            mask_antiguos = df_work[col_ref] < fecha_corte

            # SEPARAMOS LOS DATOS:
            df_eliminados = df_work[mask_antiguos].copy()  # Los que se van al log
            df_work = df_work[~mask_antiguos].copy()  # Los que se quedan

        print(f"[GOBIERNO] {nombre_tabla.upper()}: Nivel {clasificacion.upper()}. "
              f"Se identificaron {len(df_eliminados)} registros vencidos.")

    except Exception as e:
        print(f"Error procesando Governance para {nombre_tabla}: {e}")

    return df_work, df_eliminados