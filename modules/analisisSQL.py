import duckdb
import os
import pandas as pd


def ejecutar_consultas_analiticas(db_path="./data/analitycs/mercado_libre.db", output_folder="./outputs/resultadosSQL"):
    """
    Ejecuta el set de consultas analíticas desglosando la lógica técnica de cada una.
    """
    print(f"\n Generando Inteligencia de Negocio con desglose técnico...")
    os.makedirs(output_folder, exist_ok=True)
    con = duckdb.connect(db_path)

    queries = {
        "q1_top_10_clientes_valor": """
            -- LÓGICA: Unimos CLIENTES con PEDIDOS para cruzar nombres con transacciones.
            -- Se usa SUM sobre 'total_neto' para obtener la facturación real después de descuentos.
            -- El GROUP BY por ID y Nombre asegura que no mezclemos clientes con nombres similares.
            SELECT 
                c.cliente_id, 
                c.nombre, 
                c.apellido, 
                SUM(p.total_neto) as total_gastado
            FROM clientes c
            JOIN pedidos p ON c.cliente_id = p.cliente_id
            GROUP BY c.cliente_id, c.nombre, c.apellido
            ORDER BY total_gastado DESC
            LIMIT 10
        """,

        "q2_ventas_por_pais": """
            -- LÓGICA: Agrupamos directamente en la tabla PEDIDOS usando 'pais_envio'.
            -- Contamos registros para el volumen de operaciones y sumamos montos para el peso financiero.
            SELECT 
                pais_envio, 
                COUNT(pedido_id) as total_pedidos, 
                SUM(total_neto) as ingresos_totales
            FROM pedidos
            GROUP BY pais_envio
            ORDER BY ingresos_totales DESC
        """,

        "q3_productos_mas_vendidos": """
            -- LÓGICA: Triple Join. DETALLE_PEDIDOS tiene las cantidades, PRODUCTOS tiene los nombres.
            -- Sumamos la columna 'cantidad' de la tabla de detalles para ver el movimiento real de stock.
            SELECT 
                pr.nombre_producto, 
                SUM(dp.cantidad) as unidades_vendidas
            FROM detalle_pedidos dp
            JOIN productos pr ON dp.producto_id = pr.producto_id
            GROUP BY pr.nombre_producto
            ORDER BY unidades_vendidas DESC
            LIMIT 10
        """,

        "q4_conversion_eventos": """
            -- LÓGICA: Análisis de frecuencia sobre la tabla EVENTOS.
            -- Ayuda a identificar qué pasos del funnel (view, search, add_to_cart) son los más comunes.
            SELECT 
                tipo_evento, 
                COUNT(*) as cantidad_interacciones
            FROM eventos
            GROUP BY tipo_evento
            ORDER BY cantidad_interacciones DESC
        """,

        "q5_stock_critico": """
            -- LÓGICA: Filtro directo sobre PRODUCTOS.
            -- Buscamos registros donde 'stock_disponible' esté por debajo del umbral de seguridad (10).
            SELECT 
                nombre_producto, 
                stock_disponible, 
                categoria
            FROM productos
            WHERE stock_disponible < 10
            ORDER BY stock_disponible ASC
        """,

        "q6_pedidos_por_metodo_pago": """
            -- LÓGICA: Agregación por 'metodo_pago'.
            -- Útil para finanzas para ver la distribución entre efectivo, tarjeta o transferencia.
            SELECT 
                metodo_pago, 
                COUNT(*) as volumen_pedidos, 
                SUM(total_neto) as monto_total_acumulado
            FROM pedidos
            GROUP BY metodo_pago
            ORDER BY monto_total_acumulado DESC
        """,

        "q7_ticket_promedio_por_canal": """
            -- LÓGICA: Usamos la función AVG sobre 'total_neto'.
            -- Comparamos si el cliente gasta más en la App (mobile), Web o Tienda Física.
            SELECT 
                canal, 
                AVG(total_neto) as ticket_promedio
            FROM pedidos
            GROUP BY canal
        """,

        "q8_tasa_activacion_clientes": """
            -- LÓGICA:
            -- 1. Contamos el total de registros en la tabla CLIENTES (El universo).
            -- 2. Contamos cuántos de esos IDs aparecen en la tabla PEDIDOS (Los activos).
            -- 3. Calculamos el porcentaje de activación (Activos / Total * 100).
            SELECT 
                (SELECT COUNT(*) FROM clientes) as total_universo_clientes,
                COUNT(DISTINCT p.cliente_id) as clientes_activos_con_compra,
                ROUND(
                    CAST(COUNT(DISTINCT p.cliente_id) AS FLOAT) * 100 / 
                    (SELECT COUNT(*) FROM clientes), 
                    2
                ) as porcentaje_activacion
            FROM pedidos p
        """,

        "q9_descuento_promedio_por_categoria": """
            -- LÓGICA: Join entre PEDIDOS (donde está el descuento) y PRODUCTOS vía DETALLE.
            -- Permite ver qué categorías están siendo más "agresivas" comercialmente.
            SELECT 
                pr.categoria, 
                AVG(p.descuento_pct) as porcentaje_descuento_medio
            FROM pedidos p
            JOIN detalle_pedidos dp ON p.pedido_id = dp.pedido_id
            JOIN productos pr ON dp.producto_id = pr.producto_id
            GROUP BY pr.categoria
            ORDER BY porcentaje_descuento_medio DESC
        """,

        "q10_duracion_sesiones_dispositivo": """
            -- LÓGICA: Análisis de performance de usuario en la tabla EVENTOS.
            -- Calculamos el promedio de 'duracion_seg' segmentado por el tipo de hardware/dispositivo.
            SELECT 
                dispositivo, 
                AVG(duracion_seg) as promedio_segundos_sesion
            FROM eventos
            GROUP BY dispositivo
        """
    }

    try:
        for nombre, sql in queries.items():
            df_res = con.execute(sql).df()
            ruta_csv = os.path.join(output_folder, f"{nombre}.csv")
            df_res.to_csv(ruta_csv, index=False)
            print(f"   [SQL] Procesado: {nombre} -> Exportado a CSV.")

    except Exception as e:
        print(f"   [ERROR SQL] Error en la ejecución: {str(e)}")
    finally:
        con.close()

    print(f"\nProceso analítico finalizado. Archivos listos en: {output_folder}")