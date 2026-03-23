# reto_tecnico_Meli

## 📌 Descripción general
Este repositorio contiene una solución completa para una prueba técnica de Data Engineering (RetailTech / E-commerce Analytics):
- Generación de dataset sintético (`generar_dataset.py`).
- Pipeline de calidad de datos y transformaciones (`pipeline.py`).
- Control de calidad + gobernanza (`modules/` y `governance/`).
- Consultas analíticas SQL (`modules/analisisSQL.py`) y exportación en `outputs/resultadosSQL`.
- Agente conversacional con capacidades de consulta SQL (`agente.py`, `test_agente.py`).

## 📁 Estructura del proyecto
- `generar_dataset.py`: crea datos de ejemplo en `data/raw/`.
- `pipeline.py`: pipeline principal de ingesta, validación, limpieza, carga, consulta y reporte.
- `agente.py`: agente GenAI que responde preguntas mediante SQL + reglas de gobierno.
- `test_agente.py`: caso de prueba de preguntas predefinidas para el agente.
- `config.yaml`: parámetros de configuración (API key Groq).
- `modules/`: módulos reutilizables:
  - `carga.py`: carga raw y catalogos.
  - `reglas_calidad.py`: reglas de validación de datos (duplicados, nulos, fechas, etc.).
  - `creacion_catalogos.py`: funcion encargada de separar los datos en tablas y crear catalogos de esquema(HTLM) (flujo aparte para el catalogo).
  - `database.py`: carga a DuckDB / DB local.
  - `analisisSQL.py`: ejecución de consultas y generación de resultados.
  - `reporte_calidad.py`: reportes y métricas de calidad.
  - `governance.py`: políticas de retención y mascareo PII.
  - `utils.py`: utilidades generales funciones de auditoria y cuarentena.
- `data/`:
  - `raw/`: datos originales generados (clientes, pedidos, productos, etc.).
  - `clean/`: datos resultantes después de aplicar reglas de calidad.
  - `analitycs/`: base de datos local (`mercado_libre.db`) y resultados analíticos.
  - `catalogos/`: catálogos de esquema/tablas.
  - `quarantine/`: registros incidentados (error/validation).
- `outputs/`:
  - `reporte_calidad_*.md`: reporte de calidad generado.
  - `log_tiempos_pasos.csv`: métricas de duración del pipeline.
  - `resultadosSQL/`: resultados de cada query analítica (`q1_...`, etc.).
- `governance/`: documentación de gobernanza de datos (`reglas_calidad.md`, `governance.html`).
- `notebooks/`: análisis exploratorio y visualizaciones interactivas.
- `logs`: Logs de ejecucion del proceso ETL, temas de auditoria y cuarentena.
- `presentacion/`: espacio para entregable en slides.

## ▶️ Pre-requisitos
1. Python 3.10+ (preferible 3.11).
2. Instalar dependencias 
```bash
pip install -r requirements.txt
```

Ejemplo:
```bash
pip install pandas duckdb pyyaml langchain-groq
```

## 🚀 Cómo ejecutar el flujo principal (pipeline)
1. Ejecutar generación del dataset:
```bash
python generar_dataset.py
```
* Se require el lanzamiento de este script para crear los archivos CSV en `data/catalagos/` para la separacion de los catalogos
```bash
python modules/creacion_catalogos.py
```

2. Verificar que los CSV aparecen en `data/raw/`.
3. Ejecutar pipeline de calidad completo:
```bash
python pipeline.py
```
4. Salida:
- `data/clean/*.csv` (datos limpios + masking PII).
- `data/analitycs/mercado_libre.db` (base de datos local cargada).
- `outputs/resultadosSQL/` con claves de consultas.
- `outputs/reporte_calidad_*.md` y `outputs/log_tiempos_pasos.csv`.
- `data/quarantine/<timestamp>/` con filas no conformes.

## 🧪 Probar agente conversacional
1. Configurar `config.yaml` con API key para el modelo (ej. GROQ).
2. Ejecutar:
```bash
python agente.py
```
3. Ingresar preguntas en español, p.ej.: 
- "¿Nuestro stock critico es?"
- "¿Cual es el schema de la tabla cliente?"
-  "¿Puedes darme la informacion del mejor cliente?"
4. Cierre con `salir` o `exit`.

### Pruebas automáticas del agente
```bash
python test_agente.py
```

## 🛡️ Gobernanza y calidad implementada
- Reglas de calidad ejecutadas:
  - Duplicados
  - Nulos críticos
  - Emails con formato
  - Montos/moneda positivos
  - Fechas válidas
  - Categóricos válidos
  - IDs formato correcto
  - Hashing PII
  - Estándar de teléfonos
  - Políticas de retención por fecha
- PII detectado y protecciones:
  - hashed `email`, `telefono` con formato seguro
  - enmascaramiento en salida de agente (ver `agente.py` -> `validar_pii`).
- Clasificación de tablas y linaje debe consultarse en `governance/reglas_calidad.md`.

## 🧾 Contenidos extra
- `INSTRUCCIONES.md`: descripción del desafío y criterios de evaluación.
- `governance/catalogo_datos.md` (bonus): catálogo de datos y ownership.
- `notebooks/analisis_exploratorio.ipynb`: análisis interactivo, gráficos y métricas.
- `governance/governance.html`: Catalogo visual interactivo (bonus).

## 💡 Recomendaciones
1. Correr primero `generar_dataset.py` y luego `pipeline.py`.
2. Confirmar `config.yaml` con credenciales antes de levantar agente.
3. Inspeccionar logs de `data/quarantine/` y `outputs/` para debug.
4. Subir `presentacion/RetailTech_DataEngineer_[TuNombre].pdf` con resultados.

---
