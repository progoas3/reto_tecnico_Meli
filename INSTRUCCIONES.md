# Prueba Técnica — Data Engineer
## RetailTech S.A.S | Dominio: E-Commerce Analytics

---

> **Tiempo estimado:** 5 días calendario
> **Entregables:** repositorio Git (GitHub/GitLab público o privado con acceso compartido) + presentación
> **Herramientas:** todas las que uses deben ser **gratuitas y de código abierto** (ver sección de recursos al final)

---

## Contexto del negocio

**RetailTech S.A.S** es una empresa de comercio electrónico que opera en 6 países de Latinoamérica. El equipo de Data necesita construir una capa analítica confiable a partir de datos crudos de ventas, clientes, productos y comportamiento digital para responder preguntas estratégicas del negocio.

Tú eres el **Data Engineer** encargado de diseñar el pipeline, garantizar la calidad del dato y habilitarle un agente conversacional al equipo de negocio para que consulten los datos en lenguaje natural.

---

## Estructura del dataset

Descarga y ejecuta `generar_dataset.py` para obtener los archivos en `./data/`:

| Archivo | Descripción | Registros aprox. |
|---|---|---|
| `clientes.csv` | Información de clientes (PII) | 300 |
| `productos.csv` | Catálogo de productos | 80 |
| `pedidos.csv` | Cabecera de órdenes de compra | 1,200 |
| `detalle_pedidos.csv` | Líneas de cada pedido | ~4,000 |
| `eventos.csv` | Eventos digitales (web/app) | 4,000 |
| `diccionario_datos.csv` | Diccionario de datos oficial | — |

> **Importante:** el dataset tiene problemas de calidad **intencionales**. Parte de tu trabajo es detectarlos, documentarlos y resolverlos.

---

## Parte 1 — Desarrollo Técnico: SQL + Python

### 1.1 Calidad de Datos (Python)

Desarrolla un módulo de perfilado y limpieza:

1. **Perfil de datos:** para cada tabla, reporta:
   - Nulos por columna (absoluto y porcentaje)
   - Duplicados (a nivel de PK y a nivel de registro completo)
   - Estadísticas descriptivas de columnas numéricas
   - Cardinalidad de columnas categóricas

2. **Reglas de calidad:** define y aplica al menos **8 reglas de validación** documentadas. 

3. **Limpieza:** aplica las correcciones necesarias y genera archivos `*_clean.csv` en `./data/clean/`. Documenta cada transformación en un log (puede ser un CSV o JSON con `timestamp`, `tabla`, `campo`, `regla`, `registros_afectados`).

4. **Reporte de calidad:** genera un `reporte_calidad.md` con el resumen ejecutivo de hallazgos.

### 1.2 Transformaciones SQL

Crea un archivo `queries.sql` con minimo 10 consultas. Puedes usar **BigQuery**, **DuckDB** (recomendado, gratuito, corre en Python sin servidor) o **SQLite** o cualquier BBDD que sea sencilla de usar para ti. Cada consulta debe tener un comentario que explique su propósito y el impacto de negocio.


### 1.3 Pipeline en Python

Construye un pipeline modular `pipeline.py` que:

1. Lea los CSVs crudos
2. Ejecute las validaciones de calidad
3. Aplique las transformaciones y limpiezas
4. Cargue los datos limpios en una base de datos local
5. Ejecute las queries SQL y exporte los resultados como CSVs en `./outputs/`
6. Genere un log de ejecución con tiempos por etapa

**El código debe estar organizado en módulos/funciones bien nombrados.** No se evalúa el tamaño del código, sino su claridad y mantenibilidad.

---

## Parte 2 — Agente GenAI

### Contexto

El equipo de negocio (analistas, gerentes) necesita consultar los datos en lenguaje natural sin escribir SQL. Tu tarea es construir un **agente conversacional** que responda preguntas sobre los datos procesados.

### Requisitos del agente

El agente debe ser capaz de responder preguntas como:

- *"¿Cuáles fueron los 5 productos más vendidos en el segundo semestre de 2024?"*
- *"¿Qué canal tiene mejor tasa de conversión?"*
- *"¿Cuántos clientes B2B realizaron compras por encima de $500,000 COP?"*
- *"Resume el rendimiento de ventas de Colombia vs. México"*

### Especificaciones técnicas

1. **Framework:** usa uno de los siguientes (todos gratuitos):
   - [LangChain](https://www.langchain.com/) + [Ollama](https://ollama.com/) (modelos locales, recomendado para privacidad)
   - [LangChain](https://www.langchain.com/) + Google Gemini API (free tier)
   - [LlamaIndex](https://www.llamaindex.ai/) + cualquier LLM gratuito
   - [Claude API](https://www.anthropic.com/) (free tier / créditos de prueba)

2. **Herramientas del agente:** el agente debe tener al menos **1 tool**:
   - `ejecutar_sql(query: str)` — ejecuta una consulta SQL sobre BBDD de (GCP,AWS, Etc) y retorna el resultado
   - `obtener_esquema(tabla: str)` — retorna el esquema y descripción de una tabla desde el diccionario de datos
   - `resumir_reporte_calidad()` — retorna el resumen del reporte de calidad generado en la Parte 1

3. **Flujo ReAct o similar:** el agente debe razonar paso a paso (cadena de pensamiento visible), elegir la herramienta adecuada y generar la respuesta final en lenguaje natural.

4. **Interfaz:** puede ser:
   - Script CLI interactivo
   - Notebook interactivo (Jupyter)
   - UI simple con [Gradio](https://www.gradio.app/) o [Streamlit](https://streamlit.io/) (gratuitos)

5. **Casos de prueba:** incluye un archivo `test_agente.py` o una celda de notebook con al menos **5 preguntas de prueba** y sus respuestas esperadas (aproximadas).

### Restricciones importantes

> El agente es un **complemento**, no un reemplazo de tu trabajo técnico.
> - El agente **no debe generar los datos ni las queries desde cero** — debe usar los datos y transformaciones que tú ya construiste en la Parte 1.
> - Debes poder explicar **cada decisión de diseño del agente**: por qué elegiste ese framework, ese modelo, ese prompt system, esas tools.
> - Si el agente falla en responder algo, debe decirlo explícitamente en lugar de inventar datos.

---

## Parte 3 — Presentación y Análisis

### Entregable

Una presentación de **máximo 8 diapositivas** que cubra:

| Sección |  | Contenido |
|---|---|---|
| **Contexto y arquitectura** |  | Diagrama del pipeline, decisiones de diseño, herramientas elegidas |
| **Calidad de datos** |  | Hallazgos principales, reglas aplicadas, antes/después, impacto en el negocio |
| **Análisis e insights** |  | Respuestas a las 8 queries + tu query libre; visualizaciones (usa matplotlib, seaborn o plotly — todos gratuitos) |
| **Agente GenAI** |  | Arquitectura del agente, demo de casos de uso, limitaciones y mejoras |
| **Data Governance** |  | Cómo abordaste la gobernanza transversal (ver sección siguiente) |
| **Conclusiones** |  | Top 3 hallazgos de negocio + próximos pasos recomendados |

### Criterios de análisis esperados

- Identifica **al menos 3 insights accionables** para el negocio.
- Propón **al menos 1 métrica nueva** que no se derive directamente de las queries requeridas.
- Usa visualizaciones claras con títulos, ejes y unidades.

---

## Eje Transversal — Data Governance

La gobernanza del dato debe estar **presente en todas las partes** de la prueba:

### Obligatorio en Parte 1
- [ ] Clasificar cada tabla según el nivel de sensibilidad (público / interno / confidencial / restringido)
- [ ] Identificar todas las columnas PII en `clientes.csv` y `eventos.csv`
- [ ] Aplicar **enmascaramiento** de datos PII en los archivos de salida `*_clean.csv` (ej: hash SHA-256 del email, últimos 4 dígitos del teléfono)
- [ ] Respetar el campo `retencion_dias` del diccionario de datos en cualquier transformación que filtre por fechas
- [ ] Documentar el **linaje de datos**: qué tabla/campo viene de dónde en tus transformaciones

### Obligatorio en Parte 2 (Agente)
- [ ] El agente **no debe exponer datos PII** en sus respuestas (emails, teléfonos completos, nombres completos)
- [ ] Implementar un mecanismo de **validación de output** que bloquee respuestas con PII
- [ ] Documentar qué datos tiene acceso el agente y cuáles están restringidos

### Obligatorio en Parte 3 (Presentación)
- [ ] Diapositiva(s) de governance: mostrar el mapa de clasificación de datos, el linaje y las medidas de privacidad implementadas

### Bonus
- [ ] Generar un **catálogo de datos** mínimo (puede ser un Markdown o HTML) con la descripción de cada tabla, columnas, data owner y clasificación
- [ ] Implementar control de acceso por rol simulado en el agente (ej: rol `analista` vs. rol `finanzas` con acceso diferente a columnas de costos)

---

## Estructura recomendada del repositorio

```
prueba_data_engineer/
├── README.md                  # Instrucciones de instalación y ejecución
├── requirements.txt           # Dependencias Python
├── generar_dataset.py         # Script generador (provisto)
├── pipeline.py                # Pipeline principal
├── queries.sql                # Consultas SQL
├── agente.py                  # Agente GenAI (o notebook)
├── test_agente.py             # Casos de prueba del agente
├── data/
│   ├── raw/                   # Datos originales (sin modificar)
│   ├── clean/                 # Datos limpios y enmascarados
│   └── diccionario_datos.csv  # Provisto
├── outputs/
│   ├── reporte_calidad.md
│   ├── log_transformaciones.csv
│   ├── q1_top_productos.csv   # Un CSV por query
│   └── ...
├── notebooks/
│   └── analisis_exploratorio.ipynb
├── presentacion/
│   └── RetailTech_DataEngineer_[TuNombre].pdf
└── governance/
    └── catalogo_datos.md      # Bonus
```

---

## Herramientas gratuitas recomendadas

| Categoría | Herramienta | URL |
|---|---|---|
| Base de datos local | DuckDB | https://duckdb.org |
| Procesamiento de datos | pandas / polars | — |
| Visualización | matplotlib, seaborn, plotly | — |
| LLM local | Ollama (llama3, mistral) | https://ollama.com |
| LLM cloud (free tier) | Google Gemini API | https://aistudio.google.com |
| Agentes | LangChain | https://python.langchain.com |
| Agentes | LlamaIndex | https://www.llamaindex.ai |
| UI | Streamlit / Gradio | https://streamlit.io / https://www.gradio.app |
| Notebooks | JupyterLab | https://jupyter.org |
| Control de versiones | GitHub / GitLab | — |
| Presentación | Google Slides (gratis) | — |

> **Nota sobre IA:** puedes usar asistentes de IA (GitHub Copilot, ChatGPT, Claude, etc.) como apoyo para escribir código o buscar referencias. Sin embargo, **debes poder explicar y defender cada línea de código en la presentación**. Si en la presentación no puedes justificar una decisión técnica, no se asignará puntaje por esa parte.

---

## Criterios de evaluación (resumen)

| Frente | Peso | Detalle |
|---|---|---|
| Técnico | 30% | SQL, Python, pipeline, calidad de datos |
| Analítico | 25% | Profundidad del análisis, insights, métricas |
| Presentación | 20% | Claridad, visualizaciones, storytelling |
| GenAI | 25% | Diseño del agente, tools, robustez, explicabilidad |
| Data Governance | Transversal | Penaliza o bonifica en todos los frentes |

## Entrega

1. Comparte el enlace al repositorio Git.
2. Adjunta el PDF de la presentación.
3. Incluye un `README.md` con instrucciones claras para reproducir el ambiente y ejecutar el pipeline desde cero.

---

*Cualquier duda técnica sobre el dataset o el enunciado, envíala al correo del reclutador. Preguntas sobre herramientas o decisiones de implementación son decisiones tuyas — eso también se evalúa.*
