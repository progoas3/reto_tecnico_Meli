# 📑 Documentación: Motor de Reglas de Calidad y Data Governance (Meli RetailTech)

Este documento detalla el funcionamiento del pipeline de datos encargado de la transformación, limpieza y cumplimiento normativo de la capa **Silver**. El sistema garantiza datos aptos para análisis, asegura la protección de datos personales (PII) y el cumplimiento estricto de políticas de retención de datos.

---

## 1. Resumen de Reglas de Calidad (Data Cleaning)

| ID | Regla | Nivel | Campo(s) Evaluados | Lógica de Validación |
|:---|:---|:---|:---|:---|
| **R1** | **Unicidad** | Crítico | Primary Key (PK) | Eliminación de registros con IDs duplicados (`keep='first'`). |
| **R2** | **Nulos Críticos** | Crítico | IDs, Fechas, Totales | Filtrado de filas con valores ausentes en columnas mandatorias. |
| **R3** | **Formato Email** | Calidad | `email`, `data_owner` | Validación mediante Regex: `^[\w\.-]+@[\w\.-]+\.\w{2,4}$`. |
| **R4** | **Integridad Financiera**| Negocio | Precios, Costos, Totales | Validación de valores numéricos estrictamente mayores a cero. |
| **R5** | **Coherencia Temporal** | Negocio | Fechas Pedido/Entrega | Validación de fechas no futuras y secuencia lógica (Pedido <= Entrega). |
| **R6** | **Validación de Catálogo**| Negocio | Categoría, País, Estado | Comparación contra listas maestras (Enums) normalizadas. |
| **R7** | **Estructura de ID** | Técnico | Primary Keys (IDs) | Validación de patrón regex: `^[A-Z]{3,4}-\d+$`. |
| **R8** | **Protección PII (Hash)** | Seguridad | `nombre`, `email`, `apellido`| Anonimización irreversible mediante algoritmo **SHA-256**. |
| **R9** | **Imputación de Stock** | Operativo | `stock_disponible` | Sustitución de nulos por `0` (asunción de ruptura de stock). |
| **R10** | **Masking Telefónico** | Seguridad | `telefono` | Estandarización de prefijos y ocultamiento de dígitos centrales. |

---

## 2. Descripción Detallada de Transformaciones

### R8 - Anonimización PII (Hashing)
Para cumplir con la privacidad, los datos de contacto directo se transforman en una firma digital única.
* **Técnica:** Los campos se convierten a minúsculas, se eliminan espacios y se aplica **SHA-256**.
* **Propósito:** Permite realizar cruces de tablas (JOINS) y análisis de recurrencia sin exponer la identidad real del usuario.

### R9 - Imputación de Inventario (Operaciones)
Garantiza la continuidad de los cálculos de stock y evita falsos negativos en alertas de reabastecimiento.
* **Técnica:** Imputación por valor constante (`fillna(0)`).
* **Alcance:** Exclusivo para la entidad **PRODUCTOS**. Los valores nulos se asumen como "Sin Existencias" para proteger las consultas de stock crítico.

### R10 - Enmascaramiento de Teléfono (Masking)
Se implementó una lógica de protección visual para cumplir con estándares de seguridad:
* **Entrada:** `+57 311 800 7546`
* **Salida:** `+57*******7546`
* **Lógica:** Se preserva el código de país y los últimos 4 dígitos, ocultando el cuerpo del número con asteriscos.



---

## 3. Capa de Data Governance (Metadata-Driven)

El pipeline es **"Metadata-Driven"**, lo que significa que su comportamiento se ajusta automáticamente según las definiciones de los archivos de catálogo.

### Política de Retención de Datos (Purga Automática)
El motor de gobernanza calcula la vigencia de cada registro basándose en el campo `retencion_dias` definido en el catálogo para cada tabla.
* **Lógica:** Se calcula la diferencia entre la fecha del registro y la fecha actual. Si supera el límite de retención, el registro es extraído del flujo productivo y enviado a un log de auditoría.
* **Caso de Uso:** En la tabla de **Eventos**, los registros que superan los 365 días son purgados automáticamente para optimizar almacenamiento y cumplir con marcos legales.

### Clasificación de Sensibilidad
Cada entidad hereda un nivel de riesgo: **PÚBLICO, INTERNO, CONFIDENCIAL o RESTRINGIDO**.
* Esta clasificación actúa como un control de acceso para el Agente GenAI (Parte 2), impidiendo la visualización de datos en tablas marcadas como sensibles sin la debida anonimización.



---

## 4. Linaje de Datos (Data Lineage)

| Entidad Final | Fuente Original | Transformación Principal | Clasificación |
|:---|:---|:---|:---|
| **tbl_clientes** | `clientes.csv` | Hash SHA-256 (Email), Masking (Tel), Estandarización de País. | **Confidencial** |
| **tbl_pedidos** | `pedidos.csv` | Validación de Totales (>0), Coherencia de Fechas (Pedido/Entrega). | **Interno** |
| **tbl_eventos** | `eventos.csv` | Filtro de Retención Dinámico (Purga Automática), Limpieza de Dispositivo. | **Público** |
| **tbl_productos**| `productos.csv`| Imputación de Stock (Nulo -> 0), Validación de Categorías Meli. | **Interno** |

---

## 5. Gestión de Errores y Cuarentena (Auditoría)

Cualquier dato que no supere los controles de calidad (**R1 a R7**) es desviado para no contaminar la base de datos analítica:
1. **Cuarentena:** Los registros fallidos se almacenan en `./data/quarantine/` categorizados por tabla y timestamp.
2. **Trazabilidad:** Se añade la columna `motivo_error` que describe la regla exacta que falló, permitiendo auditorías de calidad de datos en origen.
3. **Reporte Automático:** Al finalizar el pipeline, se genera un informe consolidado detallando la salud de los datos (registros procesados vs. rechazados).