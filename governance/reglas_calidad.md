# 📑 Documentación: Motor de Reglas de Calidad (Data Cleaning Pipeline)

Este documento detalla las **8 reglas de validación** aplicadas en la capa de transformación (Silver Layer) del pipeline. El objetivo es garantizar que solo los datos que cumplen con los estándares de negocio lleguen a la base de datos analítica.



## 1. Resumen de Reglas y Lógica Técnica

| ID | Regla | Nivel | Campo(s) Evaluados            | Lógica de Validación |
|:---|:---|:---|:------------------------------|:---|
| **R1** | **Unicidad** | Crítico | Primary Key (PK)              | Eliminación de registros duplicados basados en el identificador único de la tabla. |
| **R2** | **Nulos Críticos** | Crítico | IDs, Fechas, Precios          | Filtrado de filas con valores `NaN` o `None` en columnas esenciales para el negocio. |
| **R3** | **Formato Email** | Calidad | `email`, `data owner`         | Validación mediante expresión regular (Regex) para asegurar correos electrónicos reales. |
| **R4** | **Integridad Financiera**| Negocio | `precio`, `costo`, `total`    | Validación de valores numéricos estrictamente mayores a cero (> 0). |
| **R5** | **Coherencia Temporal** | Negocio | `fecha_pedido`, `entrega`     | Validación de fechas no futuras y secuencia lógica (Pedido <= Entrega). |
| **R6** | **Validación de Catálogo**| Negocio | `estado`, `categoria`, `pais` | Comparación contra listas maestras de valores permitidos (Enums). |
| **R7** | **Estructura de ID** | Técnico | Primary Keys (IDs)            | Validación de patrón regex `^[A-Z]{3,4}-\d+$` (prefijo-número). |
| **R8** | **Protección PII** | Seguridad | `nombre`, `telefono`, `email` | Anonimización de datos sensibles usando el algoritmo SHA-256. |

---

## 2. Descripción Detallada de Transformaciones

### R1 - Gestión de Duplicados
Se garantiza la integridad referencial eliminando duplicados a nivel de registro y de llave primaria. 
* **Acción:** Se conserva el primer registro encontrado (`keep='first'`) y los duplicados se envían a cuarentena.

### R2 - Limpieza de Nulos
Identifica huecos de información en columnas parametrizadas en `config_nulos`.
* **Acción:** Si un campo crítico es nulo, el registro completo es rechazado para evitar errores en cálculos agregados.

### R3 - Validación de Emails
Aplica un filtro Regex dinámico para detectar estructuras de correo inválidas.
* **Regex utilizado:** `^[\w\.-]+@[\w\.-]+\.\w{2,4}$`
* **Propósito:** Garantizar la contactabilidad del cliente.



### R4 - Integridad Monetaria
Valida que las columnas de precios y costos no contengan ceros, valores negativos o errores de tipo (N/A).
* **Impacto:** Evita distorsiones en el cálculo de márgenes y KPIs de venta.

### R5 - Validación de Fechas
Convierte strings a objetos `datetime` y realiza dos comprobaciones:
1.  **Fecha vs Hoy:** Ningún evento puede haber ocurrido en el futuro.
2.  **Relatividad:** La fecha de entrega no puede ser menor a la de creación del pedido.

### R6 - Normalización de Categorías
Asegura que los campos categóricos coincidan con el catálogo de Meli.
* **Normalización:** Se aplica `.lower().strip()` para que diferencias de mayúsculas o espacios no generen errores de validación.

### R7 - Formato Estructural de IDs
Valida que las PKs sigan el estándar corporativo.
* **Ejemplos válidos:** `CLI-001`, `PROD-9987`, `PED-102`.
* **Propósito:** Mantener la consistencia visual y técnica en toda la base de datos.

### R8 - Anonimización PII (Seguridad)
Cumplimiento de normativas de protección de datos personales.
* **Técnica:** Hashing SHA-256. 
* **Nota:** Los datos personales se vuelven irreconocibles pero mantienen su consistencia para análisis de "unicidad" sin comprometer la identidad del usuario.

### R9 - Imputación de Inventario (Operaciones)
Asegura la continuidad de los cálculos de stock y evita falsos negativos en alertas de reabastecimiento.
* **Técnica:** Imputación por valor constante (fillna(0)).
* **Alcance:** Exclusivo para la entidad PRODUCTOS sobre la columna stock_disponible.
* **Nota:** Los valores nulos detectados se asumen como "Sin Existencias" (0). Esto protege las consultas SQL de alertas críticas, garantizando que un dato ausente sea tratado como una prioridad de compra y no como un error de sistema.

---

## 3. Flujo de Errores (Quarantine)

Cuando un registro falla en cualquiera de las reglas de la **R1 a la R7**, el pipeline realiza lo siguiente:
1.  Extrae el registro fallido del flujo principal.
2.  Agrega la columna `motivo_error` con la descripción del hallazgo.
3.  Lo guarda en `./data/quarantine/{timestamp}/{tabla}_{regla}.csv` para su posterior revisión y limpieza manual.
