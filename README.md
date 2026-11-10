# Mini ETL E-commerce

Pipeline de datos de extremo a extremo que lee órdenes, clientes y productos en CSV, valida su calidad, los transforma con **PySpark**, calcula métricas de negocio y las carga en **MySQL** para analizarlas con SQL. La orquestación se define con un DAG de **Airflow**.

El proyecto simula localmente una arquitectura moderna en AWS (S3 + Glue + Redshift), con la misma lógica que se usaría en la nube.

> **Estado:** en desarrollo. Ver el [roadmap](#roadmap).

---

## Caso de negocio

Una tienda de e-commerce recibe periódicamente archivos con sus órdenes de venta. El equipo de análisis necesita datos confiables para responder preguntas como:

- ¿Cuánto ingreso genera cada categoría de producto?
- ¿Quiénes son los mejores clientes y de qué ciudades?
- ¿Cuál es el producto más comprado por cada cliente?
- ¿Cómo evolucionan las ventas acumuladas día a día?

Los archivos llegan con problemas reales (valores vacíos, duplicados, montos inválidos), por lo que el pipeline debe **validar antes de transformar** y separar los registros rechazados indicando el motivo.

### Datos de entrada (`data/raw/`)

| Archivo | Columnas |
|---|---|
| `orders.csv` | order_id, customer_id, product_id, amount, order_date |
| `customers.csv` | customer_id, name, city |
| `products.csv` | product_id, product_name, category |

`orders.csv` contiene errores intencionales para practicar calidad de datos: `customer_id` vacío, `order_id` duplicado y `amount` negativo.

### Datos de salida

| Ruta | Contenido |
|---|---|
| `data/processed/orders_clean/` | Órdenes válidas, enriquecidas con cliente y producto |
| `data/rejected/` | Órdenes que fallaron la validación, con el motivo del rechazo |
| `data/metrics/` | Métricas agregadas (ingresos por categoría, ranking de clientes, etc.) |
| MySQL (`ecommerce_dw`) | Tablas de métricas listas para consultar con SQL |

---

## Arquitectura

```
 data/raw/  (orders, customers, products)        <- zona RAW
        |
        v
 PySpark job (src/etl_orders.py)
   1. Extract        : lectura de CSV
   2. Data Quality   : nulos, duplicados, montos inválidos
   3. Transform      : joins, columnas calculadas (impuesto)
   4. Aggregate      : groupBy + window functions
        |
        +--> data/rejected/                       <- registros inválidos + motivo
        +--> data/processed/ , data/metrics/      <- zona PROCESSED
        |
        v
 Carga a MySQL (src/load_to_mysql.py)             <- data warehouse
        |
        v
 Consultas analíticas (sql/analytics_queries.sql)

 Airflow DAG: extract >> validate >> transform >> load
```

### Equivalencia local → nube

| Este proyecto (local) | Equivalente en AWS |
|---|---|
| `data/raw/`, `data/processed/` (carpetas) | Buckets / prefijos en **S3** |
| `src/etl_orders.py` (PySpark) | **AWS Glue Job** (Spark administrado) |
| MySQL 8 | **Amazon Redshift** (data warehouse) |
| `dags/orders_pipeline_dag.py` | **Airflow** / MWAA |

Para llevarlo a la nube bastaría cambiar las rutas locales por `s3://...` y la carga a MySQL por una carga a Redshift (por ejemplo con `COPY` desde S3).

---

## Stack

| Componente | Versión / nota |
|---|---|
| Python | 3.14 (venv) |
| PySpark | 4.3.0 |
| pandas | < 3 (compatibilidad con PySpark) |
| Java | OpenJDK 17 |
| MySQL | 8.x + MySQL Workbench (Windows) |
| Airflow | Entorno separado (pendiente) |
| Entorno | Windows 11 + WSL2 (Ubuntu) + VS Code |

---

## Estructura del proyecto

```
mini-etl-ecommerce/
├── data/
│   ├── raw/                  # archivos de entrada
│   ├── processed/            # datos limpios y enriquecidos
│   ├── rejected/             # registros rechazados
│   └── metrics/              # métricas agregadas
├── src/
│   ├── etl_orders.py         # job PySpark (extract, validate, transform, aggregate)
│   └── load_to_mysql.py      # carga de métricas a MySQL
├── sql/
│   ├── create_tables.sql
│   └── analytics_queries.sql # joins, GROUP BY, window functions
├── dags/
│   └── orders_pipeline_dag.py
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Reglas de calidad de datos

| Regla | Acción |
|---|---|
| `customer_id` nulo | Rechazar (`reason = "customer_id nulo"`) |
| `order_id` duplicado | Conservar el primero, rechazar el resto |
| `amount <= 0` | Rechazar (`reason = "monto inválido"`) |
| `customer_id` o `product_id` sin coincidencia en su catálogo | Rechazar (`reason = "llave huérfana"`) |
| `order_date` con formato inválido | Rechazar (`reason = "fecha inválida"`) |

Los registros rechazados **no se descartan en silencio**: se guardan en `data/rejected/` con el motivo, para poder auditarlos.

---

## Conceptos que cubre

| Área | Dónde se practica |
|---|---|
| **PySpark** | `SparkSession`, `DataFrame`, `select`, `filter`, `withColumn`, `groupBy`, `join` en `etl_orders.py` |
| **SQL** | `JOIN`, `GROUP BY`, `SUM/COUNT/AVG` en `analytics_queries.sql` |
| **Window functions** | `ROW_NUMBER()` (producto top por cliente), `SUM() OVER` (ventas acumuladas por día) |
| **ETL** | Flujo completo extract → validate → transform → load |
| **S3 / Glue / Redshift** | Simulados con carpetas, PySpark y MySQL (ver equivalencias) |
| **Airflow** | DAG de 4 tareas con dependencias |

---

## Cómo ejecutarlo

### Requisitos

- Windows 11 con WSL2 (Ubuntu)
- OpenJDK 17
- Python 3.12+ con `venv`
- MySQL 8 accesible desde WSL (`networkingMode=mirrored` en `.wslconfig`)

### Instalación

```bash
git clone <url-del-repositorio>
cd mini-etl-ecommerce

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Configuración de MySQL

Copia la plantilla y edítala con tus datos:

```
cp .env.example .env
```

Luego ejecuta `sql/create_tables.sql` en MySQL Workbench.

### Ejecución

```bash
python src/etl_orders.py      # valida, transforma y agrega
python src/load_to_mysql.py   # carga las métricas en MySQL
```

---

## Resultados

_Pendiente: se completará con ejemplos de salidas y consultas._

---

## Roadmap

- [x] Entorno: WSL2, JDK 17, venv, PySpark
- [ ] Conexión WSL → MySQL de Windows
- [ ] Datasets de ejemplo (`orders`, `customers`, `products`)
- [ ] Tablas y consultas en MySQL (joins, GROUP BY, window functions)
- [ ] Job PySpark (calidad, transformaciones, agregaciones)
- [ ] Carga de métricas a MySQL
- [ ] DAG de Airflow
- [ ] Documentar el despliegue equivalente en AWS (S3 + Glue + Redshift)

---

## Decisiones y aprendizajes

- **WSL2 en lugar de Docker:** Spark y Airflow funcionan de forma nativa en Linux, con menos capas que mantener.
- **Proyecto dentro del sistema de archivos de Linux (`~/`):** evita la lentitud de `/mnt/c/`.
- **JDK 17:** versión compatible con Spark 4.x; el JDK más reciente de Windows no se usa para este proyecto.
- **pandas < 3:** PySpark aún no soporta completamente pandas 3.x.
- **MySQL como sustituto de Redshift:** permite practicar SQL analítico (incluidas window functions) sin costo.

_Se irán agregando más conforme avance el proyecto._