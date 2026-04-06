# Pipeline ETL: Optimización y Analytics para E-commerce
Este proyecto despliega un ecosistema **ETL (Extract, Transform, Load)** robusto, diseñado para transformar datos crudos de comercio electrónico en inteligencia de negocio accionable. El sistema integra flujos de ventas con métricas avanzadas de **Supply Chain Management**.

## Objetivo del Proyecto
Desarrollar un pipeline automatizado que consolide 11 fuentes de datos independientes, resuelva inconsistencias operativas y genere KPIs críticos de **logística** y **rentabilidad**.

---

## Stack Tecnológico
* **Lenguaje:** Python 3.10+
* **Procesamiento:** Pandas (DataFrames, Agregaciones complejas, Joins)
* **Almacenamiento Optimizado:** PyArrow (Engine para archivos Parquet)
* **Gestión de Entorno:** Glob & OS (Automatización de rutas y auditoría de archivos)

---

## Arquitectura del Pipeline

El script `etl.py` implementa una arquitectura de procesamiento en 6 etapas secuenciales:

1. **Extracción (Extract):** Consolidación automatizada de 11 datasets JSON (Órdenes, Clientes, Inventario, Depósitos, etc.) mediante patrones de búsqueda global.
2. **Tratamiento de Datos Críticos (Cleaning):** Eliminación de registros sin `customer_id` o `total_amount` e imputación inteligente de campos opcionales (`notes`, `promotion_id`).
3. **Gestión de Ciclo de Vida (Deduplicación):** Lógica de **"Último Registro Válido"** para conservar únicamente el estado más reciente de cada transacción tras ordenamiento cronológico.
4. **Optimización de Memoria (Transform):** Conversión de objetos a tipos `category`, reduciendo significativamente el consumo de memoria RAM.
5. **Lógica de Negocio (Analytics):** Cálculo del **Top 5 de clientes (Gasto Acumulado)**, identificación del **producto más vendido**, tendencias de **facturación mensual**, **% de ocupación por depósito** e **índice de rotación de stock**.
6. **Carga Eficiente (Load):** Persistencia en **CSV** (para consumo humano) y **Parquet** (formato columnar optimizado para ingeniería de datos).

---

## Estructura del Proyecto

```text
├── data/                # Carpeta con los 11 archivos JSON fuente
├── output/              # Resultados del ETL (CSV y Parquet generados)
├── etl.py               # Script principal del pipeline
├── README.md            # Documentación del proyecto
└── requirements.txt     # Listado de librerías necesarias
```

# Guía de Instalación y Ejecución

Sigue estos pasos detallados para configurar el proyecto desde cero:

### 1. Inicializar el Repositorio
Si estás creando el proyecto por primera vez o quieres empezar una versión limpia:

```bash
mkdir ecommerce-etl-pipeline
cd ecommerce-etl-pipeline
git init
```

### 2. Configurar el Entorno Virtual
Es fundamental para mantener las dependencias aisladas y evitar conflictos con otras librerías del sistema.

**En Windows:**
```bash
python -m venv venv
.\venv\Scripts\activate
```
**En Mac/Linux:**
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Instalar Dependencias
Instala las librerías necesarias para el procesamiento, transformación y optimización de datos:

```bash
pip install pandas pyarrow
```

### 4. Ejecutar el Pipeline
Asegúrate de tener los archivos fuente (`.json`) dentro de la carpeta `data/` en la raíz del proyecto y ejecuta el script:

```bash
python etl.py
```

## Auditoría de Eficiencia
El sistema incluye un módulo de auditoría de almacenamiento que compara formatos de salida. En este flujo, el formato **Parquet** demostró ser significativamente más eficiente en espacio y velocidad de lectura que el **CSV** tradicional, optimizando costos potenciales de infraestructura de datos en entornos de producción.

---

## Autor
**Matías Olmos** *Ingeniero Industrial | Data Engineer*
