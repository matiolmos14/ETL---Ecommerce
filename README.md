# 🚀 Pipeline ETL: Optimización y Analytics para E-commerce Multicanal

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![Pandas](https://img.shields.io/badge/Library-Pandas-150458.svg)](https://pandas.pydata.org/)
[![Data-Engineering](https://img.shields.io/badge/Domain-Data%20Engineering-green.svg)]()
[![Supply-Chain](https://img.shields.io/badge/Domain-Supply%20Chain-orange.svg)]()

Este proyecto despliega un ecosistema **ETL (Extract, Transform, Load)** robusto, diseñado para transformar datos crudos de comercio electrónico en inteligencia de negocio accionable. El sistema integra flujos de ventas con métricas avanzadas de **Supply Chain Management**.

## 🎯 Objetivo del Proyecto
Desarrollar un pipeline automatizado que consolide 11 fuentes de datos independientes, resuelva inconsistencias operativas y genere KPIs críticos de **logística** y **rentabilidad**.

---

## 🛠️ Stack Tecnológico
* **Lenguaje:** Python 3.10+
* **Procesamiento:** Pandas (DataFrames, Agregaciones complejas, Joins)
* **Almacenamiento Optimizado:** PyArrow (Engine para archivos Parquet)
* **Gestión de Entorno:** Glob & OS (Automatización de rutas y auditoría de archivos)

---

## ⚙️ Arquitectura del Pipeline

El script `etl.py` implementa una arquitectura de procesamiento en 6 etapas secuenciales:

1. **Extracción (Extract):** Consolidación automatizada de 11 datasets JSON (Órdenes, Clientes, Inventario, Depósitos, etc.) mediante patrones de búsqueda global.
2. **Tratamiento de Datos Críticos (Cleaning):** Eliminación de registros sin `customer_id` o `total_amount` e imputación inteligente de campos opcionales (`notes`, `promotion_id`).
3. **Gestión de Ciclo de Vida (Deduplicación):** Lógica de **"Último Registro Válido"** para conservar únicamente el estado más reciente de cada transacción tras ordenamiento cronológico.
4. **Optimización de Memoria (Transform):** Conversión de objetos a tipos `category`, reduciendo significativamente el consumo de memoria RAM.
5. **Lógica de Negocio (Analytics):** Cálculo de **LTV (Lifetime Value)**, tendencias temporales, **% de ocupación de depósitos** e **índice de rotación de stock**.
6. **Carga Eficiente (Load):** Persistencia en **CSV** (para consumo humano) y **Parquet** (formato columnar optimizado para ingeniería de datos).

---

## 📂 Estructura del Proyecto
```text
├── data/                # Carpeta con los 11 archivos JSON fuente
├── output/              # Resultados del ETL (CSV y Parquet generados)
├── etl.py               # Script principal del pipeline
├── README.md            # Documentación del proyecto
└── requirements.txt     # Listado de librerías necesarias

