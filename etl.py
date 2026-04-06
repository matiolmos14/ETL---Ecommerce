import pandas as pd
import glob
import os

# 1. VERIFICACIÓN DE ARCHIVOS
# Usamos glob para identificar si los archivos fuente existen antes de iniciar el proceso
archivos = glob.glob('data/ecommerce_*.json')

if not archivos:
    print("ERROR: No se encontraron los archivos en la carpeta data/")
else:
    print(f"Archivos encontrados: {len(archivos)}")

    try:
        # 2. CARGA DE DATOS (EXTRACT)
        # Importación de los 11 datasets principales del e-commerce para su procesamiento
        df_orders      = pd.read_json('data/ecommerce_orders.json')
        df_order_items = pd.read_json('data/ecommerce_order_items.json')
        df_customers   = pd.read_json('data/ecommerce_customers.json')
        df_products    = pd.read_json('data/ecommerce_products.json')
        df_brands      = pd.read_json('data/ecommerce_brands.json')
        df_categories  = pd.read_json('data/ecommerce_categories.json')
        df_inventory   = pd.read_json('data/ecommerce_inventory.json')
        df_promotions  = pd.read_json('data/ecommerce_promotions.json')
        df_reviews     = pd.read_json('data/ecommerce_reviews.json')
        df_suppliers   = pd.read_json('data/ecommerce_suppliers.json')
        df_warehouses  = pd.read_json('data/ecommerce_warehouses.json')

        # 3. ETAPA I: ESTRATEGIAS DE ELIMINACIÓN DE NULOS
        print("\n--- ETAPA I: TRATAMIENTO DE VALORES NULOS ---")
        # Registramos el conteo inicial para auditoría de calidad de datos
        antes_nulos = len(df_orders)

        # Eliminamos filas donde el ID de cliente o el monto total son nulos,
        # ya que sin estos datos la transacción no es útil para el análisis de ventas.
        df_orders = df_orders.dropna(subset=['customer_id', 'total_amount'])
        print(f"-> Filas eliminadas por datos críticos faltantes: {antes_nulos - len(df_orders)}")

        # Imputación de valores: Rellenamos vacíos con etiquetas descriptivas para evitar errores en joins
        # Convertimos a string para asegurar compatibilidad total con el formato Parquet
        df_orders["promotion_id"] = df_orders["promotion_id"].fillna("Sin código").astype(str)
        df_orders["notes"] = df_orders["notes"].fillna("Sin notas")
        
        # Si no hay descuento, asumimos 0 para poder realizar cálculos matemáticos después
        if 'discount_percent' in df_orders.columns:
            df_orders['discount_percent'] = df_orders['discount_percent'].fillna(0)

        # 4. ETAPA II: GESTIÓN DE DUPLICADOS
        print("\n--- ETAPA II: AUDITORÍA Y ELIMINACIÓN DE DUPLICADOS ---")
        # Convertimos a fecha para poder ordenar cronológicamente
        df_orders['order_date'] = pd.to_datetime(df_orders['order_date'])

        # Estrategia: Ordenamos por fecha y nos quedamos con el último registro de cada order_id
        # Esto asegura que si una orden fue actualizada, mantengamos el estado más reciente.
        df_orders = df_orders.sort_values('order_date').drop_duplicates(subset=['order_id'], keep='last')

        # MEJORA DE NEGOCIO: Filtrado de órdenes canceladas
        # Para reportes de gasto y ventas, solo consideramos transacciones reales
        df_ventas_validas = df_orders[df_orders['status'] != 'cancelado'].copy()

        # 5. ETAPA III: OPTIMIZACIÓN DE TIPOS DE DATOS
        print("\n--- ETAPA III: CORRECCIÓN DE TIPOS DE DATOS ---")
        # Aseguramos que los montos sean numéricos para cálculos de reportes
        df_orders['total_amount'] = pd.to_numeric(df_orders['total_amount'], errors='coerce')
        
        # Optimización de memoria: Convertimos columnas repetitivas a tipo 'category'
        # Esto reduce drásticamente el peso del DataFrame en RAM
        for col in ["status", "payment_method", "shipping_method"]:
            if col in df_orders.columns:
                df_orders[col] = df_orders[col].astype("category")

        # 6. ETAPA IV: CÁLCULO DE MÉTRICAS
        print("\n--- ETAPA IV: CÁLCULO DE MÉTRICAS CLAVE ---")
        
        # Reporte 1: TOP 5 Clientes con mayor gasto
        # Se utiliza df_ventas_validas para no contar dinero de órdenes canceladas
        reporte_clientes = pd.merge(df_ventas_validas, df_customers, on="customer_id", how="left") \
                            .groupby(["customer_id", "first_name", "last_name"])["total_amount"].sum() \
                            .sort_values(ascending=False).head(5).reset_index()

        # Reporte 2: Producto más vendido (cantidad)
        # Se agrega .head(1) para responder específicamente cuál es el producto mas vendido
        df_ventas_full = pd.merge(df_order_items, df_products, on="product_id", how="left")
        reporte_ventas = df_ventas_full.groupby("product_name")["quantity"].sum() \
                            .sort_values(ascending=False).head(1).reset_index()

        # Reporte 3: Evolución de ventas (mensual)
        # Se utiliza df_ventas_validas y reset_index para un formato de tabla optimizado
        df_orders_aux = df_ventas_validas.copy()
        df_orders_aux["mes"] = df_orders_aux["order_date"].dt.to_period("M").astype(str)
        reporte_mensual = df_orders_aux.groupby("mes")["total_amount"].sum().reset_index()

        # Reporte 4: Análisis de Logística (Ocupación de Depósitos)
        # Cruzamos inventario con depósitos para analizar capacidad vs ocupación real
        df_stock_full = pd.merge(df_inventory, df_warehouses, on="warehouse_id", how="left")
        reporte_logistica = df_stock_full.groupby("warehouse_name").agg({
            'quantity': 'sum',
            'capacity_units': 'first',
            'location': 'first'
        })
        reporte_logistica['%_ocupacion'] = (reporte_logistica['quantity'] / reporte_logistica['capacity_units']) * 100

        # Reporte 5: Rotación de Inventario por Depósito (KPI Logístico)
        # Calculamos qué tan rápido rota el stock comparando ventas vs inventario actual
        ventas_totales_prod = df_order_items.groupby("product_id")["quantity"].sum().reset_index()
        df_rotacion = pd.merge(df_inventory, ventas_totales_prod, on="product_id", how="left").fillna(0)
        df_rotacion = pd.merge(df_rotacion, df_warehouses, on="warehouse_id", how="left")

        reporte_rotacion = df_rotacion.groupby("warehouse_name").agg({
            'quantity_y': 'sum', # Total vendido
            'quantity_x': 'sum', # Stock actual
            'location': 'first'
        }).rename(columns={'quantity_y': 'total_vendido', 'quantity_x': 'stock_actual'})

        # El índice de rotación indica cuántas veces se renueva el stock físicamente
        reporte_rotacion['indice_rotacion'] = (reporte_rotacion['total_vendido'] / reporte_rotacion['stock_actual']).round(2)

        # 7. ETAPA V: ALMACENAMIENTO DE RESULTADOS
        print("\n--- ETAPA V: LOAD - GUARDANDO RESULTADOS ---")
        output_dir = 'output'
        os.makedirs(output_dir, exist_ok=True)

        # Definimos rutas para poder comparar de eficiencia en el almacenamiento
        csv_path = f'{output_dir}/orders_clean.csv'
        parquet_path = f'{output_dir}/orders_clean.parquet'

        # Guardamos los datos en formato csv
        df_orders.to_csv(csv_path, index=False)
        df_orders.to_parquet(parquet_path, index=False)

        # Exportamos de reportes para el área de BI 
        reporte_clientes.to_csv(f'{output_dir}/reporte_clientes.csv', index=False)
        reporte_ventas.to_csv(f'{output_dir}/reporte_producto_mas_vendido.csv', index=False)
        reporte_mensual.to_csv(f'{output_dir}/reporte_ventas_mensual.csv', index=False)
        reporte_logistica.to_csv(f'{output_dir}/reporte_logistica_depositos.csv', index=True)
        reporte_rotacion.to_csv(f'{output_dir}/reporte_rotacion_inventario.csv', index=True)

        # 8. ETAPA VI: AUDITORÍA DE EFICIENCIA
        print("\n--- ETAPA VI: ANÁLISIS DE ALMACENAMIENTO ---")
        # Calculamos el tamaño de los archivos finales en Kilobytes
        size_csv = os.path.getsize(csv_path) / 1024
        size_parquet = os.path.getsize(parquet_path) / 1024

        print(f"Tamaño del dataset en CSV:     {size_csv:.2f} KB")
        print(f"Tamaño del dataset en Parquet: {size_parquet:.2f} KB")

        # Calculamos el ratio de compresión para demostrar optimización de uso de memoria
        if size_parquet > 0:
            ahorro = (size_csv / size_parquet)
            print(f"Resultado: Parquet es {ahorro:.1f}x más eficiente en espacio.")

        print("\n" + "="*40)
        print("PIPELINE ETL COMPLETADO EXITOSAMENTE")
        print(f"Registros finales procesados: {len(df_orders)}")
        print("="*40)

    except Exception as e:
        print(f"Error crítico detectado durante el proceso: {e}")