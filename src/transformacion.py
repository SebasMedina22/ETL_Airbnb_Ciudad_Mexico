# -*- coding: utf-8 -*-
"""
----------------------------------------------------------
    Modulo: transformacion.py
    Descripcion: Clase Transformacion para procesar datos de Airbnb
                 implementando limpieza y preparacion de datos
                 para el proceso ETL.
                 
    Tareas implementadas:
    1. Limpieza de valores nulos y duplicados
    2. Normalizacion de precios (quitar $, comas)
    3. Conversion de fechas a formato ISO
    4. Derivacion de variables (mes, año, trimestre, dia)
    5. Categorizacion de precios por rangos
    6. Expansion de campos anidados (amenities)
    7. Generacion de DataFrame limpio
----------------------------------------------------------
"""
import pandas as pd
import numpy as np
from datetime import datetime
import re
import sys
from logs import Logs

# Configurar encoding UTF-8 para Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')


class Transformacion:
    """
    Clase para aplicar transformaciones a los datos de Airbnb.
    Implementa todas las tareas minimas requeridas en el Punto 3 del taller.
    """
    
    def __init__(self, usar_logs=True):
        """
        Inicializa la clase de transformacion.
        
        Args:
            usar_logs (bool): Si True, registra todas las operaciones en logs
        """
        self.usar_logs = usar_logs
        if self.usar_logs:
            self.logs = Logs(nombre_proceso='TRANSFORMACION')
            self.logs.info("Inicializando modulo de Transformacion de Airbnb")
    
    def _log(self, mensaje, nivel='info'):
        """
        Metodo privado para registrar mensajes en logs.
        
        Args:
            mensaje (str): Mensaje a registrar
            nivel (str): Nivel del mensaje ('info', 'warning', 'error')
        """
        if self.usar_logs:
        if nivel == 'info':
                self.logs.info(mensaje)
            elif nivel == 'warning':
                self.logs.warning(mensaje)
        elif nivel == 'error':
                self.logs.error(mensaje)
    
    # ============================================================
    # TAREA 1: LIMPIEZA DE VALORES NULOS Y DUPLICADOS
    # ============================================================
    
    def eliminar_duplicados(self, df, columna_id='id'):
        """
        Elimina registros duplicados basandose en una columna ID.
        
        Args:
            df (DataFrame): DataFrame a limpiar
            columna_id (str): Nombre de la columna con ID unico
            
        Returns:
            DataFrame: DataFrame sin duplicados
        """
        registros_antes = len(df)
        self._log(f"Iniciando eliminacion de duplicados basado en columna '{columna_id}'")
        
        # Eliminar duplicados
        df_limpio = df.drop_duplicates(subset=[columna_id], keep='first')
        
        registros_despues = len(df_limpio)
        duplicados_eliminados = registros_antes - registros_despues
        
        if self.usar_logs:
            self.logs.registrar_transformacion(
                nombre_transformacion="Eliminacion de duplicados",
                registros_antes=registros_antes,
                registros_despues=registros_despues,
                detalles=f"{duplicados_eliminados} registros duplicados eliminados"
            )
        
        return df_limpio
    
    def manejar_valores_nulos(self, df, estrategia='eliminar', columnas_criticas=None):
        """
        Maneja valores nulos en el DataFrame segun estrategia definida.
        
        Args:
            df (DataFrame): DataFrame a limpiar
            estrategia (str): 'eliminar' o 'imputar'
            columnas_criticas (list): Lista de columnas que no pueden tener nulos
            
        Returns:
            DataFrame: DataFrame con nulos manejados
        """
        registros_antes = len(df)
        self._log(f"Iniciando manejo de valores nulos - Estrategia: {estrategia}")
        
        # Registrar cantidad de nulos por columna
        nulos_por_columna = df.isnull().sum()
        columnas_con_nulos = nulos_por_columna[nulos_por_columna > 0]
        
        if len(columnas_con_nulos) > 0:
            self._log(f"Columnas con valores nulos detectadas: {len(columnas_con_nulos)}")
            for columna, cantidad in columnas_con_nulos.items():
                porcentaje = (cantidad / registros_antes) * 100
                self._log(f"  - {columna}: {cantidad} nulos ({porcentaje:.2f}%)")
        
        # Aplicar estrategia
        if estrategia == 'eliminar':
            if columnas_criticas:
                # Eliminar filas con nulos solo en columnas criticas
                df_limpio = df.dropna(subset=columnas_criticas)
                self._log(f"Eliminando filas con nulos en columnas criticas: {columnas_criticas}")
            else:
                # Eliminar todas las filas con algun nulo
                df_limpio = df.dropna()
                self._log("Eliminando todas las filas con valores nulos")
        
        elif estrategia == 'imputar':
            df_limpio = df.copy()
            # Imputar numericas con mediana, categoricas con moda
            for columna in df_limpio.columns:
                if df_limpio[columna].isnull().any():
                    if df_limpio[columna].dtype in ['int64', 'float64']:
                        mediana = df_limpio[columna].median()
                        df_limpio[columna].fillna(mediana, inplace=True)
                        self._log(f"  - Imputando {columna} con mediana: {mediana}")
                    else:
                        moda = df_limpio[columna].mode()[0] if not df_limpio[columna].mode().empty else 'Desconocido'
                        df_limpio[columna].fillna(moda, inplace=True)
                        self._log(f"  - Imputando {columna} con moda: {moda}")
        
        registros_despues = len(df_limpio)
        
        if self.usar_logs:
            self.logs.registrar_transformacion(
                nombre_transformacion="Manejo de valores nulos",
                registros_antes=registros_antes,
                registros_despues=registros_despues,
                detalles=f"Estrategia: {estrategia}"
            )
        
        return df_limpio
    
    # ============================================================
    # TAREA 2: NORMALIZACION DE PRECIOS
    # ============================================================
    
    def normalizar_precios(self, df, columna_precio='price'):
        """
        Normaliza precios eliminando simbolos ($, comas) y convirtiendo a float.
        
        Args:
            df (DataFrame): DataFrame con columna de precios
            columna_precio (str): Nombre de la columna de precio
            
        Returns:
            DataFrame: DataFrame con precios normalizados
        """
        self._log(f"Iniciando normalizacion de precios en columna '{columna_precio}'")
        df_limpio = df.copy()
        
        if columna_precio not in df_limpio.columns:
            self._log(f"Columna '{columna_precio}' no encontrada", nivel='warning')
            return df_limpio
        
        # Contar valores originales
        valores_originales = df_limpio[columna_precio].nunique()
        
        # Convertir a string primero para procesamiento
        df_limpio[columna_precio] = df_limpio[columna_precio].astype(str)
        
        # Eliminar simbolos $ y comas
        df_limpio[columna_precio] = df_limpio[columna_precio].str.replace('$', '', regex=False)
        df_limpio[columna_precio] = df_limpio[columna_precio].str.replace(',', '', regex=False)
        df_limpio[columna_precio] = df_limpio[columna_precio].str.strip()
        
        # Convertir a float, manejar errores
        df_limpio[columna_precio] = pd.to_numeric(df_limpio[columna_precio], errors='coerce')
        
        # Estadisticas
        nulos_generados = df_limpio[columna_precio].isnull().sum()
        precio_min = df_limpio[columna_precio].min()
        precio_max = df_limpio[columna_precio].max()
        precio_promedio = df_limpio[columna_precio].mean()
        
        self._log(f"Normalizacion de precios completada:")
        self._log(f"  - Valores unicos originales: {valores_originales}")
        self._log(f"  - Valores convertidos a NaN: {nulos_generados}")
        self._log(f"  - Precio minimo: ${precio_min:.2f}")
        self._log(f"  - Precio maximo: ${precio_max:.2f}")
        self._log(f"  - Precio promedio: ${precio_promedio:.2f}")
        
        return df_limpio
    
    # ============================================================
    # TAREA 3: CONVERSION DE FECHAS A FORMATO ISO
    # ============================================================
    
    def convertir_fechas_iso(self, df, columnas_fecha=None):
        """
        Convierte columnas de fecha a formato ISO (YYYY-MM-DD).
        
        Args:
            df (DataFrame): DataFrame con columnas de fecha
            columnas_fecha (list): Lista de nombres de columnas con fechas
            
        Returns:
            DataFrame: DataFrame con fechas en formato ISO
        """
        self._log("Iniciando conversion de fechas a formato ISO (YYYY-MM-DD)")
        df_limpio = df.copy()
        
        # Si no se especifican columnas, buscar automaticamente
        if columnas_fecha is None:
            columnas_fecha = []
            for col in df_limpio.columns:
                if 'date' in col.lower() or 'fecha' in col.lower():
                    columnas_fecha.append(col)
        
        self._log(f"Columnas de fecha detectadas: {columnas_fecha}")
        
        for columna in columnas_fecha:
            if columna in df_limpio.columns:
                try:
                    # Convertir a datetime
                    df_limpio[columna] = pd.to_datetime(df_limpio[columna], errors='coerce')
                    
                    # Contar nulos generados
                    nulos = df_limpio[columna].isnull().sum()
                    
                    self._log(f"  - Columna '{columna}' convertida a formato ISO")
                    if nulos > 0:
                        self._log(f"    ⚠ {nulos} fechas no pudieron ser convertidas (NaT)", nivel='warning')
                
        except Exception as e:
                    self._log(f"    ❌ Error al convertir columna '{columna}': {str(e)}", nivel='error')
        
        return df_limpio
    
    # ============================================================
    # TAREA 4: DERIVACION DE VARIABLES DESDE FECHAS
    # ============================================================
    
    def derivar_variables_fecha(self, df, columna_fecha='date', prefijo=''):
        """
        Deriva variables temporales desde una columna de fecha:
        - Año (year)
        - Mes (month)
        - Dia (day)
        - Trimestre (quarter)
        - Dia de la semana (day_of_week)
        
        Args:
            df (DataFrame): DataFrame con columna de fecha
            columna_fecha (str): Nombre de la columna de fecha
            prefijo (str): Prefijo para las nuevas columnas
            
        Returns:
            DataFrame: DataFrame con variables derivadas
        """
        self._log(f"Iniciando derivacion de variables desde columna '{columna_fecha}'")
        df_limpio = df.copy()
        
        if columna_fecha not in df_limpio.columns:
            self._log(f"Columna '{columna_fecha}' no encontrada", nivel='warning')
            return df_limpio
        
        # Asegurar que la columna es datetime
        if df_limpio[columna_fecha].dtype != 'datetime64[ns]':
            df_limpio[columna_fecha] = pd.to_datetime(df_limpio[columna_fecha], errors='coerce')
        
        # Derivar variables
        nombre_year = f"{prefijo}year" if prefijo else "year"
        nombre_month = f"{prefijo}month" if prefijo else "month"
        nombre_day = f"{prefijo}day" if prefijo else "day"
        nombre_quarter = f"{prefijo}quarter" if prefijo else "quarter"
        nombre_day_of_week = f"{prefijo}day_of_week" if prefijo else "day_of_week"
        
        df_limpio[nombre_year] = df_limpio[columna_fecha].dt.year
        df_limpio[nombre_month] = df_limpio[columna_fecha].dt.month
        df_limpio[nombre_day] = df_limpio[columna_fecha].dt.day
        df_limpio[nombre_quarter] = df_limpio[columna_fecha].dt.quarter
        df_limpio[nombre_day_of_week] = df_limpio[columna_fecha].dt.dayofweek  # 0=Lunes, 6=Domingo
        
        variables_creadas = [nombre_year, nombre_month, nombre_day, nombre_quarter, nombre_day_of_week]
        
        self._log(f"Variables temporales creadas: {variables_creadas}")
        self._log(f"  - Rango de años: {df_limpio[nombre_year].min():.0f} - {df_limpio[nombre_year].max():.0f}")
        self._log(f"  - Meses unicos: {df_limpio[nombre_month].nunique()}")
        
        return df_limpio
    
    # ============================================================
    # TAREA 5: CATEGORIZACION DE PRECIOS
    # ============================================================
    
    def categorizar_precios(self, df, columna_precio='price', columna_nueva='price_category'):
        """
        Categoriza precios en rangos: Bajo, Medio, Alto, Premium.
        
        Rangos:
        - Bajo: <= percentil 25
        - Medio: percentil 25-50
        - Alto: percentil 50-75
        - Premium: > percentil 75
        
        Args:
            df (DataFrame): DataFrame con columna de precios
            columna_precio (str): Nombre de la columna de precio
            columna_nueva (str): Nombre para la nueva columna de categoria
            
        Returns:
            DataFrame: DataFrame con columna de categoria de precio
        """
        self._log(f"Iniciando categorizacion de precios desde columna '{columna_precio}'")
        df_limpio = df.copy()
        
        if columna_precio not in df_limpio.columns:
            self._log(f"Columna '{columna_precio}' no encontrada", nivel='warning')
            return df_limpio
        
        # Calcular percentiles
        p25 = df_limpio[columna_precio].quantile(0.25)
        p50 = df_limpio[columna_precio].quantile(0.50)
        p75 = df_limpio[columna_precio].quantile(0.75)
        
        self._log(f"Percentiles calculados:")
        self._log(f"  - P25 (Bajo): ${p25:.2f}")
        self._log(f"  - P50 (Medio): ${p50:.2f}")
        self._log(f"  - P75 (Alto): ${p75:.2f}")
        
        # Crear categorias
        def categorizar(precio):
            if pd.isna(precio):
                return 'Desconocido'
            elif precio <= p25:
                return 'Bajo'
            elif precio <= p50:
                return 'Medio'
            elif precio <= p75:
                return 'Alto'
            else:
                return 'Premium'
        
        df_limpio[columna_nueva] = df_limpio[columna_precio].apply(categorizar)
        
        # Contar por categoria
        conteo = df_limpio[columna_nueva].value_counts()
        self._log(f"Distribucion de categorias:")
        for categoria, cantidad in conteo.items():
            porcentaje = (cantidad / len(df_limpio)) * 100
            self._log(f"  - {categoria}: {cantidad} ({porcentaje:.2f}%)")
        
        return df_limpio
    
    # ============================================================
    # TAREA 6: EXPANSION DE CAMPOS ANIDADOS (AMENITIES)
    # ============================================================
    
    def expandir_amenities(self, df, columna_amenities='amenities', top_n=10):
        """
        Expande campo amenities en columnas binarias para los N amenities mas comunes.
        
        Args:
            df (DataFrame): DataFrame con columna de amenities
            columna_amenities (str): Nombre de la columna con amenities
            top_n (int): Numero de amenities mas comunes a expandir
            
        Returns:
            DataFrame: DataFrame con columnas binarias de amenities
        """
        self._log(f"Iniciando expansion de amenities - Top {top_n}")
        df_limpio = df.copy()
        
        if columna_amenities not in df_limpio.columns:
            self._log(f"Columna '{columna_amenities}' no encontrada", nivel='warning')
            return df_limpio
        
        # Procesar amenities (suele venir como string de lista)
        try:
            # Contar frecuencia de cada amenity
            todas_amenities = []
            for amenities_str in df_limpio[columna_amenities].dropna():
                if isinstance(amenities_str, str):
                    # Intentar evaluar como lista
                    try:
                        amenities_list = eval(amenities_str)
                        if isinstance(amenities_list, list):
                            todas_amenities.extend(amenities_list)
                    except:
                        pass
            
            if len(todas_amenities) == 0:
                self._log("No se pudieron extraer amenities", nivel='warning')
                return df_limpio
            
            # Obtener top N mas comunes
            from collections import Counter
            contador = Counter(todas_amenities)
            top_amenities = [amenity for amenity, count in contador.most_common(top_n)]
            
            self._log(f"Top {top_n} amenities mas comunes:")
            for i, (amenity, count) in enumerate(contador.most_common(top_n), 1):
                self._log(f"  {i}. {amenity}: {count} propiedades")
            
            # Crear columnas binarias
            for amenity in top_amenities:
                nombre_columna = f"has_{amenity.lower().replace(' ', '_').replace('-', '_')}"
                df_limpio[nombre_columna] = df_limpio[columna_amenities].apply(
                    lambda x: 1 if isinstance(x, str) and amenity in x else 0
                )
            
            self._log(f"Creadas {len(top_amenities)} columnas binarias de amenities")
        
        except Exception as e:
            self._log(f"Error al expandir amenities: {str(e)}", nivel='error')
        
        return df_limpio
    
    # ============================================================
    # METODO PRINCIPAL: APLICAR TODAS LAS TRANSFORMACIONES
    # ============================================================
    
    def transformar_listings(self, df):
        """
        Aplica TODAS las transformaciones a la tabla listings en orden.
        
        Args:
            df (DataFrame): DataFrame de listings sin procesar
            
        Returns:
            DataFrame: DataFrame transformado y listo para carga
        """
        self._log("="*60)
        self._log("INICIANDO TRANSFORMACION COMPLETA DE LISTINGS")
        self._log("="*60)
        
        registros_iniciales = len(df)
        columnas_iniciales = len(df.columns)
        
        self._log(f"Datos iniciales:")
        self._log(f"  - Registros: {registros_iniciales:,}")
        self._log(f"  - Columnas: {columnas_iniciales}")
        
        # PASO 1: Eliminar duplicados
        df = self.eliminar_duplicados(df, columna_id='id')
        
        # PASO 2: Normalizar precios
        df = self.normalizar_precios(df, columna_precio='price')
        
        # PASO 3: Manejar valores nulos en columnas criticas
        columnas_criticas = ['id', 'name', 'price']
        df = self.manejar_valores_nulos(df, estrategia='eliminar', columnas_criticas=columnas_criticas)
        
        # PASO 4: Convertir fechas
        df = self.convertir_fechas_iso(df, columnas_fecha=['last_scraped', 'host_since', 'first_review', 'last_review'])
        
        # PASO 5: Categorizar precios
        df = self.categorizar_precios(df, columna_precio='price', columna_nueva='price_category')
        
        # PASO 6: Expandir amenities (si existe la columna)
        if 'amenities' in df.columns:
            df = self.expandir_amenities(df, columna_amenities='amenities', top_n=10)
        
        # Estadisticas finales
        registros_finales = len(df)
        columnas_finales = len(df.columns)
        
        self._log("="*60)
        self._log("TRANSFORMACION COMPLETA FINALIZADA")
        self._log("="*60)
        self._log(f"Datos finales:")
        self._log(f"  - Registros: {registros_finales:,} (perdidos: {registros_iniciales - registros_finales})")
        self._log(f"  - Columnas: {columnas_finales} (añadidas: {columnas_finales - columnas_iniciales})")
        
        return df
    
    def transformar_reviews(self, df):
        """
        Aplica transformaciones a la tabla reviews.
        
        Args:
            df (DataFrame): DataFrame de reviews sin procesar
            
        Returns:
            DataFrame: DataFrame transformado
        """
        self._log("="*60)
        self._log("INICIANDO TRANSFORMACION COMPLETA DE REVIEWS")
        self._log("="*60)
        
        registros_iniciales = len(df)
        
        # PASO 1: Eliminar duplicados
        if 'id' in df.columns:
            df = self.eliminar_duplicados(df, columna_id='id')
        
        # PASO 2: Convertir fechas
        df = self.convertir_fechas_iso(df, columnas_fecha=['date'])
        
        # PASO 3: Derivar variables temporales
        if 'date' in df.columns:
            df = self.derivar_variables_fecha(df, columna_fecha='date', prefijo='review_')
        
        # PASO 4: Manejar nulos
        df = self.manejar_valores_nulos(df, estrategia='eliminar', columnas_criticas=['listing_id', 'date'])
        
        registros_finales = len(df)
        
        self._log("="*60)
        self._log("TRANSFORMACION DE REVIEWS FINALIZADA")
        self._log(f"Registros: {registros_iniciales:,} → {registros_finales:,}")
        self._log("="*60)
        
        return df
    
    def finalizar(self):
        """
        Finaliza el proceso de transformacion y cierra logs.
        """
        if self.usar_logs:
            self.logs.finalizar()


# ============================================================
# SCRIPT DE PRUEBA
# ============================================================

if __name__ == "__main__":
    print("="*60)
    print("PRUEBA DE TRANSFORMACION DE AIRBNB")
    print("="*60)
    
    # Importar extraccion
    from extraccion import Extraccion
    from config import MONGODB_URI, MONGODB_DATABASE
    
    # Extraer datos
    print("\n1. Extrayendo datos desde MongoDB Atlas...")
    extraccion = Extraccion()
    df_listings = extraccion.extraer_mongo_df(MONGODB_URI, MONGODB_DATABASE, "listings")
    df_reviews = extraccion.extraer_mongo_df(MONGODB_URI, MONGODB_DATABASE, "reviews")
    
    # Crear transformador
    print("\n2. Inicializando transformador...")
    transformador = Transformacion(usar_logs=True)
    
    # Transformar listings
    print("\n3. Transformando LISTINGS...")
    df_listings_limpio = transformador.transformar_listings(df_listings)
    
    # Transformar reviews
    print("\n4. Transformando REVIEWS...")
    df_reviews_limpio = transformador.transformar_reviews(df_reviews)
    
    # Mostrar resultados
    print("\n" + "="*60)
    print("RESULTADOS DE LA TRANSFORMACION")
    print("="*60)
    print(f"\n📊 LISTINGS:")
    print(f"  - Registros limpios: {len(df_listings_limpio):,}")
    print(f"  - Columnas: {len(df_listings_limpio.columns)}")
    print(f"\n📊 REVIEWS:")
    print(f"  - Registros limpios: {len(df_reviews_limpio):,}")
    print(f"  - Columnas: {len(df_reviews_limpio.columns)}")
    
    # Guardar ejemplo en CSV
    print("\n5. Guardando datos transformados de ejemplo...")
    import os
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    data_folder = os.path.join(project_root, "data")
    
    ruta_listings = os.path.join(data_folder, "listings_transformado_ejemplo.csv")
    ruta_reviews = os.path.join(data_folder, "reviews_transformado_ejemplo.csv")
    
    df_listings_limpio.head(100).to_csv(ruta_listings, index=False)
    df_reviews_limpio.head(100).to_csv(ruta_reviews, index=False)
    print(f"✅ Archivos guardados en: {data_folder}")
    
    # Finalizar
    transformador.finalizar()
    
    print("\n✅ Transformacion completada exitosamente!")
    print(f"📄 Revisa el log en: {transformador.logs.ruta_log}")

    