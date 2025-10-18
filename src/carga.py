# -*- coding: utf-8 -*-
"""
----------------------------------------------------------
    Modulo: carga.py
    Descripcion: Clase Carga para insertar datos transformados
                 en SQLite y exportar a archivos XLSX.
                 
    Funcionalidades:
    1. Conexion a base de datos SQLite
    2. Creacion de tablas para listings y reviews
    3. Insercion de datos transformados
    4. Exportacion a archivos Excel (.xlsx)
    5. Verificacion de carga correcta
    6. Registro de eventos en logs
----------------------------------------------------------
"""
import pandas as pd
import sqlite3
from sqlalchemy import create_engine, inspect, text
from datetime import datetime
import os
import sys
from logs import Logs

# Configurar encoding UTF-8 para Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')


class Carga:
    """
    Clase para cargar datos transformados en SQLite y exportar a Excel.
    Implementa todas las funcionalidades requeridas en el Punto 4 del taller.
    """
    
    def __init__(self, ruta_db='../data/airbnb_etl.db', usar_logs=True):
        """
        Inicializa la clase de carga.
        
        Args:
            ruta_db (str): Ruta donde se creara la base de datos SQLite
            usar_logs (bool): Si True, registra todas las operaciones en logs
        """
        self.ruta_db = ruta_db
        self.usar_logs = usar_logs
        
        if self.usar_logs:
            self.logs = Logs(nombre_proceso='CARGA')
            self.logs.info("Inicializando modulo de Carga de datos")
        
        # Crear engine de SQLAlchemy para SQLite
        self.engine = create_engine(f'sqlite:///{ruta_db}', echo=False)
        
        # Crear directorio si no existe
        directorio = os.path.dirname(ruta_db)
        if directorio and not os.path.exists(directorio):
            os.makedirs(directorio)
            self._log(f"Directorio creado: {directorio}")
    
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
    # CONEXION Y CREACION DE TABLAS
    # ============================================================
    
    def conectar_sqlite(self):
        """
        Establece conexion con la base de datos SQLite.
        
        Returns:
            bool: True si la conexion fue exitosa, False en caso contrario
        """
        try:
            # Probar conexion
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self._log(f"Conexion exitosa a SQLite: {self.ruta_db}")
            return True
            
        except Exception as e:
            self._log(f"Error al conectar a SQLite: {str(e)}", nivel='error')
            return False
    
    def crear_tablas(self):
        """
        Crea las tablas necesarias en SQLite para listings y reviews.
        
        Returns:
            bool: True si las tablas se crearon correctamente
        """
        try:
            self._log("Iniciando creacion de tablas en SQLite")
            
            # SQL para crear tabla listings
            sql_listings = """
            CREATE TABLE IF NOT EXISTS listings (
                id INTEGER PRIMARY KEY,
                name TEXT,
                host_id INTEGER,
                host_name TEXT,
                neighbourhood TEXT,
                latitude REAL,
                longitude REAL,
                room_type TEXT,
                price REAL,
                minimum_nights INTEGER,
                number_of_reviews INTEGER,
                last_review DATE,
                reviews_per_month REAL,
                calculated_host_listings_count INTEGER,
                availability_365 INTEGER,
                price_category TEXT,
                last_scraped DATETIME,
                host_since DATETIME,
                first_review DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            # SQL para crear tabla reviews
            sql_reviews = """
            CREATE TABLE IF NOT EXISTS reviews (
                id INTEGER PRIMARY KEY,
                listing_id INTEGER,
                date DATE,
                review_year INTEGER,
                review_month INTEGER,
                review_day INTEGER,
                review_quarter INTEGER,
                review_day_of_week INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (listing_id) REFERENCES listings (id)
            )
            """
            
            # Ejecutar creacion de tablas
            with self.engine.connect() as conn:
                conn.execute(text(sql_listings))
                conn.execute(text(sql_reviews))
                conn.commit()
            
            self._log("Tablas creadas exitosamente:")
            self._log("  - listings: Informacion de propiedades")
            self._log("  - reviews: Resenas de usuarios")
            
            return True
            
        except Exception as e:
            self._log(f"Error al crear tablas: {str(e)}", nivel='error')
            return False
    
    # ============================================================
    # INSERCION DE DATOS
    # ============================================================
    
    def cargar_listings(self, df_listings, accion='replace'):
        """
        Carga datos de listings en la tabla SQLite.
        
        Args:
            df_listings (DataFrame): DataFrame con datos de listings transformados
            accion (str): 'replace' para reemplazar, 'append' para añadir
            
        Returns:
            bool: True si la carga fue exitosa
        """
        try:
            registros_antes = len(df_listings)
            self._log(f"Iniciando carga de listings - Accion: {accion}")
            self._log(f"Registros a cargar: {registros_antes:,}")
            
            # Preparar datos para SQLite
            df_carga = df_listings.copy()
            
            # Convertir ObjectId a string (si existe)
            if '_id' in df_carga.columns:
                df_carga['_id'] = df_carga['_id'].astype(str)
                self._log("ObjectId convertidos a string para compatibilidad con SQLite")
            
            # Añadir timestamp de creacion
            df_carga['created_at'] = datetime.now()
            
            # Cargar a SQLite
            if_exists = 'replace' if accion == 'replace' else 'append'
            df_carga.to_sql('listings', self.engine, if_exists=if_exists, index=False)
            
            # Verificar carga
            with self.engine.connect() as conn:
                resultado = conn.execute(text("SELECT COUNT(*) FROM listings"))
                registros_cargados = resultado.scalar()
            
            self._log(f"Listings cargados exitosamente: {registros_cargados:,} registros")
            
            if self.usar_logs:
                self.logs.registrar_transformacion(
                    nombre_transformacion="Carga de listings a SQLite",
                    registros_antes=registros_antes,
                    registros_despues=registros_cargados,
                    detalles=f"Base de datos: {self.ruta_db}"
                )
            
            return True
            
        except Exception as e:
            self._log(f"Error al cargar listings: {str(e)}", nivel='error')
            return False
    
    def cargar_reviews(self, df_reviews, accion='replace'):
        """
        Carga datos de reviews en la tabla SQLite.
        
        Args:
            df_reviews (DataFrame): DataFrame con datos de reviews transformados
            accion (str): 'replace' para reemplazar, 'append' para añadir
            
        Returns:
            bool: True si la carga fue exitosa
        """
        try:
            registros_antes = len(df_reviews)
            self._log(f"Iniciando carga de reviews - Accion: {accion}")
            self._log(f"Registros a cargar: {registros_antes:,}")
            
            # Preparar datos para SQLite
            df_carga = df_reviews.copy()
            
            # Convertir ObjectId a string (si existe)
            if '_id' in df_carga.columns:
                df_carga['_id'] = df_carga['_id'].astype(str)
                self._log("ObjectId convertidos a string para compatibilidad con SQLite")
            
            # Añadir timestamp de creacion
            df_carga['created_at'] = datetime.now()
            
            # Cargar a SQLite
            if_exists = 'replace' if accion == 'replace' else 'append'
            df_carga.to_sql('reviews', self.engine, if_exists=if_exists, index=False)
            
            # Verificar carga
            with self.engine.connect() as conn:
                resultado = conn.execute(text("SELECT COUNT(*) FROM reviews"))
                registros_cargados = resultado.scalar()
            
            self._log(f"Reviews cargados exitosamente: {registros_cargados:,} registros")
            
            if self.usar_logs:
                self.logs.registrar_transformacion(
                    nombre_transformacion="Carga de reviews a SQLite",
                    registros_antes=registros_antes,
                    registros_despues=registros_cargados,
                    detalles=f"Base de datos: {self.ruta_db}"
                )
            
            return True
            
        except Exception as e:
            self._log(f"Error al cargar reviews: {str(e)}", nivel='error')
            return False
    
    # ============================================================
    # EXPORTACION A EXCEL
    # ============================================================
    
    def exportar_a_excel(self, df_listings, df_reviews, ruta_excel='../data/airbnb_datos_limpios.xlsx'):
        """
        Exporta los datos transformados a un archivo Excel.
        
        Args:
            df_listings (DataFrame): DataFrame de listings
            df_reviews (DataFrame): DataFrame de reviews
            ruta_excel (str): Ruta del archivo Excel a crear
            
        Returns:
            bool: True si la exportacion fue exitosa
        """
        try:
            self._log(f"Iniciando exportacion a Excel: {ruta_excel}")
            
            # Crear directorio si no existe
            directorio = os.path.dirname(ruta_excel)
            if directorio and not os.path.exists(directorio):
                os.makedirs(directorio)
            
            # Crear archivo Excel con multiples hojas
            with pd.ExcelWriter(ruta_excel, engine='openpyxl') as writer:
                # Hoja 1: Listings
                df_listings.to_excel(writer, sheet_name='Listings', index=False)
                
                # Hoja 2: Reviews
                df_reviews.to_excel(writer, sheet_name='Reviews', index=False)
                
                # Hoja 3: Resumen
                resumen = pd.DataFrame({
                    'Tabla': ['Listings', 'Reviews'],
                    'Registros': [len(df_listings), len(df_reviews)],
                    'Columnas': [len(df_listings.columns), len(df_reviews.columns)],
                    'Fecha_Exportacion': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')] * 2
                })
                resumen.to_excel(writer, sheet_name='Resumen', index=False)
            
            self._log(f"Archivo Excel creado exitosamente:")
            self._log(f"  - Ubicacion: {ruta_excel}")
            self._log(f"  - Hojas: Listings, Reviews, Resumen")
            self._log(f"  - Listings: {len(df_listings):,} registros")
            self._log(f"  - Reviews: {len(df_reviews):,} registros")
            
            return True
            
        except Exception as e:
            self._log(f"Error al exportar a Excel: {str(e)}", nivel='error')
            return False
    
    # ============================================================
    # VERIFICACION DE CARGA
    # ============================================================
    
    def verificar_carga(self):
        """
        Verifica que los datos se cargaron correctamente en SQLite.
        
        Returns:
            dict: Diccionario con estadisticas de verificacion
        """
        try:
            self._log("Iniciando verificacion de carga en SQLite")
            
            with self.engine.connect() as conn:
                # Contar registros en cada tabla
                resultado_listings = conn.execute(text("SELECT COUNT(*) FROM listings"))
                total_listings = resultado_listings.scalar()
                
                resultado_reviews = conn.execute(text("SELECT COUNT(*) FROM reviews"))
                total_reviews = resultado_reviews.scalar()
                
                # Verificar integridad referencial
                resultado_fk = conn.execute(text("""
                    SELECT COUNT(*) 
                    FROM reviews r 
                    LEFT JOIN listings l ON r.listing_id = l.id 
                    WHERE l.id IS NULL
                """))
                reviews_sin_listing = resultado_fk.scalar()
                
                # Obtener informacion de las tablas
                inspector = inspect(self.engine)
                columnas_listings = len(inspector.get_columns('listings'))
                columnas_reviews = len(inspector.get_columns('reviews'))
            
            # Crear resumen de verificacion
            verificacion = {
                'listings': {
                    'total_registros': total_listings,
                    'total_columnas': columnas_listings,
                    'estado': 'OK' if total_listings > 0 else 'ERROR'
                },
                'reviews': {
                    'total_registros': total_reviews,
                    'total_columnas': columnas_reviews,
                    'estado': 'OK' if total_reviews > 0 else 'ERROR'
                },
                'integridad': {
                    'reviews_sin_listing': reviews_sin_listing,
                    'estado': 'OK' if reviews_sin_listing == 0 else 'WARNING'
                },
                'base_datos': {
                    'ruta': self.ruta_db,
                    'tamaño_mb': round(os.path.getsize(self.ruta_db) / (1024*1024), 2) if os.path.exists(self.ruta_db) else 0
                }
            }
            
            # Log de verificacion
            self._log("="*60)
            self._log("VERIFICACION DE CARGA COMPLETADA")
            self._log("="*60)
            self._log(f"📊 LISTINGS:")
            self._log(f"  - Registros: {verificacion['listings']['total_registros']:,}")
            self._log(f"  - Columnas: {verificacion['listings']['total_columnas']}")
            self._log(f"  - Estado: {verificacion['listings']['estado']}")
            
            self._log(f"📊 REVIEWS:")
            self._log(f"  - Registros: {verificacion['reviews']['total_registros']:,}")
            self._log(f"  - Columnas: {verificacion['reviews']['total_columnas']}")
            self._log(f"  - Estado: {verificacion['reviews']['estado']}")
            
            self._log(f"🔗 INTEGRIDAD:")
            self._log(f"  - Reviews sin listing: {verificacion['integridad']['reviews_sin_listing']}")
            self._log(f"  - Estado: {verificacion['integridad']['estado']}")
            
            self._log(f"💾 BASE DE DATOS:")
            self._log(f"  - Ruta: {verificacion['base_datos']['ruta']}")
            self._log(f"  - Tamaño: {verificacion['base_datos']['tamaño_mb']} MB")
            self._log("="*60)
            
            return verificacion
            
        except Exception as e:
            self._log(f"Error en verificacion: {str(e)}", nivel='error')
            return None
    
    # ============================================================
    # METODO PRINCIPAL: CARGA COMPLETA
    # ============================================================
    
    def cargar_datos_completos(self, df_listings, df_reviews, exportar_excel=True):
        """
        Ejecuta el proceso completo de carga de datos.
        
        Args:
            df_listings (DataFrame): DataFrame de listings transformados
            df_reviews (DataFrame): DataFrame de reviews transformados
            exportar_excel (bool): Si True, exporta tambien a Excel
            
        Returns:
            dict: Resultado del proceso de carga
        """
        self._log("="*60)
        self._log("INICIANDO PROCESO COMPLETO DE CARGA")
        self._log("="*60)
        
        resultados = {
            'conexion': False,
            'tablas_creadas': False,
            'listings_cargados': False,
            'reviews_cargados': False,
            'excel_exportado': False,
            'verificacion': None
        }
        
        # PASO 1: Conectar a SQLite
        self._log("PASO 1: Conectando a SQLite...")
        resultados['conexion'] = self.conectar_sqlite()
        
        if not resultados['conexion']:
            self._log("❌ No se pudo conectar a SQLite. Abortando proceso.", nivel='error')
            return resultados
        
        # PASO 2: Crear tablas
        self._log("PASO 2: Creando tablas...")
        resultados['tablas_creadas'] = self.crear_tablas()
        
        if not resultados['tablas_creadas']:
            self._log("❌ No se pudieron crear las tablas. Abortando proceso.", nivel='error')
            return resultados
        
        # PASO 3: Cargar listings
        self._log("PASO 3: Cargando listings...")
        resultados['listings_cargados'] = self.cargar_listings(df_listings)
        
        # PASO 4: Cargar reviews
        self._log("PASO 4: Cargando reviews...")
        resultados['reviews_cargados'] = self.cargar_reviews(df_reviews)
        
        # PASO 5: Exportar a Excel (opcional)
        if exportar_excel:
            self._log("PASO 5: Exportando a Excel...")
            resultados['excel_exportado'] = self.exportar_a_excel(df_listings, df_reviews)
        
        # PASO 6: Verificar carga
        self._log("PASO 6: Verificando carga...")
        resultados['verificacion'] = self.verificar_carga()
        
        # Resumen final
        self._log("="*60)
        self._log("PROCESO DE CARGA FINALIZADO")
        self._log("="*60)
        
        exitos = sum(1 for v in resultados.values() if v is True)
        total = len([k for k in resultados.keys() if k != 'verificacion'])
        
        self._log(f"Pasos exitosos: {exitos}/{total}")
        self._log(f"Estado general: {'✅ EXITOSO' if exitos == total else '⚠️ PARCIAL'}")
        
        return resultados
    
    def finalizar(self):
        """
        Finaliza el proceso de carga y cierra logs.
        """
        if self.usar_logs:
            self.logs.finalizar()


# ============================================================
# SCRIPT DE PRUEBA
# ============================================================

if __name__ == "__main__":
    print("="*60)
    print("PRUEBA DE CARGA DE DATOS AIRBNB")
    print("="*60)
    
    # Importar transformacion
    from transformacion import Transformacion
    from extraccion import Extraccion
    from config import MONGODB_URI, MONGODB_DATABASE
    
    # PASO 1: Extraer datos
    print("\n1. Extrayendo datos desde MongoDB Atlas...")
    extraccion = Extraccion()
    df_listings = extraccion.extraer_mongo_df(MONGODB_URI, MONGODB_DATABASE, "listings")
    df_reviews = extraccion.extraer_mongo_df(MONGODB_URI, MONGODB_DATABASE, "reviews")
    
    # PASO 2: Transformar datos
    print("\n2. Transformando datos...")
    transformacion = Transformacion(usar_logs=False)  # Sin logs para prueba rapida
    df_listings_limpio = transformacion.transformar_listings(df_listings)
    df_reviews_limpio = transformacion.transformar_reviews(df_reviews)
    
    # PASO 3: Cargar datos
    print("\n3. Cargando datos a SQLite y Excel...")
    carga = Carga(usar_logs=True)
    resultados = carga.cargar_datos_completos(df_listings_limpio, df_reviews_limpio)
    
    # Mostrar resultados
    print("\n" + "="*60)
    print("RESULTADOS DE LA CARGA")
    print("="*60)
    for paso, resultado in resultados.items():
        if paso != 'verificacion':
            estado = "✅" if resultado else "❌"
            print(f"{estado} {paso.replace('_', ' ').title()}")
    
    if resultados['verificacion']:
        print(f"\n📊 Verificacion:")
        print(f"  - Listings: {resultados['verificacion']['listings']['total_registros']:,} registros")
        print(f"  - Reviews: {resultados['verificacion']['reviews']['total_registros']:,} registros")
        print(f"  - Base de datos: {resultados['verificacion']['base_datos']['tamaño_mb']} MB")
    
    # Finalizar
    carga.finalizar()
    
    print("\n✅ Proceso de carga completado!")
    print(f"📄 Revisa el log en: {carga.logs.ruta_log}")
