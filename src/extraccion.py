# -*- coding: utf-8 -*-
"""
----------------------------------------------------------
    Modulo: extraccion.py
    Descripcion: Clase para conectar a MongoDB Atlas y 
                 extraer datos de las colecciones:
                 - listings
                 - reviews
                 - calendar 
----------------------------------------------------------
"""
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ConfigurationError
import sys
from logs import Logs

# Configurar encoding UTF-8 para Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')


class Extraccion:
    """
    Clase para gestionar la conexión y extracción de datos 
    desde una base de datos MongoDB local.
    """
    def __init__(self, usar_logs=True):
        """
        Inicializa la clase de extraccion.
        
        Args:
            usar_logs (bool): Si True, registra todas las operaciones en logs
        """
        self.usar_logs = usar_logs
        if self.usar_logs:
            self.logs = Logs(nombre_proceso='EXTRACCION')
            self.logs.info("Inicializando modulo de Extraccion de Airbnb")
    
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
    
    def extraccion_csv(self, ruta="", separador=""):
        df = pd.read_csv(ruta, sep=separador)
        if self.validar_df(df):
            return df
    
    def extraccion_xlsx(self, ruta=""):
        df = pd.read_excel(ruta)
        if self.validar_df(df):
            return df

    def get_mongodb(self, uri, database):
        try:
            self._log(f"Conectando a MongoDB Atlas - Base de datos: {database}")
            client = MongoClient(uri)
            db = client[database]
            db.list_collection_names()
            self._log(f"Conexion exitosa a la base de datos: {database}")
            print(f"✅ Conexión exitosa a la base de datos: {database}")
            return db
        except ConnectionError as e:
            self._log(f"Error al conectar la base de datos: {str(e)}", nivel='error')
            print("❌ Error al conectar la base de datos:", e)
            return None
        
    def extraer_mongo_df(self, uri, database, coleccion, filtro={}):
        """
        Extrae datos de una colección de MongoDB y los retorna como un DataFrame.
        Muestra mensajes básicos en consola sobre la conexión y cantidad de registros.
        """
        try:
            self._log(f"Conectando a la base de datos '{database}'...")
            print(f"Conectando a la base de datos '{database}'...")
            client = MongoClient(uri)
            db = client[database]
            db.list_collection_names()  # fuerza verificación
            self._log(f"Conexion exitosa a la base de datos '{database}'.")
            print(f"✅ Conexión exitosa a la base de datos '{database}'.")

            self._log(f"Extrayendo datos de la coleccion '{coleccion}'...")
            print(f"Extrayendo datos de la colección '{coleccion}'...")
            datos = list(db[coleccion].find(filtro))

            if datos:
                df = pd.DataFrame(datos)
                self._log(f"Coleccion '{coleccion}' extraida correctamente. Registros obtenidos: {len(df)}")
                print(f"📦 Colección '{coleccion}' extraída correctamente. Registros obtenidos: {len(df)}")
                return df
            else:
                self._log(f"No se encontraron datos en la coleccion '{coleccion}'.", nivel='warning')
                print(f"⚠️ No se encontraron datos en la colección '{coleccion}'.")
                return pd.DataFrame()

        except Exception as e:
            self._log(f"Error al extraer datos de MongoDB ({coleccion}): {str(e)}", nivel='error')
            print(f"❌ Error al extraer datos de MongoDB ({coleccion}): {e}")
            return pd.DataFrame()
        
    def validar_df(self, df=pd.DataFrame()):
        return len(df) > 0
    
    def finalizar(self):
        """
        Finaliza el proceso de logs.
        """
        if self.usar_logs:
            self.logs.info("FIN DEL LOG")
            self.logs.finalizar()


# 👇 ESTE BLOQUE DEBE ESTAR FUERA DE LA CLASE (SIN INDENTAR)
if __name__ == "__main__":
    # URI de MongoDB Atlas (nube)
    uri = "mongodb+srv://sebastianmedina311114_db_user:dbjHmHZvZw1fbNT7@cluster0.ch7rr86.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    database = "bi_mx"

    extraccion = Extraccion()
    print("🚀 Iniciando prueba de conexión a MongoDB Atlas...")

    df_listings = extraccion.extraer_mongo_df(uri, database, "listings")
    df_reviews = extraccion.extraer_mongo_df(uri, database, "reviews")
    # df_calendar = extraccion.extraer_mongo_df(uri, database, "calendar")

    print("\nResumen general:")
    print(f"Listings: {len(df_listings)} registros")
    print(f"Reviews: {len(df_reviews)} registros")
