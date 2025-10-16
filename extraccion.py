"""
----------------------------------------------------------
    Módulo: extraccion.py
    Descripción: Clase para conectar a MongoDB local y 
                 extraer datos de las colecciones:
                 - listings
                 - reviews
                 - calendar 
----------------------------------------------------------
"""
import pandas as pd
import requests
from pymongo import MongoClient
from pymongo.errors import ConfigurationError
from mysql.connector import Error


class Extraccion:
    """
    Clase para gestionar la conexión y extracción de datos 
    desde una base de datos MongoDB local.
    """
    def __init__(self):
        pass
    
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
            client = MongoClient(uri)
            db = client[database]
            db.list_collection_names()
            print(f"✅ Conexión exitosa a la base de datos: {database}")
            return db
        except ConnectionError as e:
            print("❌ Error al conectar la base de datos:", e)
            return None
        
    def extraer_mongo_df(self, uri, database, coleccion, filtro={}):
        """
        Extrae datos de una colección de MongoDB y los retorna como un DataFrame.
        Muestra mensajes básicos en consola sobre la conexión y cantidad de registros.
        """
        try:
            print(f"Conectando a la base de datos '{database}'...")
            client = MongoClient(uri)
            db = client[database]
            db.list_collection_names()  # fuerza verificación
            print(f"✅ Conexión exitosa a la base de datos '{database}'.")

            print(f"Extrayendo datos de la colección '{coleccion}'...")
            datos = list(db[coleccion].find(filtro))

            if datos:
                df = pd.DataFrame(datos)
                print(f"📦 Colección '{coleccion}' extraída correctamente. Registros obtenidos: {len(df)}")
                return df
            else:
                print(f"⚠️ No se encontraron datos en la colección '{coleccion}'.")
                return pd.DataFrame()

        except Exception as e:
            print(f"❌ Error al extraer datos de MongoDB ({coleccion}): {e}")
            return pd.DataFrame()
        
    def validar_df(self, df=pd.DataFrame()):
        return len(df) > 0


# 👇 ESTE BLOQUE DEBE ESTAR FUERA DE LA CLASE (SIN INDENTAR)
if __name__ == "__main__":
    uri = "mongodb://localhost:27017/"
    database = "bi_mx"

    extraccion = Extraccion()
    print("🚀 Iniciando prueba de conexión...")

    df_listings = extraccion.extraer_mongo_df(uri, database, "listings")
    df_reviews = extraccion.extraer_mongo_df(uri, database, "reviews")
    # df_calendar = extraccion.extraer_mongo_df(uri, database, "calendar")

    print("\nResumen general:")
    print(f"Listings: {len(df_listings)} registros")
    print(f"Reviews: {len(df_reviews)} registros")
