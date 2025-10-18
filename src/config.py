# -*- coding: utf-8 -*-
"""
----------------------------------------------------------
    Modulo: config.py
    Descripcion: Configuraciones centralizadas para el proyecto ETL
                 de Airbnb Ciudad de Mexico.
----------------------------------------------------------
"""

# Configuracion de MongoDB Atlas
MONGODB_URI = "mongodb+srv://sebastianmedina311114_db_user:dbjHmHZvZw1fbNT7@cluster0.ch7rr86.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
MONGODB_DATABASE = "bi_mx"

# Configuracion de SQLite
SQLITE_DB_PATH = "../data/airbnb_etl.db"

# Configuracion de archivos de salida
EXCEL_OUTPUT_PATH = "../data/airbnb_datos_limpios.xlsx"
CSV_OUTPUT_PATH = "../data/airbnb_datos_limpios.csv"

# Configuracion de logs
LOGS_DIRECTORY = "../logs"