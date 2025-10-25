# 🏠 ETL Airbnb Ciudad de México

## 📋 Descripción del Proyecto

Este proyecto implementa un proceso ETL (Extract, Transform, Load) completo para analizar datos de Airbnb en Ciudad de México. El proceso extrae datos desde MongoDB Atlas, los transforma aplicando limpieza y normalización, y los carga en una base de datos SQLite y archivos Excel.

## 🎯 Objetivo

Aplicar los conceptos de Extracción, Transformación y Carga (ETL) sobre los datasets de Airbnb Ciudad de México, implementando un proceso automatizado en Python que incluya manejo de logs y documentación del flujo de transformación.

## 🏗️ Estructura del Proyecto

```
etl_airbnb/
├── src/                    # Código fuente Python
│   ├── extraccion.py      # Clase para extraer datos de MongoDB
│   ├── transformacion.py  # Clase para transformar datos
│   ├── carga.py          # Clase para cargar datos a SQLite/Excel
│   ├── logs.py           # Sistema de logging
│   └── config.py         # Configuraciones
├── notebooks/             # Jupyter Notebooks
│   └── exploracion_airbnb.ipynb
├── data/                  # Datos procesados
│   ├── airbnb_etl.db     # Base de datos SQLite
│   └── airbnb_datos_limpios.xlsx
├── logs/                  # Archivos de log
├── README.md
└── requirements.txt
```

## 🚀 Instalación

### 1. Crear entorno virtual
```bash
python -m venv venv
```

### 2. Activar entorno virtual
```bash
# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

### 3. Instalar dependencias
```bash
pip install -r requirements.txt
```

## 📊 Ejecución del ETL

### Ejecutar proceso completo
```bash
cd src
python extraccion.py
python transformacion.py
python carga.py
```

### Ejecutar módulos individuales
```bash
# Solo extracción
python src/extraccion.py

# Solo transformación
python src/transformacion.py

# Solo carga
python src/carga.py
```

## 👥 Integrantes del Grupo

- **Sebastian Medina**
- **Melisa**
- **Carlos**

## 📋 Responsabilidades

### Sebastian Medina
Transformación de datos
### Melisa
Exploración
### Carlos
Carga y logs

## 📈 Características Implementadas

### 🔍 Extracción
- Conexión a MongoDB Atlas
- Extracción de colecciones: listings, reviews
- Validación de datos
- Logging detallado

### 🔄 Transformación
- Limpieza de valores nulos y duplicados
- Normalización de precios
- Conversión de fechas a formato ISO
- Derivación de variables temporales
- Categorización de precios
- Expansión de campos anidados (amenities)

### 📊 Carga
- Base de datos SQLite
- Exportación a Excel (.xlsx)
- Verificación de integridad
- Logging de operaciones

### 📝 Logging
- Archivos de log por ejecución
- Niveles: INFO, WARNING, ERROR
- Registro de transformaciones aplicadas
- Conteo de registros antes/después

## 📊 Datos Procesados

- **Listings**: 52,468 propiedades
- **Reviews**: 1,388,226 reseñas
- **Base de datos**: SQLite con tablas normalizadas
- **Excel**: Archivo multi-hoja con datos limpios

## 🔧 Tecnologías Utilizadas

- **Python 3.8+**
- **Pandas** - Manipulación de datos
- **PyMongo** - Conexión a MongoDB
- **SQLAlchemy** - ORM para SQLite
- **OpenPyXL** - Exportación a Excel
- **Matplotlib/Seaborn** - Visualizaciones
- **MongoDB Atlas** - Base de datos origen
- **SQLite** - Base de datos destino
