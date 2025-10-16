from extraccion import Extraccion
from transformacion import Transformacion

uri = "mongodb://localhost:27017/"
database = "bi_mx"
coleccion = "listings"

extraccion = Extraccion()  # <-- crea la instancia
df = extraccion.extraer_mongo_df(uri, database, coleccion)
Transformacion.transformacion(df)
# print(df.head())
