# -*- coding: utf-8 -*-
"""
----------------------------------------------------------
    Modulo: logs.py
    Descripcion: Clase para gestionar el registro de logs
                 del proceso ETL de Airbnb.
                 
    Registra: 
    - Eventos de INFO, WARNING y ERROR
    - Transformaciones aplicadas
    - Cantidad de registros procesados
    - Fecha y hora de cada operacion
----------------------------------------------------------
"""
import logging
import os
from datetime import datetime


class Logs:
    """
    Clase para gestionar el sistema de logging del proyecto ETL.
    Crea archivos de log con formato: logs/log_YYYYMMDD_HHMMSS.txt
    """
    
    def __init__(self, carpeta_logs='../logs', nombre_proceso='ETL'):
        """
        Inicializa el sistema de logging.
        
        Args:
            carpeta_logs (str): Carpeta donde se guardan los logs
            nombre_proceso (str): Nombre del proceso para identificar en logs
        """
        # Obtener ruta absoluta de la carpeta logs
        if carpeta_logs.startswith('../'):
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(script_dir)
            self.carpeta_logs = os.path.join(project_root, carpeta_logs.replace('../', ''))
        else:
            self.carpeta_logs = carpeta_logs
        
        # Crear carpeta de logs si no existe
        if not os.path.exists(self.carpeta_logs):
            os.makedirs(self.carpeta_logs)
        
        # Generar nombre del archivo con fecha y hora
        self.fecha_hora = datetime.now().strftime('%Y%m%d_%H%M%S')
        nombre_archivo = f'log_{nombre_proceso}_{self.fecha_hora}.txt'
        self.ruta_log = os.path.join(self.carpeta_logs, nombre_archivo)
        
        # Configurar logging
        logging.basicConfig(
            filename=self.ruta_log,
            level=logging.INFO,
            filemode='a',  # Modo append (añadir)
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # También mostrar en consola
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console.setFormatter(formatter)
        logging.getLogger('').addHandler(console)
        
        # Registrar inicio del log
        self.info(f"="*60)
        self.info(f"INICIO DEL LOG - Proceso: {nombre_proceso}")
        self.info(f"Archivo de log: {nombre_archivo}")
        self.info(f"="*60)
    
    def info(self, mensaje):
        """
        Registra un mensaje de nivel INFO.
        
        Args:
            mensaje (str): Mensaje a registrar
        """
        logging.info(mensaje)
    
    def warning(self, mensaje):
        """
        Registra un mensaje de nivel WARNING.
        
        Args:
            mensaje (str): Mensaje de advertencia
        """
        logging.warning(mensaje)
    
    def error(self, mensaje):
        """
        Registra un mensaje de nivel ERROR.
        
        Args:
            mensaje (str): Mensaje de error
        """
        logging.error(mensaje)
    
    def registrar_transformacion(self, nombre_transformacion, registros_antes, registros_despues, detalles=""):
        """
        Registra una transformacion aplicada con estadisticas.
        
        Args:
            nombre_transformacion (str): Nombre de la transformacion
            registros_antes (int): Cantidad de registros antes
            registros_despues (int): Cantidad de registros despues
            detalles (str): Detalles adicionales de la transformacion
        """
        diferencia = registros_antes - registros_despues
        porcentaje = (diferencia / registros_antes * 100) if registros_antes > 0 else 0
        
        mensaje = f"""
╔════════════════════════════════════════════════════════════
║ TRANSFORMACION APLICADA: {nombre_transformacion}
╠════════════════════════════════════════════════════════════
║ Registros ANTES:   {registros_antes:,}
║ Registros DESPUES: {registros_despues:,}
║ Diferencia:        {diferencia:,} ({porcentaje:.2f}%)
"""
        if detalles:
            mensaje += f"║ Detalles:          {detalles}\n"
        
        mensaje += "╚════════════════════════════════════════════════════════════"
        
        self.info(mensaje)
    
    def finalizar(self):
        """
        Registra el fin del proceso y cierra el log.
        """
        self.info(f"="*60)
        self.info(f"FIN DEL LOG")
        self.info(f"="*60)
        logging.shutdown()


# Prueba de la clase si se ejecuta directamente
if __name__ == "__main__":
    # Crear instancia de logs
    logs = Logs(nombre_proceso='PRUEBA')
    
    # Ejemplos de uso
    logs.info("Este es un mensaje informativo")
    logs.warning("Esta es una advertencia")
    logs.error("Este es un error")
    
    # Registrar una transformacion
    logs.registrar_transformacion(
        nombre_transformacion="Eliminacion de valores nulos",
        registros_antes=1000,
        registros_despues=950,
        detalles="Eliminados registros sin precio"
    )
    
    logs.finalizar()
    print(f"\n✅ Log de prueba creado en: {logs.ruta_log}")

