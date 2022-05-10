PRUEBA TÉCNICA LULO

Cumpliendo con la prueba técnica se entrega el siguiente repositorio de código: https://github.com/oscar7489/prueba-lulo

EL repositorio contiene la siguiente estructura:
    1. Carpeta data que contiene las salidas de cada etapa de la preuba. En especial enfásis en las carpetas:
        * data_raw = data en crudo del llamado de la api.
        * data_cleaned = archivos parquet después del pre procesado de los datos en crudo.
        * profiling = contiene los reportes en html pedidos en la prueba.
        * metrics = contiene las metricas y salidas pedidas en la prueba.

    2. Carpeta logs con los logs por etapas del proceso.

    3. Carpeta src con los archivos de código realizados para cada etapa.

    4. config.ini que contiene las rutas de salida de cada etapa.

    5. main.py que contiene el código de la función principal. Para ejecutarlo se le debe indicar el mes del año 2020 y la etapa a ejecutar. En la ayuda de la función se encuentran las etapas. Un ejemplo de ejcución sería: python main.py --etapa=get_info --mes=diciembre

    6. requirements.in y requirements.txt con las dependencias del código. El requirements.txt se creo con pip-compile a partir de python 3.10.

    