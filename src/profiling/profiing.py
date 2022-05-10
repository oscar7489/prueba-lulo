import os
import pandas as pd
from pandas_profiling import ProfileReport
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
plt.set_loglevel('WARNING')
import src.commons as commons



def profiling (etapa, mes):
    
    LOGGER = commons.log(os.environ["LOG_NAME"], etapa) 
    path_daily = os.environ["PATH_CAST_STD_DAILY"]
    path_full = os.environ["PATH_CAST_STD_FULL"]
    
    parquet_file_daily = commons.list_parquet_files(path_daily)
    parquet_file_full = commons.list_parquet_files(path_full)
    
    dates = commons.dates(mes)
    
    for i in range(len(parquet_file_daily)):
        
        LOGGER.info("----------")
        LOGGER.info("8.1. Leyendo la data limpia para la fecha: {0} ...".format(dates[i]))        
        df = pd.read_parquet(parquet_file_daily[i])
        LOGGER.info("Data leída correctamente")
        
        LOGGER.info("8.2. Creando reporte para el día: {0}".format(dates[i])) 
        profile_daily = ProfileReport(df)
        profile_daily.to_file(os.environ["PATH_PROFILING_DAILY"] + "profiling-{0}.html".format(dates[i]))
        LOGGER.info("8.2. Reporte creado correctamente")
    

    LOGGER.info("8.3. Leyendo data limpia del mes completo") 
    df_append_parquet = pd.read_parquet(parquet_file_full[0])
    LOGGER.info("Data leída correctamente")
    LOGGER.info("8.4. Creando reporte para el mes completo") 
    profile_full = ProfileReport(df_append_parquet)
    profile_full.to_file(os.environ["PATH_PROFILING_FULL"] + "profiling_full.html")
    LOGGER.info("8.4. Reporte creado correctamente")
    
    return None