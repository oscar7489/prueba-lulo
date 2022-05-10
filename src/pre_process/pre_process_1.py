import pandas as pd
import os
import src.commons as commons


def pre_process_1 (etapa, mes):
    
    LOGGER = commons.log(os.environ["LOG_NAME"], etapa)   
    dates = commons.dates(mes)

    for i in dates:
        LOGGER.info("----------")
        LOGGER.info("2.1. Leyendo la data raw para la fecha: {0} ...".format(i))
        path = os.environ["PATH_DATA_RAW"] + "tv-shows-info-{0}.json".format(i)
        df = pd.read_json(path)
        LOGGER.info("2.1. Data raw leída correctamente")
        
        LOGGER.info("2.2. Guardando data raw sin pre procesado 1 ...")
        df.to_csv(os.environ["PATH_DATA_PRE_PROCESS_1"] + 'tv-shows-info-full-{0}.csv'.format(i), index=False)
        LOGGER.info("2.2. Data raw sin pre procesado 1 guardada correctamente")
        
        LOGGER.info("2.3. Iniciando pre procesado 1 ...")
        
        LOGGER.info("2.4. Identificando columnas con JSON embebidos y separandolas de las columnas correctas ...")
        good_columns, bad_columns, df_good, df_bad = commons. json_embebed_columns(df)
        LOGGER.info("2.4. Identificación y separación OK")

        if len(good_columns) > 0:
            df_good.to_csv(os.environ["PATH_DATA_PRE_PROCESS_1"] + 'tv-shows-info-good-{0}.csv'.format(i), index=False)

        if len(bad_columns) > 0:
            for j in bad_columns:
                df2 = df_bad[j]
                df2 = df2.dropna()
                df2.to_json(os.environ["PATH_DATA_PRE_PROCESS_1"] + 'tv-shows-info-bad-{0}-{1}.json'.format(j, i), orient='records')
                
        LOGGER.info("2.3. Pre procesado 1 terminado correctamente")
    
    return None