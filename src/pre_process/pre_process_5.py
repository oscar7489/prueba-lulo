import os
import pandas as pd
import src.commons as commons


def pre_process_5(etapa, mes):

    LOGGER = commons.log(os.environ["LOG_NAME"], etapa)

    path = os.environ["PATH_DATA_PRE_PROCESS_4"]
    json_files = commons.list_json_files(path)

    for j in json_files:

        lst = []
        for pos, char in enumerate(j):
            if(char == '-'):
                lst.append(pos)
        nombre_columna = j[lst[-4]+1:lst[-3]]
        fecha_file = j[lst[-3]+1:lst[-1]+3]

        LOGGER.info("----------")
        LOGGER.info(
            "6.1. Leyendo la data pre procesada 4 para la fecha: {0} ...".format(fecha_file))
        df = pd.read_json(j)
        LOGGER.info("6.1. Data raw leída correctamente")

        LOGGER.info("6.2. Iniciando pre procesado 5 ...")

        LOGGER.info(
            "6.3. Identificando columnas con JSON embebidos y separandolas de las columnas correctas ...")
        good_columns, bad_columns, df_good, df_bad = commons.json_embebed_columns(
            df)
        LOGGER.info("6.3. Identificación y separación OK")

        LOGGER.info("6.4. Renombrando columnas correctas ...")
        new_good_columns = []
        for column in good_columns:
            new_good_columns.append(nombre_columna + '_' + column)
        df_good.columns = new_good_columns
        LOGGER.info("6.4. Renombrado OK")

        LOGGER.info("6.5. Renombrando columnas incorrectas ...")
        new_bad_columns = []
        for column2 in bad_columns:
            new_bad_columns.append(nombre_columna + '_' + column2)
        df_bad.columns = new_bad_columns
        LOGGER.info("6.5. Renombrado OK")

        if len(new_good_columns) > 0:
            df_good.to_csv(os.environ["PATH_DATA_PRE_PROCESS_5"] +
                           'tv-shows-info-good-{0}-{1}.csv'.format(nombre_columna, fecha_file), index=False)

        if len(new_bad_columns) > 0:
            for i in new_bad_columns:
                df2 = df_bad[i]
                df2 = df2.dropna()
                df2.to_json(os.environ["PATH_DATA_PRE_PROCESS_5"] +
                            'tv-shows-info-bad-{0}-{1}.json'.format(i, fecha_file), orient='records')

        LOGGER.info("6.2. Pre procesado 5 terminado correctamente")

    return None
