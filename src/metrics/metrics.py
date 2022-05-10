import pandas as pd
import os
import src.commons as commons


def metrics(etapa, mes):

    LOGGER = commons.log(os.environ["LOG_NAME"], etapa)
    LOGGER.info("----------")

    LOGGER.info("9.1. Leyendo data limpia del mes completo ...")
    path_full = os.environ["PATH_CAST_STD_FULL"]
    parquet_file_full = commons.list_parquet_files(path_full)
    df = pd.read_parquet(parquet_file_full[0])
    LOGGER.info("9.1. Data leída correctamente")
    LOGGER.info("----------")

    df = df.rename(columns={'id': 'ID'})
    df = df.rename(columns={'name': 'Nombre del capítulo'})
    df = df.rename(columns={'_embedded_show_name': 'Nombre de la serie'})
    df = df.rename(columns={'_embedded_show_genres': 'Genero'})
    df = df.rename(columns={'_embedded_show_type': 'Tipo'})
    df = df.rename(columns={'_embedded_show_officialSite': 'Sitio Web'})

    LOGGER.info("9.2. Calculando metricas de runtime de todas las series  ...")
    tabla_metricas = df['runtime'].describe()
    tabla_metricas.to_json(os.environ["PATH_METRICS"] + 'metrics_runtime.json')
    LOGGER.info("9.2. La media de runtime de las series es: {0} minutos".format(
        df['runtime'].mean()))
    LOGGER.info("----------")

    LOGGER.info("9.3. Contando los generos de todas las series  ...")
    LOGGER.info("9.3.1. Tabla de generos de las series:")
    tabla_genero = df.groupby(['Genero'])['Genero'].count().rename("Total")
    tabla_genero.to_csv(os.environ["PATH_METRICS"] + 'generos.csv')
    LOGGER.info(tabla_genero)
    LOGGER.info("9.3.2. Tabla de tipos de las series:")
    tabla_genero = df.groupby(['Tipo'])['Tipo'].count().rename("Total")
    tabla_genero.to_csv(os.environ["PATH_METRICS"] + 'tipos.csv')
    LOGGER.info(tabla_genero)
    LOGGER.info("----------")

    LOGGER.info(
        "9.4. Listando los dominios web del sitio oficial de las series  ...")
    tabla_dominios = df[['ID', 'Nombre de la serie',
                         'Nombre del capítulo', 'Sitio Web']]
    tabla_dominios.to_csv(
        os.environ["PATH_METRICS"] + 'dominios.csv', index=False)
    LOGGER.info(tabla_dominios)
    LOGGER.info("----------")

    return
