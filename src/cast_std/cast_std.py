import os
import pandas as pd
from numpy import float64
import src.commons as commons


def cast_std (etapa, mes):
    
    LOGGER = commons.log(os.environ["LOG_NAME"], etapa)  
    
    path1 = os.environ["PATH_DATA_PRE_PROCESS_1"]
    path2 = os.environ["PATH_DATA_PRE_PROCESS_3"]
    csv_files1 = commons.list_csv_files(path1)
    csv_files2 = commons.list_csv_files(path2)

    csv_good = []
    for i in csv_files1:
        if 'good' in i:
            csv_good.append(i)

    embedded = []
    for j in csv_files2:
        if 'embedded' in j:
            embedded.append(j)

    links = []
    for k in csv_files2:
        if 'links' in k:
            links.append(j)
            
    dates = commons.dates(mes)

    for m in range(len(csv_good)):
        
        date = dates[m]
        LOGGER.info("----------")
        
        LOGGER.info("7.1. Leyendo la data obtenida en el pre procesado para la fecha: {0} ...".format(date))
        df_good = pd.read_csv(csv_good[m])
        df_embedded = pd.read_csv(embedded[m])
        
        columns_embedded = df_embedded.columns.values
        
        if "_embedded_show_network" in columns_embedded:
            df_embedded_2 = df_embedded.drop(columns=['_embedded_show_network'])
        else:
            df_embedded_2 = df_embedded
            
        df_links = pd.read_csv(links[m])
        LOGGER.info("7.1. Data leída correctamente")
        
        LOGGER.info("7.2. Uniendo data limpia ...")
        df_merge1 = pd.merge(df_good, df_embedded_2,
                             left_index=True, right_index=True)
        df_merge2 = pd.merge(df_merge1, df_links,
                             left_index=True, right_index=True)
        LOGGER.info("7.2. Data limpia OK")

        LOGGER.info("7.3. Casteando data limpia ...")
        df_casted = df_merge2.astype({
                                    "id": str,
                                    "url": str,
                                    "name": str,
                                    "season": float64,
                                    "number": float64,
                                    "type": str,
                                    "airdate": str,
                                    "airtime": str,
                                    "runtime": float64,
                                    "summary": str,
                                    "_embedded_show_id": str,                
                                    "_embedded_show_url": str,
                                    "_embedded_show_name": str,
                                    "_embedded_show_type": str,
                                    "_embedded_show_language": str,
                                    "_embedded_show_genres": str,
                                    "_embedded_show_status": str,
                                    "_embedded_show_runtime": float64,
                                    "_embedded_show_averageRuntime": float64,
                                    "_embedded_show_premiered": str,
                                    "_embedded_show_ended": str,
                                    "_embedded_show_officialSite": str,
                                    "_embedded_show_weight": float64,
                                    "_embedded_show_dvdCountry": str,
                                    "_embedded_show_summary": str,
                                    "_embedded_show_updated": float64,
                                    "_links_self_href": str,
                                    })
        LOGGER.info("7.3. Casteo OK ...")
        
        LOGGER.info("7.4. Guardando data limpia por día ...")
        df_casted.to_parquet(os.environ["PATH_CAST_STD_DAILY"] + "tv-shows-info-{0}.parquet".format(date), compression='snappy', index=False)
        LOGGER.info("7.4. Guardado OK")
    
    LOGGER.info("7.5. Guardando data limpia de todo el mes ...")
    df_append_parquet = commons.append_parquet(os.environ["PATH_CAST_STD_DAILY"])
    df_append_parquet.to_parquet(os.environ["PATH_CAST_STD_FULL"] + "tv-shows-info-full.parquet".format(date), compression='snappy', index=False)
    LOGGER.info("7.5. Guardado OK")
    
    return None