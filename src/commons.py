import configparser
import logging
import pandas as pd
import os


def set_environment_variables ():
    
    config = configparser.ConfigParser()
    config.sections()
    config_path = "config.ini"
    config.read(config_path)
    
    os.environ["PATH_LOGS"] = config["prueba_lulo"]["path_logs"]   
    os.environ["API_URL"] = config["prueba_lulo"]["api_url"]
    os.environ["PATH_DATA_RAW"] = config["prueba_lulo"]["path_data_raw"]
    os.environ["PATH_DATA_PRE_PROCESS_1"] = config["prueba_lulo"]["path_data_pre_process_1"]
    os.environ["PATH_DATA_PRE_PROCESS_2"] = config["prueba_lulo"]["path_data_pre_process_2"]
    os.environ["PATH_DATA_PRE_PROCESS_3"] = config["prueba_lulo"]["path_data_pre_process_3"]
    os.environ["PATH_DATA_PRE_PROCESS_4"] = config["prueba_lulo"]["path_data_pre_process_4"]
    os.environ["PATH_DATA_PRE_PROCESS_5"] = config["prueba_lulo"]["path_data_pre_process_5"]
    os.environ["PATH_CAST_STD_DAILY"] = config["prueba_lulo"]["path_cast_std_daily"]
    os.environ["PATH_CAST_STD_FULL"] = config["prueba_lulo"]["path_cast_std_full"]
    os.environ["PATH_PROFILING_DAILY"] = config["prueba_lulo"]["path_profiling_daily"]
    os.environ["PATH_PROFILING_FULL"] = config["prueba_lulo"]["path_profiling_full"]
    os.environ["PATH_METRICS"] = config["prueba_lulo"]["path_metrics"]
                
    return None


def log_name (etapa):
    
    logger_name = {
        "get_info": "Obteniendo_Data",
        "pre_process_1": "Pre_Procesado_1",
        "pre_process_2": "Pre_Procesado_2",
        "pre_process_3": "Pre_Procesado_3",
        "pre_process_4": "Pre_Procesado_4",
        "pre_process_5": "Pre_Procesado_5",
        "cast_std": "Casteo_de_Data",
        "profiling": "Reporte",
        "metrics": "Metricas",
    }
    
    nombre_log = logger_name.get(etapa)    
    return nombre_log


def log(var, etapa):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s -> %(name)s -> %(levelname)s: %(message)s",
        datefmt="%d-%m-%Y %H:%M:%S",
        filename=os.environ["PATH_LOGS"] + '{0}.log'.format(etapa),
        encoding='utf-8',
        filemode='w',
    )
    LOGGER = logging.getLogger(var)
    return LOGGER    


def timer(start, end, etapa):
    LOGGER = logging.getLogger(etapa)
    hours, rem = divmod(end - start, 3600)
    minutes, seconds = divmod(rem, 60)
    LOGGER.info(
        "Duración de la etapa: {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds)
    )
    return None
    

def json_embebed_columns(df):

    schema = df.columns.values

    good_columns = []
    bad_columns = []

    for i in schema:
        a = df[i][0]
        b = type(a) is dict

        if b == True:
            bad_columns.append(i)
        else:
            good_columns.append(i)
            
    df_good = df[good_columns]
    df_bad = df[bad_columns]

    return good_columns, bad_columns, df_good, df_bad


def dates(mes):

    meses1 = {
        'enero': '01',
        'marzo': '03',
        'mayo': '05',
        'julio': '07',
        'agosto': '08',
        'octubre': '10',
        'diciembre': '12'
    }

    meses2 = {
        'abril': '04',
        'junio': '06',
        'septiembre': '09',
        'noviembre': '11'
    }

    nombre_meses1 = meses1.keys()
    nombre_meses2 = meses2.keys()

    if mes in nombre_meses1:
        n = 32
        m = meses1.get(mes)
    elif mes in nombre_meses2:
        n = 31
        m = meses2.get(mes)
    else:
        n = 29
        m = '02'

    dates = []

    for i in range(1, n):
        if i < 10:
            j = str(i)
            day = '0{0}'.format(j)
        else:
            j = str(i)
            day = '{0}'.format(j)

        date = '2020-{0}-{1}'.format(m, day)
        dates.append(date)

    return dates


def list_json_files(path):

    files = os.listdir(path)

    json_files = []

    for i in files:
        if '.json' in i:
            paths = path + i
            json_files.append(paths)

    return json_files


def list_csv_files(path):

    files = os.listdir(path)

    csv_files = []

    for i in files:
        if '.csv' in i:
            paths = path + i
            csv_files.append(paths)

    return csv_files


def list_parquet_files(path):

    files = os.listdir(path)

    parquet_files = []

    for i in files:
        if '.parquet' in i:
            paths = path + i
            parquet_files.append(paths)

    return parquet_files


def append_parquet(path):
    
    parquet_files = list_parquet_files(path)
    
    df1 = pd.read_parquet(parquet_files[0])
    schema = df1.columns.values
    df_append = df = pd.DataFrame(columns=schema)
    
    for i in range(len(parquet_files)):
        
        df = pd.read_parquet(parquet_files[i])
        # df_append = df.append(df, ignore_index=True)
        df_append = pd.concat([df_append, df])
        
    return df_append