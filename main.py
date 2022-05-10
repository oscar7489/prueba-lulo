import os
import time
import click
import src.commons as commons
import src.get_info.get_info as get_info 
import src.pre_process.pre_process_1 as pre_process_1
import src.pre_process.pre_process_2 as pre_process_2
import src.pre_process.pre_process_3 as pre_process_3
import src.pre_process.pre_process_4 as pre_process_4
import src.pre_process.pre_process_5 as pre_process_5
import src.cast_std.cast_std as cast_std
import src.profiling.profiing as profiling
import src.metrics.metrics as metrics


def step_get_info(etapa, mes):
    
    try:
        LOGGER = commons.log(os.environ["LOG_NAME"], etapa)
        LOGGER.info("1. Iniciando el llamado a la API ...")
        
        get_info.get_info(etapa, mes)
        
        LOGGER.info("1. Etapa concluida correctamente")
    
    except Exception as e:
        LOGGER.info("1. Etapa Fallida")

    return None


def step_pre_process_1(etapa, mes):
    
    try:
        LOGGER = commons.log(os.environ["LOG_NAME"], etapa)
        LOGGER.info("2. Iniciando etapa de pre procesado 1 ...")
        
        pre_process_1.pre_process_1(etapa, mes)
        
        LOGGER.info("2. Etapa concluida correctamente")
        
    except Exception as e:
        LOGGER.info("2. Etapa Fallida")

    return None


def step_pre_process_2(etapa, mes):
    
    try:
        LOGGER = commons.log(os.environ["LOG_NAME"], etapa)
        LOGGER.info("3. Iniciando etapa de pre procesado 2 ...")
        
        pre_process_2.pre_process_2(etapa, mes)
        
        LOGGER.info("3. Etapa concluida correctamente")
        
    except Exception as e:
        LOGGER.info("3. Etapa Fallida")

    return None


def step_pre_process_3(etapa, mes):

    try:
        LOGGER = commons.log(os.environ["LOG_NAME"], etapa)
        LOGGER.info("4. Iniciando etapa de pre procesado 3 ...")
        
        pre_process_3.pre_process_3(etapa, mes)
        
        LOGGER.info("4. Etapa concluida correctamente")
        
    except Exception as e:
        LOGGER.info("4. Etapa Fallida")

    return None


def step_pre_process_4(etapa, mes):

    try:
        LOGGER = commons.log(os.environ["LOG_NAME"], etapa)
        LOGGER.info("5. Iniciando etapa de pre procesado 4 ...")
        
        pre_process_4.pre_process_4(etapa, mes)
        
        LOGGER.info("5. Etapa concluida correctamente")
        
    except Exception as e:
        LOGGER.info("5. Etapa Fallida")

    return None


def step_pre_process_5(etapa, mes):

    try:
        LOGGER = commons.log(os.environ["LOG_NAME"], etapa)
        LOGGER.info("6. Iniciando etapa de pre procesado 5 ...")
        
        pre_process_5.pre_process_5(etapa, mes)
        
        LOGGER.info("6. Etapa concluida correctamente")
        
    except Exception as e:
        LOGGER.info("6. Etapa Fallida")

    return None


def step_cast_std(etapa, mes):
    
    try:
        LOGGER = commons.log(os.environ["LOG_NAME"], etapa)
        LOGGER.info("7. Iniciando etapa de casteo y estandarización ...")
        
        cast_std.cast_std(etapa, mes)
        
        LOGGER.info("7. Etapa concluida correctamente")
        
    except Exception as e:
        LOGGER.info("7. Etapa Fallida")

    return None


def step_profiling(etapa, mes):
    
    try:
        LOGGER = commons.log(os.environ["LOG_NAME"], etapa)
        LOGGER.info("8. Iniciando etapa de creación de reporte ...")
        
        profiling.profiling(etapa, mes)
        
        LOGGER.info("8. Etapa concluida correctamente")
        
    except Exception as e:
        LOGGER.info("8. Etapa Fallida")

    return None
    

def step_metrics(etapa, mes):
    
    try:
        LOGGER = commons.log(os.environ["LOG_NAME"], etapa)
        LOGGER.info("9. Iniciando etapa de cálculo de métricas ...")
        
        metrics.metrics(etapa, mes)
        
        LOGGER.info("9. Etapa concluida correctamente")
        
    except Exception as e:
        LOGGER.info("9. Etapa Fallida")

    return None


@click.command()
@click.option('--etapa', default='metrics', help='Ingrese la etapa de la prueba que desea ejecutar. El orden es: get_info -> pre_process_1 -> pre_process_2 -> pre_process_3 -> pre_process_4 -> pre_process_5 -> cast_std -> profiling -> metrics')
@click.option('--mes', default='diciembre', help='Mes del año 2020 del cual desea extraer la informacion de la API')

def main(etapa, mes):
    
    start = time.time()    
    commons.set_environment_variables()
    
    log_name = commons.log_name(etapa)
    os.environ["LOG_NAME"] = log_name    
    LOGGER = commons.log(os.environ["LOG_NAME"], etapa)
    
    LOGGER.info("Prueba técnica Lulo Bank")
    LOGGER.info("Ejecutando etapa {0}".format(etapa))
    
    function_dict = {
        "get_info": step_get_info,
        "pre_process_1": step_pre_process_1,
        "pre_process_2": step_pre_process_2,
        "pre_process_3": step_pre_process_3,
        "pre_process_4": step_pre_process_4,
        "pre_process_5": step_pre_process_5,
        "cast_std": step_cast_std,
        "profiling": step_profiling,
        "metrics": step_metrics,
    }
    
    step_function = function_dict.get(etapa)
    step_function(etapa, mes)
     
    commons.timer(start, time.time(), log_name)

    return None


if __name__ == "__main__":
    main()
