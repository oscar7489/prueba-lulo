import requests
import os
import json
import src.commons as commons

def get_info(etapa, mes):
    
    LOGGER = commons.log(os.environ["LOG_NAME"], etapa)   

    dates = commons.dates(mes)

    for i in dates:
        LOGGER.info("----------")
        LOGGER.info("1.1. Obteniendo información de la API de la fecha: {0} ...".format(i))
        # api_response = requests.get('http://api.tvmaze.com/schedule/web?date={0}'.format(i))
        api_response = requests.get(os.environ["API_URL"] + 'date=' + i)
        json_response = api_response.json()
        # with open("data/data_raw/tv-shows-info-{0}.json".format(i), "w") as outfile:
        #     json.dump(json_response, outfile)
        with open(os.environ["PATH_DATA_RAW"] + "tv-shows-info-{0}.json".format(i), "w") as outfile:
            json.dump(json_response, outfile)
        LOGGER.info("1.1. Información guardada correctamente")
    
    return None