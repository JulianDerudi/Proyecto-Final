import requests
import pandas as pd
from datetime import datetime
from configparser import ConfigParser

import pyarrow as pa
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError


#############################################################################
###############################  FUNCIONES  #################################
#############################################################################

def get_data(base_url, endpoint, data_field, params=None, headers=None):
    """
    Realiza una solicitud GET a una API para obtener datos.

    Parámetros:
        * base_url (str): La URL base de la API.
        * endpoint (str): El endpoint de la API al que se realizará la solicitud.
        * params (dict): Parámetros de consulta para enviar con la solicitud.
        * data_field (str): El nombre del campo en el JSON que contiene los datos.
        * headers (dict): Encabezados para enviar con la solicitud.

    Retorna:
        * dict: Los datos obtenidos de la API en formato JSON.
    """
    try:
        endpoint_url = f"{base_url}/{endpoint}"
        response = requests.get(endpoint_url, params=params, headers=headers)
        response.raise_for_status()  # Levanta una excepción si hay un error en la respuesta HTTP.

        # Verificar si los datos están en formato JSON.
        try:
            data = response.json()
            data = data[data_field]
        except:
            print("El formato de respuesta no es el esperado")
            return None
        return data

    except requests.exceptions.RequestException as e:
        # Capturar cualquier error de solicitud, como errores HTTP.
        print(f"La petición ha fallado. Código de error : {e}")
        return None
    

def build_table(json_data):
    """
    Construye un DataFrame de pandas a partir de datos en formato JSON.

    Parámetros:
        * json_data (dict): Los datos en formato JSON obtenidos de una API.

    Retorna:
        * DataFrame: Un DataFrame de pandas que contiene los datos.
    """
    try:
        df = pd.json_normalize(json_data)
        return df
    except:
        print("Los datos no están en el formato esperado")
        return None
    

def build_table_incremental(json_data):
    """ 
     Construye un DataFrame de pandas a partir de datos en formato JSON.

     Parámetros:
         * json_data (dict): Los datos en formato JSON obtenidos de una API.

     Retorna:
         * DataFrame: Un DataFrame de pandas que contiene los datos junto a su fecha y hora de extraccion.
    """
    # Convertir los datos a un DataFrame de Pandas para su análisis
    df = pd.DataFrame(json_data)

    # Obtener la fecha y hora actuales
    fecha_hora = datetime.now()
    
    # Agregar la columna de fecha (solo la fecha sin la hora)
    df['fecha_consulta'] = fecha_hora.date()
    
    # Agregar la columna de hora (solo la hora sin la fecha)
    df['hora_consulta'] = fecha_hora.strftime('%H:%M:%S')  # Formato HH:MM:SS compatible con deltalake
    
    return df

def save_data_as_delta(df, path, mode="overwrite", partition_cols=None):
    """
    Guarda un dataframe en formato Delta Lake en la ruta especificada.
    A su vez, es capaz de particionar el dataframe por una o varias columnas.
    Por defecto, el modo de guardado es "overwrite".

    Parámetros:
      * df (pd.DataFrame): El dataframe a guardar.
      * path (str): La ruta donde se guardará el dataframe en formato Delta Lake.
      * mode (str): El modo de guardado. Son los modos que soporta la libreria
      * deltalake: "overwrite", "append", "error", "ignore".
      * partition_cols (list or str): La/s columna/s por las que se particionará el
      * dataframe. Si no se especifica, no se particionará.
    """
    write_deltalake(
        path, df, mode=mode, partition_by=partition_cols
    )


def save_new_data_as_delta(new_data, data_path, predicate, partition_cols=None):
    """
    Guarda solo nuevos datos en formato Delta Lake usando la operación MERGE,
    comparando los datos ya cargados con los datos que se desean almacenar
    asegurando que no se guarden registros duplicados.

    Parámetros:
      * new_data (pd.DataFrame): Los datos que se desean guardar.
      * data_path (str): La ruta donde se guardará el dataframe en formato Delta Lake.
      * predicate (str): La condición de predicado para la operación MERGE.
    """

    try:
      dt = DeltaTable(data_path)
      new_data_pa = pa.Table.from_pandas(new_data)
      # Se insertan en target, datos de source que no existen en target
      dt.merge(
          source=new_data_pa,
          source_alias="source",
          target_alias="target",
          predicate=predicate
      ) \
      .when_not_matched_insert_all() \
      .execute()

    # Si no existe la tabla Delta Lake, se guarda como nueva
    except TableNotFoundError:
      save_data_as_delta(new_data, data_path, partition_cols=partition_cols)


#############################################################################
############################## DATOS DE LA API ##############################
#############################################################################

# URL base de la API
base_url = "http://api.wmata.com/Bus.svc/json"

# Instanciar un ConfigParser, que se encargará de leer el archivo config.ini
parser = ConfigParser()

# parser.read("Extraccion en APIs/pipeline.conf")
parser.read("pipeline.conf")

api_credentials = parser["api-credentials"]

# key necesaria para acceder a la API y puesta en un encabezado
api_key = api_credentials["api_key"]
headers = {'api_key': api_key}


#############################################################################
#############################  EXTRACCION FULL  #############################
#############################################################################

# endpoint de las paradas de bus cercanas, si se omite parametros devuelve todas
endpointFull = "jStops"

# Creacion de JSON utilizando get_data
json_paradas_bus = get_data(base_url, endpointFull, data_field="Stops", headers=headers)

# Transfomacion de JSON a DataFrame utilizando build_table
dataFrameFull = build_table(json_paradas_bus)


##############################################################################
##########################  EXTRACCION INCREMENTAL  ##########################
##############################################################################

# endpoint de las posiciones de los autobuses actualmente
# Las posiciones se actualizan aproximadamente cada 20 a 30, de 7 a 10 segundos.
endpointIncremental = "jBusPositions"

# Creacion de JSON utilizando get_data
json_posiciones_bus = get_data(base_url, endpointIncremental, data_field="BusPositions", headers=headers)

# Transfomacion de JSON a DataFrame utilizando almacenar_datos
dataFrameIncremental = build_table_incremental(json_posiciones_bus)


#############################################################################
###########################  ALMACENAMIENTO FULL  ###########################
#############################################################################

# Direccion de la api
bronze_dir = "datalake/bronze/wmata_api"


# Direccion del endpoint dentro de la api
paradas_buses = f"{bronze_dir}/stop_Search"

# Almacena solo si hay cambios 
save_new_data_as_delta(dataFrameFull, paradas_buses, "target.StopID = source.StopID")


##############################################################################
########################  ALMACENAMIENTO INCREMENTAL  ########################
##############################################################################

# Direccion del endpoint dentro de la api
posiciones_buses = f"{bronze_dir}/bus_Position"

# Almacena y particiona por la fecha y hora de la extraccion
save_data_as_delta(dataFrameIncremental, posiciones_buses, partition_cols=["fecha_consulta","hora_consulta"])