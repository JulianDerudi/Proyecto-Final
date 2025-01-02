import pyarrow as pa
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError

import ast


#############################################################################
###############################  FUNCIONES  #################################
#############################################################################

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


def explode_columns(df_origin, cols_to_select, cols_to_explode):
    """
    Hacer un "Explode" de columnas que contienen listas en filas separadas.

    Parametros:
        * df_origin (pd.DataFrame): El DataFrame original.
        * cols_to_select (list): Lista de columnas a seleccionar del DF original.
        * cols_to_explode (list): Lista de columnas que contienen listas y se van a hacer explode.

    Retorna:
        * pd.DataFrame: Un nuevo DataFrame con las columnas seleccionadas y las columnas especificadas explotadas.
    """
    try:
        # Seleccionar las columnas del DataFrame original
        df_result = df_origin[cols_to_select].copy()

        # Iterar sobre las columnas que deben ser explotadas
        for col_to_explode in cols_to_explode:
            # Verificar si la columna ya tiene listas, y hacer explode
            if df_result[col_to_explode].apply(lambda x: isinstance(x, list)).all():
                df_result = df_result.explode(col_to_explode)
            else:
                print(f"Advertencia: La columna {col_to_explode} no contiene listas. No se puede hacer explode.")
                continue

        return df_result
    except KeyError as e:
        print(f"Algunas columnas no encontradas en el DataFrame: {e}")
        return None
    except Exception as e:
        print(f"Ha ocurrido un error: {e}")
        return None

##############################################################################
###############################  PROCESAMIENTO  ##############################
##############################################################################

# Direccion de la api
bronze_dir = "datalake/bronze/wmata_api"

# Direccion del endpoint dentro de la api
paradas_buses = f"{bronze_dir}/stop_Search"

# Direccion del endpoint dentro de la api
posiciones_buses = f"{bronze_dir}/bus_Position"


#####  EXTRACCION DE LOS DATOS DE LA CAPA BRONZE PASADOS A DATAFRAMES PARA SU PROCESAMIENTO  #####
dataFrameFull = DeltaTable(paradas_buses).to_pandas()

dataFrameIncremental = DeltaTable(posiciones_buses).to_pandas()


############################
#### TIPO DE DATOS NULL ####

# dataFrameIncremental puede llegar a tener valores NULL en BlockNumber
imputation_mapping = {"BlockNumber" : "Null"}

# Reemplaza los nulos por los valores del diccionario
dataFrameIncremental = dataFrameIncremental.fillna(imputation_mapping) 


###################################
#### TIPO DE DATOS CATEGORICOS ####

# dataFrameFull no tiene una columna categorica
# dataFrameIncremental tiene "DirectionText"


#####################################
#### CONVERSION DE TIPO DE DATOS ####

#### FULL ####
# Columnas a cambiar su tipo
conversion_mapping_full = {
    # "Name" : "string",  #???
    "StopID" : "int32"
}

dataFrameFull = dataFrameFull.astype(conversion_mapping_full)


#### INCREMENTAL ####
# Columnas a cambiar su tipo
conversion_mapping_incremental = { 
    # "Deviation" : "int8",    #???
    # "fecha_consulta" : "",   #???
    # "hora_consulta" : "",    #???
    "VehicleID" : "int16",
    "TripID" : "int32",
    "DirectionNum" : "int8",
    "DateTime" : "datetime64[ns]",    
    "TripStartTime" : "datetime64[ns]",
    "TripEndTime" : "datetime64[ns]",
    "DirectionText" : "category"   # tipo de datos categorico
}

dataFrameIncremental = dataFrameIncremental.astype(conversion_mapping_incremental)


##############################
#### MANEJO DE DUPLICADOS ####

# filtramos duplicados por StopID
dataFrameFull = dataFrameFull.drop_duplicates(subset="StopID", keep="first").copy()

# filtramos duplicados por VehicleID
dataFrameIncremental = dataFrameIncremental.drop_duplicates(subset="VehicleID", keep="first").copy()


############################
#### RENOMBRAR COLUMNAS ####

# Renombramos columna Name para que quede mas clara
dataFrameFull = dataFrameFull.rename(columns={"Name": "Stop_Name"})


############################
#### NORMALIZAR COLUMNA ####

 
# Creamos otro dataFrame con la "StopID" y "Routes" para evitar una columna con atributos multivaluados

# Convertir las cadenas de texto a listas reales
dataFrameFull["Routes"] = dataFrameFull["Routes"].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)

# Nuevo dataFrame con columna "Routes" con valores atomicos
dataFrameFull_Routes = explode_columns(dataFrameFull, ["StopID", "Routes"], ["Routes"])

# Borramos la columna Routes del DF original
dataFrameFull = dataFrameFull.drop(columns=["Routes"])


#############################################################################
##########################  ALMACENAMIENTO Silver  ##########################
#############################################################################

# Direccion de la api
silver_dir = "datalake/silver/wmata_api"


#############################################
# Direccion del endpoint dentro de la api
paradas_buses = f"{silver_dir}/stop_Search"

# Almacena solo si hay cambios 
save_new_data_as_delta(dataFrameFull, paradas_buses, "target.StopID = source.StopID")


#############################################
# Direccion del endpoint dentro de la api
posiciones_buses = f"{silver_dir}/bus_Position"

# Almacena y particiona por la fecha y hora de la extraccion
save_data_as_delta(dataFrameIncremental, posiciones_buses, partition_cols=["fecha_consulta","hora_consulta"])


#############################################
# Direccion del endpoint dentro de la api
router_buses = f"{silver_dir}/stop_Search_Routes"

# Almacena solo si hay cambios 
save_new_data_as_delta(dataFrameFull_Routes, router_buses, "target.StopID = source.StopID AND target.Routes = source.Routes")

