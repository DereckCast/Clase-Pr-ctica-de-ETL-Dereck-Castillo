import logging
import pandas as pd
from sqlalchemy import create_engine, text
import Credentials as C

# Configuración del logging
logging.basicConfig(filename='etl.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

try:
    # Información de la conexión
    server = C.server
    database = C.database
    driver = 'ODBC Driver 17 for SQL Server'

    # Archivo que contiene la consulta SQL
    Diseases_query_file = 'Diseases.sql'

    # Crear la cadena de conexión
    connection_string = f'mssql+pyodbc://{server}/{database}?driver={driver}&trusted_connection=yes'
    engine = create_engine(connection_string)

    # Leer la consulta desde el archivo
    with open(Diseases_query_file, 'r') as file:
        query = file.read().strip()  # Eliminar espacios en blanco al inicio y al final

    # Ejecutar la consulta y obtener el DataFrame
    data_frame = pd.read_sql(query, engine)

    logging.info('Datos extraidos exitosamente de SQL Server.')
except Exception as e:
    logging.error(f'Error extrayendo datos: {e}')
    raise

# TRANSFORMACION
try:
    # Reemplazar valores NULL en la columna 'Response' con 'N/A'
    data_frame['Response'].fillna('N/A', inplace=True)
    logging.info('Valores NULL en la columna Response reemplazados por N/A.')

    # Reemplazar valores NULL en la columna 'DataValueFootnoteSymbol' con 'N/A'
    data_frame['DataValueFootnoteSymbol'].fillna('N/A', inplace=True)
    logging.info('Valores NULL en la columna DataValueFootnoteSymbol reemplazados por N/A.')

    # Reemplazar valores NULL en la columna '[DataValueFootnote]' con 'N/A'
    data_frame['DataValueFootnote'].fillna('N/A', inplace=True)
    logging.info('Valores NULL en la columna [DataValueFootnote] reemplazados por N/A.')

     # Reemplazar valores NULL en la columna '[Geolocation]' con 'N/A'
    data_frame['Geolocation'].fillna('N/A', inplace=True)
    logging.info('Valores NULL en la columna [Geolocation] reemplazados por N/A.')

    # Reemplazar valores NULL en las columnas 'DataValue', 'DataValueAlt', 'LowConfidenceLimit', y 'HighConfidenceLimit' con 0
    data_frame['DataValue'].fillna(0, inplace=True)
    data_frame['DataValueAlt'].fillna(0, inplace=True)
    data_frame['LowConfidenceLimit'].fillna(0, inplace=True)
    data_frame['HighConfidenceLimit'].fillna(0, inplace=True)

    # Convertir la columna 'HighConfidenceLimit' de varchar a float
    data_frame['HighConfidenceLimit'] = data_frame['HighConfidenceLimit'].astype(float)

 # Eliminar las columnas especificadas
    data_frame.drop(columns=[
        'DataValueFootnoteSymbol', 
        'StratificationCategory2', 'Stratification2', 
        'StratificationCategory3', 'Stratification3',
        'ResponseID', 'StratificationCategoryID2', 
        'StratificationID2', 'StratificationCategoryID3', 
        'StratificationID3'
    ], inplace=True)
except Exception as e:
    logging.error(f'Error transformando los datos: {e}')
    raise

#CARGAR DATOS

try:
    data_frame.to_sql('Chronic_Diseases_Modificadas', engine, if_exists='append', index=False)
    logging.info('Datos cargados exitosamente')

except Exception as e:
    logging.error(f'Error cargando los datos: {e}')
    raise

# CREAR LA BASE DE DATOS Y EL MODELO DIMENSIONAL
try:
    # Crear la base de datos DimensionalDB
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(text("CREATE DATABASE DimensionalDB"))
    logging.info('Base de datos DimensionalDB creada exitosamente.')

    connection_string_dimensional = f'mssql+pyodbc://{server}/DimensionalDB?driver={driver}&trusted_connection=yes'
    engine_dimensional = create_engine(connection_string_dimensional)

    # Leer y ejecutar el archivo SQL para crear el modelo dimensional
    create_dimensional_model_file = 'create_dimensional_model.sql'
    with open(create_dimensional_model_file, 'r') as file:
        create_dimensional_model_sql = file.read()

    with engine_dimensional.connect() as conn:
        conn.execute(text(create_dimensional_model_sql))
    logging.info('Modelo dimensional creado exitosamente.')

except Exception as e:
    logging.error(f'Error creando la base de datos o el modelo dimensional: {e}')
    raise

# CARGAR LOS DATOS AL MODELO DIMENSIONAL
try:
    query_modificada = 'SELECT * FROM Chronic_Diseases_Modificadas'
    df_modificada = pd.read_sql(query_modificada, engine)

    df_location = df_modificada[['LocationID', 'LocationAbbr', 'LocationDesc', 'Geolocation']].drop_duplicates()
    df_location.to_sql('DimLocation', engine_dimensional, if_exists='append', index=False)
    logging.info('Datos cargados exitosamente en DimLocation.')

    df_topic = df_modificada[['TopicID', 'Topic']].drop_duplicates()
    df_topic.to_sql('DimTopic', engine_dimensional, if_exists='append', index=False)
    logging.info('Datos cargados exitosamente en DimTopic.')

    df_question = df_modificada[['QuestionID', 'Question']].drop_duplicates()
    df_question.to_sql('DimQuestion', engine_dimensional, if_exists='append', index=False)
    logging.info('Datos cargados exitosamente en DimQuestion.')

    df_stratification = df_modificada[['StratificationID1', 'StratificationCategoryID1', 'StratificationCategory1', 'Stratification1']].drop_duplicates()
    df_stratification.to_sql('DimStratification', engine_dimensional, if_exists='append', index=False)
    logging.info('Datos cargados exitosamente en DimStratification.')

    df_data_value_type = df_modificada[['DataValueTypeID', 'DataValueType']].drop_duplicates()
    df_data_value_type.to_sql('DimDataValueType', engine_dimensional, if_exists='append', index=False)
    logging.info('Datos cargados exitosamente en DimDataValueType.')

    df_fact = df_modificada[['YearStart', 'YearEnd', 'DataValue', 'DataValueAlt', 'LowConfidenceLimit', 'HighConfidenceLimit', 'DataValueUnit', 'DataValueFootnote', 'Response', 'LocationID', 'TopicID', 'QuestionID', 'StratificationID1', 'DataValueTypeID']]
    df_fact.to_sql('FactChronicDiseases', engine_dimensional, if_exists='append', index=False)
    logging.info('Datos cargados exitosamente en FactChronicDiseases.')

except Exception as e:
    logging.error(f'Error cargando los datos en el modelo dimensional: {e}')
    raise