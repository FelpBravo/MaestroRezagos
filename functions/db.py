from ast import Constant
import config.config as constant 
from config.log import logger

from sqlalchemy import create_engine
import pymssql 
import pandas
import traceback

def connect_alchemy(df, table = constant.TBL_MAESTRO_REZAGO):
    logger(f'\t\t\t\t[INSERTAR REGISTROS] Iniciando conexión a la base de datos.')
    
    informacion= {
        'Log': 'Sin información',
        'Excepcion' : '', 
        }
    
    try:
        logger(f'\t\t\t\t\tCreando conexión a [{constant.SERVER_GESTION}]-[{constant.BD_INTEGRA}] con sqlalchemy...')
        engine = create_engine(f'mssql+pymssql://{constant.UID}:{constant.PWD}@{constant.SERVER_GESTION}/{constant.BD_INTEGRA}')
        
        logger(f'\t\t\t\t\tInsertando en tabla ...')
        df.to_sql(table, con=engine, if_exists='append', schema=constant.SCHEMA_MAE, index=False, method="multi",chunksize=100,)

        informacion['Log'] = f'[OK] {str(len(df))} Registros cargados exitosamente.'
        
        engine.dispose()
        
    except Exception as e:
        informacion['Excepcion'] = "EXCEPTION_DB_ALCHEMY: " + str(e) + str(traceback.format_exc(limit=1))
        informacion['Log'] = informacion['Excepcion']
        
    finally:
        logger('\t\t\t\t\t' + str(informacion['Log']))
        return informacion['Log']


def connect_pymssql(server, data_base, query=None, type=None, variable=False):
    logger(f'\t\t\t\t[OBTENER TABLA BD] Iniciando conexión a la base de datos.')
    conn= None
    df= None
    resp = None
    bitacora = False
    informacion= {
        'Log': 'Sin información',
        'Excepcion' : '', 
        }
    try:
        
        logger(f'\t\t\t\t\tCreando conexión a [{constant.SERVER_GESTION}]-[{constant.BD_INTEGRA}] con pymssql...')
        conn = pymssql.connect(server, constant.UID, constant.PWD, data_base)
        cursor = conn.cursor()
       
        if type == 'pandas' and variable is False: 
            if str(query).upper().startswith('EXEC') or 'EXEC cuadre.' in str(query):
                if str(query).upper().startswith('EXEC'):
                    logger(f'\t\t\t\t\t({server}-{data_base}) Enviando SP con pandas...   \n \t\t\t\t\t\t\t\t\t\t{query}')
                    dfs = []
                    for chunk in pandas.read_sql_query(query, conn, chunksize=100):
                        dfs.append(chunk) 
                    df = pandas.concat(dfs, ignore_index=True)
                    
                else:
                    logger(f'\t\t\t\t\t({server}-{data_base}) Enviando SP con pandas...   \n \t\t\t\t\t\t\t\t\t\t{query}')
                    df = pandas.read_sql_query(query, conn) 
                    
            else:
                logger(f'\t\t\t\t\t({server}-{data_base}) Enviando consulta SQL con pandas...   \n \t\t\t\t\t\t\t\t\t\t{query}')
                df = pandas.read_sql_query(open(file=query).read(), conn) 
                
            if bitacora is False and (df is not None and not df.empty):   
                informacion['Log'] = f'[OK] "{str(len(df))}" Registros obtenidos correctamente.'
            else:
                informacion['Log'] = f'[NOK] No se encuentran registros dentro de la base de datos.'
                
        elif type == 'pandas' and variable:
            if variable:
                logger(f'\t\t\t\t\t({server}-{data_base}) Enviando query para obtener datos de calendario...   \n \t\t\t\t\t\t\t\t\t\t')
                df = pandas.read_sql_query(query, conn)
            if df is not None and not df.empty:   
                informacion['Log'] = f'[OK] "{str(len(df))}" Registros obtenidos correctamente.'
            else:
                informacion['Log'] = f'[NOK] No se encuentran registros dentro de la base de datos.'
                 
        else:
            cursor.execute(query)
            if cursor.description is not None:
                resp = list(cursor.fetchall()[0])[0] #Ejemplo salida fetchall [(3263,)]
                conn.commit()    
                bitacora = True
            else: 
                conn.commit()
                bitacora = True
            if bitacora:
                informacion['Log'] = f'[BITÁCORA OK] SP Bitácora ejecutado correctamente.'
            else:
                informacion['Log'] = f'[BITÁCORA NOK] SP Bitácora finalizado con error.'

    except Exception as e:
        informacion['Excepcion'] = "EXCEPTION_DB_PYMSSQL: " + str(e) + str(traceback.format_exc(limit=1))
        informacion['Log'] = informacion['Excepcion']
        
    finally:
        conn.close()
        cursor.close()  
        logger('\t\t\t\t\t' + str(informacion['Log']))
        resp = resp if bitacora else df
    
        return resp, informacion['Log']