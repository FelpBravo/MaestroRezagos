
import config.config as constant
from config.log import logger

from datetime import datetime
import os
import pandas as pd
import csv
import numpy as np
import traceback
 
pd.set_option('display.max_colwidth', None)

ERROR_03 = 'CARACTERES INVALIDOS;'
ERROR_04 = 'LARGO DEL REGISTRO NO VALIDO;'
ERROR_05 = 'FORMATO ID REZAGO NO NUMERICO;'
ERROR_06 = 'LARGO DE ID REZAGO NO VALIDO;'
ERROR_07 = 'ESTADO DEL REZAGO NO VALIDO; '

def dividir_descripcion_error(desc_error):
    valor= ''
    if desc_error:
        for n in range(1, len(desc_error)+1):
            if n % 2 == 0:
                valor += desc_error[n:n+2] + ';'
            if n == 1:
                valor += desc_error[0:n+1] + ';'
        valor = valor.rstrip(';')
    return valor

def validar_contenido(fullLocalPath, fileName='', s=''):
    logger(f'\t\t\t\t[VALIDAR CONTENIDO] Iniciando validación de contenido del archivo {fileName}')
    
    df= None
    df_valid= None
    df_invalid= None
    total = ''
    data= []
    informacion= {
        'Log': 'Sin información',
        'Excepcion' : '', 
        }
   
    try:
        with open(fullLocalPath, encoding='utf-8', errors='ignore') as f:
            count=0
            for line in f:
                if count > 0:
                    data.append(str(line.replace('\n','')))
                    # if constant.QA:                  #ELIMINAR
                    #     s = int(s) + 1           #ELIMINAR
                    #     s = str(s).zfill(25) #ELIMINAR                         
                    #     data.append(str(line.replace('\n','')) + s) #ELIMINAR
                    # else:
                    #     data.append(str(line.replace('\n','')))
                count += 1
            
        df= pd.DataFrame(data, columns=['Column_name'])
        df['Column_name'] = df['Column_name'].str.replace('\t', ' ')
        del data 
        df.index= df.index + 2
        if df is not None and len(df) > 0:
            #...Crear un diccionario que contenga el inicio y fin de cada columna
            columns = {}
            start = 0
            for column_name, length in constant.lengths.items():
                end = start + length
                columns[column_name] = (start, end)
                start = end
            #...Crear nuevas columnas usando str.slice()
            for column_name, (start, end) in columns.items():
                df[column_name] = df['Column_name'].str.slice(start, end)
            
            #...Validaciones y creación de columnas.
            df['longitud_total'] = df['Column_name'].str.len()
            df['nro_linea'] = df.index
            df['error_caracteres'] = np.where(df['Column_name'].str.contains(r'\\|;'), ERROR_03, '') #.. Código error 03
            df['error_longitud'] = np.where(df['Column_name'].str.len() == 277, '', ERROR_04) #.. Código error 04
            df['error_no_numerico_id_rzg_unico'] = np.where(df['error_longitud'] == '', np.where(df['id_rzg_unico'].str.isdigit(), '', ERROR_05 ), '') #.. Código error 05
            df['error_largo_id_rzg_unico'] = np.where((df['error_longitud'] == '') & (df['id_rzg_unico'].str.len() == constant.lengths['id_rzg_unico']), '', ERROR_06 ) #.. Código error 06
            df['error_estado_rezago'] = np.where((df['estado_rezago']== 'G') | (df['estado_rezago']== 'R'), '', ERROR_07 ) #.. Código error 07
            df['desc_error'] = df['error_caracteres']+df['error_longitud']+df['error_no_numerico_id_rzg_unico']+df['error_largo_id_rzg_unico']+df['error_estado_rezago']
            df['desc_error'] = df['desc_error'].str.strip() #.apply(lambda x: dividir_descripcion_error(x))
            df['registro_valido'] = np.where(df['desc_error'] == '', True, False) 
            
            #...Separando registros validos y no validos.
            df_valid = df[df['registro_valido']].reset_index(drop=True)
            df_invalid = df[~df['registro_valido']].reset_index(drop=True)
            
            total= str(len(df))
            
            informacion['Log'] = f'Validaciones terminadas correctamente. |Totales:{total}|Validos:{str(len(df_valid))}|No Validos:{str(len(df_invalid))}|'
            del df
            
        else:
            informacion['Log'] = f'Largo de dataframe no valido para ingresar a las validaciones [Largo: {str(len(df))}].'
    
    except Exception as e:
        informacion['Excepcion']= "__EXCEPTION_FILE: " + str(e) + str(traceback.format_exc(limit=1))
        informacion['Log'] = informacion['Excepcion']
        
    finally:
        logger('\t\t\t\t\t' + str(informacion['Log']))
        
        return total, df_valid, df_invalid, informacion['Log']
    
def guardar_salida_local(df, cod_ins_super, ext, type, fileName=''):
    informacion= {
        'Log': 'Sin información',
        'Excepcion' : '', 
        }
    output_name = ''
    try:
        
        if type:
            logger(f'\t\t\t\t[REGISTROS VALIDOS] Guardado de salida local.')
            output_name = constant.LOCAL_OUTPUT_PATH.format(cod_ins_super = cod_ins_super) + fileName
            df['fecha_validacion'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df['fecha_carga'] = ''
            df['cod_afp'] = cod_ins_super
            columns = list(constant.lengths.keys()) + ['fecha_validacion', 'fecha_carga' , 'cod_afp']
            df = df[columns]
            
            df.to_csv(output_name, sep = '\t', index=False, quoting=csv.QUOTE_NONE, quotechar='', escapechar='\\')
            
        else:
            logger(f'\t\t\t\t[REGISTROS NO VALIDOS] Guardado de salida local.')
            output_name = constant.LOCAL_OUTPUT_PATH.format(cod_ins_super = cod_ins_super) + constant.OUTPUT_NOK_FILE.format(extafp = ext)
            df = df.rename(columns={
                'nro_linea': 'Nro. Linea',
                'Column_name': 'Información del Registro', 
                'desc_error': 'Descripción del Error'
                })
            df = df[['Nro. Linea','Información del Registro', 'Descripción del Error']]
            
            df.to_csv(output_name, sep = '\t', index=False, quoting=csv.QUOTE_NONE, quotechar='', escapechar='\\')
        
        informacion['Log'] = f'Archivo {output_name} copiado en el local.'
        
    except Exception as e:
        informacion['Excepcion'] = "EXCEPTION_FILE: " + str(e) + str(traceback.format_exc(limit = 1))
        informacion['Log'] = informacion['Excepcion']
        
    finally:
        logger('\t\t\t\t\t' + str(informacion['Log']))
        
        return informacion['Log']
    
def obtener_df_validos(ruta, parametro):
    logger(f'\t\t\t\t[OBTENER DF VALIDOS] Buscando archivos validos en ruta {ruta}.')
    
    try:
        df= None
        df_concatenados= []
        informacion= {
            'Log': 'Sin información',
            'Excepcion' : '', 
        }
        
        lista_archivos = os.listdir(ruta)
        archivos_validos= []
        ultimo_archivo_del_dia = ''
        hora_archivo = []
        
        if len(lista_archivos) > 0:
            for f in lista_archivos:
                if str(f).startswith(parametro):
                    archivos_validos.append(f)
                    hora_archivo.append(int(f[13:17])) #... Esto es para agregar las horas y luego obtener el maximo
                else:
                    informacion['Log'] = 'No se encuentran registros validos a rescatar.'
                    
            if len(archivos_validos) > 0 and len(hora_archivo) > 0 :
                #... Ejemplo crez20230411_1332 
                for a in archivos_validos:
                    if parametro  + '_' + str(max(hora_archivo)).zfill(4) in a:
                        ultimo_archivo_del_dia = a
                        df = pd.read_csv(ruta + a, sep='\t', dtype=str)
                        break
                    
                # for a in archivos_validos:
                #     df = pd.read_csv(ruta + a, sep='\t', dtype=str)
                #     df_concatenados.append(df)
                # df = pd.concat(df_concatenados, ignore_index=True, axis=0)
                
                #df = df.drop_duplicates(subset=['id_rzg_unico'])
                df['fecha_carga'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                informacion['Log'] = f'[OK] {str(len(df))} Registros obtenidos desde ultimo archivo cargado "{str(ultimo_archivo_del_dia)}".'
                
        else:
            informacion['Log'] = 'El directorio no contiene archivos.'

    except Exception as e:
        informacion['Excepcion'] = "EXCEPTION_FILE: " + str(e) + str(traceback.format_exc(limit = 1))
        informacion['Log'] = informacion['Excepcion']
    finally:
        logger('\t\t\t\t\t' + str(informacion['Log']))
        
        return df, ultimo_archivo_del_dia, informacion['Log']
    
def guardar_consolidado_local(df, cod_ins_super):
    logger(f'\t\t\t\t[DEPOSITAR CONSOLIDADO] Depositar archivo consolidado en casilla SFTP.')
    
    try:
        informacion= {
            'Log': 'Sin información',
            'Excepcion' : '', 
        }
        ruta_consolidado_local= constant.LOCAL_OUTPUT_PATH.format(cod_ins_super = cod_ins_super) + constant.OUTPUT_CONSOLIDADO_FILE     
        
        df.sort_values(by=['rut_afiliado'], inplace= True)         
        df.to_csv(ruta_consolidado_local, sep=';', index=False, quoting=csv.QUOTE_NONE, quotechar='', escapechar='\\')  
         
        informacion['Log'] = f'[OK] Archivo consolidado guardado en local. [{ruta_consolidado_local}]'

    except Exception as e:
        informacion['Excepcion'] = "EXCEPTION_FILE: " + str(e) + str(traceback.format_exc(limit = 1))
        informacion['Log'] = informacion['Excepcion']
    finally:
        logger('\t\t\t\t\t' + str(informacion['Log']))
        
        return informacion['Log']
    