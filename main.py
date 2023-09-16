
import functions.mail as mail
import functions.db as db
import functions.sftp as sftp
import functions.file as file
import functions.bitacora as bitacora
import config.config as constant 
from config.log import logger
import os
import pandas as pd
import sys
import warnings
import traceback
import random
import datetime as dt
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')
pd.set_option('display.max_colwidth', None)

FECHA_OFICIAL_ACTUAL = dt.date(2023, 5, 26) if constant.QA else dt.date.today() #dt.date(2023, 4, 10)   
SEMANA= 0 if constant.QA else 1  #...  0:'Semana Actual'  -  1:'Semana Anterior'
PARAMETRO_BUSQUEDA = 'crez' + constant.YYYYMMDD 
PARAMETRO_VALIDACION = 'proceso_validacion'
PARAMETRO_CARGA_DATOS = 'proceso_carga_datos'
PARAMETRO_CONSOLIDADO = 'proceso_consolidado'

ID_PROCESO_PADRE = '10600'
ID_PROCESO_NOMBRE_ARCHIVO = '10601'
ID_PROCESO_CONTENIDO_ARCHIVO = '10602'
ID_PROCESO_CARGA = '10603'
ID_PROCESO_CONSOLIDADO = '10604'
FIN_SCRIPT_OK = '::El script a finalizado correctamente::.'
PARAMETRO_VALIDAR_EJECUCION = FECHA_OFICIAL_ACTUAL.strftime('%Y%m%d')
PARAMETRO_VALIDAR_EJECUCION_BITACORA = dt.date.today().strftime('%Y%m%d')
fecha_semana_pasada = FECHA_OFICIAL_ACTUAL - dt.timedelta(weeks=SEMANA) 
dia_semana_pasada = fecha_semana_pasada.weekday()
fecha_lunes_pasado = fecha_semana_pasada - dt.timedelta(days=dia_semana_pasada)
fecha_domingo_pasado = fecha_lunes_pasado + dt.timedelta(days=6)
log = ''
fecha_consolidado = datetime.now().strftime('%Y%m%d')
fecha_ejecucion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
fileName = ''
glosa_inst = ''
dfProceso = None
dfContactos = None
df_valid = ''
df_invalid = ''

def notificar_excepcion(cod='', glosa='', msj='', dfContactos= None):
    logger(msj)
    mail.send('EXCEPTION', cod, glosa, msg = msj, dfContactos = dfContactos)
    logger('.::El script se a detenido debido a un error::.')

def obtener_semana_actual(date):
    lunes = date - timedelta(days=date.weekday())
    dias_semana = []
    for d in range(5):
        dia = lunes + timedelta(days = d)
        dias_semana.append(dia.strftime("%Y%m%d"))
        
    return ','.join(dias_semana)

dfCalendario, log= db.connect_pymssql(
    constant.SERVER_GESTION, 
    constant.BD_INTEGRA, 
    constant.CONSULTA_CALENDARIO.format(lista_dias_semana = obtener_semana_actual(FECHA_OFICIAL_ACTUAL)), 
    type='pandas', 
    variable=True,
    )

dfViernesPasado, log= db.connect_pymssql(
    constant.SERVER_GESTION, 
    constant.BD_INTEGRA, 
    constant.CONSULTA_CALENDARIO_VIERNES_PASADO.format(lunes = fecha_lunes_pasado.strftime('%Y%m%d'), domingo = fecha_domingo_pasado.strftime('%Y%m%d')), 
    type='pandas', 
    variable=True,
    )

dfViernesPasado, log= db.connect_pymssql(
    constant.SERVER_GESTION, 
    constant.BD_INTEGRA, 
    constant.CONSULTA_CALENDARIO_VIERNES_PASADO.format(lunes = fecha_lunes_pasado.strftime('%Y%m%d'), domingo = fecha_domingo_pasado.strftime('%Y%m%d')), 
    type='pandas', 
    variable=True,
    )

DF_VALIDAR_PRIMERA_EJECUCION, log = db.connect_pymssql(
    constant.SERVER_GESTION, constant.BD_INTEGRA, constant.SPLST_VALIDAR_EJECUCION_MAESTRO_REZAGOS.format(fecha_ejecucion=PARAMETRO_VALIDAR_EJECUCION_BITACORA), type='pandas')

CERCANO_A_LUNES = str(dfCalendario['mas_cercano_a_lunes'][0])
CERCANO_A_VIERNES = str(dfCalendario['mas_cercano_a_viernes'][0])
CERCANO_A_VIERNES_PASADO = str(dfViernesPasado['mas_cercano_a_viernes'][0])

print(datetime.strptime(CERCANO_A_VIERNES_PASADO, '%Y%m%d').date().strftime('%d-%m'))
print(FECHA_OFICIAL_ACTUAL.strftime('%d-%m'))

if len(sys.argv) > 1:
    
    logger('::Inicio del script con parámetro ' + str(sys.argv[1]) + '::.')
    
    if CERCANO_A_VIERNES == PARAMETRO_VALIDAR_EJECUCION and (sys.argv[1] == PARAMETRO_VALIDACION or sys.argv[1] == PARAMETRO_CARGA_DATOS):
        logger(f'::[VALIDACIÓN Y CARGA] Día habil para ejecutar')
        #...Proceso de validación de archivos, contenidos, guardado de archivos y notificaciones.  
        if sys.argv[1] == PARAMETRO_VALIDACION:
            try:
                id_ins_general, _ = bitacora.registrar(ID_PROCESO_PADRE, 'start')
                
                #... Conexión base de datos para recuperar información de instituciones ....
                dfProceso, log= db.connect_pymssql(constant.SERVER_GESTION, constant.BD_INTEGRA, constant.script_01, type='pandas')
                dfContactos, log= db.connect_pymssql(constant.SERVER_GESTION, constant.BD_INTEGRA, constant.script_02, type='pandas')
                
                if not str(log).startswith('EXCEPTION'):
                    dfProceso[
                        ['validacionArchivo','ExcepcionValidacionArchivo','rutaLocal',
                        'validacionContenido','ExcepcionValidacionContenido', 'nombreArchivoEntrada']]= ''
                    
                    #... Validación del nombre del archivo y notificación ....
                    diccionario= dict(zip(dfProceso['cod_ins_super'].apply(lambda x: str(x).strip()), dfProceso['ext_archivo'].apply(lambda x: str(x).strip())))
                    for cod_ins_super, ext_archivo in diccionario.items():
                        
                        id_ins_validacion, _ = bitacora.registrar(ID_PROCESO_NOMBRE_ARCHIVO, 'start')
                        glosa_inst= str(dfProceso.loc[dfProceso["cod_ins_super"] == cod_ins_super, "glosa"].iloc[0])
                
                        logger(f'01__[{cod_ins_super}]__[{glosa_inst}]____VALIDACIÓN NOMBRE ARCHIVO Y NOTIFICACIONES. (id_instancia={id_ins_validacion})')
                            
                        if ext_archivo:
                            fullLocalPath, nombreArchivo, log= sftp.connect('ENTRADA_ARCHIVO_REZ', cod_ins_super, ext_archivo)
                
                            if str(log).startswith('[NOT]'):
                                dfProceso.loc[dfProceso['cod_ins_super']==cod_ins_super, 'validacionArchivo'] = str(log)
                                mail.send(
                                    cod_ins_super = cod_ins_super,
                                    type = 'ARCHIVO_REZ_NO_ENCONTRADO', 
                                    glosa = glosa_inst, 
                                    msg = str(log)[6:],
                                    dfContactos = dfContactos,
                                    df_validar_ejecucion = DF_VALIDAR_PRIMERA_EJECUCION
                                    )
                                
                            elif str(log).startswith('[NOK]'):
                                dfProceso.loc[dfProceso['cod_ins_super']==cod_ins_super, 'validacionArchivo'] = str(log)
                                mail.send(
                                    type = 'FORMATO_NOMBRE_ARCHIVO_REZ_INCORRECTO', 
                                    nombre_archivo = nombreArchivo,
                                    cod_ins_super = cod_ins_super, 
                                    glosa = glosa_inst, 
                                    ext = ext_archivo, 
                                    msg = str(log)[6:],
                                    fecha_ejecucion = fecha_ejecucion,
                                    dfContactos = dfContactos
                                    )
                                
                            elif str(log).startswith('EXCEPTION'):
                                dfProceso.loc[dfProceso['cod_ins_super']==cod_ins_super, 'ExcepcionValidacionArchivo'] = str(log)  
                                mail.send(
                                    type = 'EXCEPTION', 
                                    cod_ins_super = cod_ins_super, 
                                    glosa = glosa_inst, 
                                    msg = str(log),
                                    dfContactos = dfContactos
                                    )
                            if fullLocalPath:
                                dfProceso.loc[dfProceso['cod_ins_super'] == cod_ins_super, 'rutaLocal'] =  str(fullLocalPath) 
                                dfProceso.loc[dfProceso['cod_ins_super'] == cod_ins_super, 'nombreArchivoEntrada'] =  str(nombreArchivo) 
                                
                            bitacora.registrar(
                                id = ID_PROCESO_NOMBRE_ARCHIVO,
                                estado = 1 if not str(log).startswith('EXCEPTION') else 0,
                                instance_id = id_ins_validacion,
                                log= log,
                                nombre_archivo_ok= nombreArchivo,
                                ruta_sftp= str(constant.REMOTE_OUTPUT_PATH).format(cod_ins_super= cod_ins_super)
                                )
                                
                        else:
                            logger(f'\t\t\t\t__ADVERTENCIA__La institución no contiene una extensión dentro de la base de datos.')
                            
                    #... Validación del contenido del archivo ....    
                    diccionario= dict(zip(dfProceso['cod_ins_super'].apply(lambda x: str(x).strip()), dfProceso['rutaLocal'].apply(lambda x: str(x).strip())))      
                        
                    for cod_ins_super, rutaLocal in diccionario.items():
                        
                        id_ins_validacion, _ = bitacora.registrar(ID_PROCESO_CONTENIDO_ARCHIVO, 'start')
                        glosa = str(dfProceso.loc[dfProceso["cod_ins_super"] == cod_ins_super, "glosa"].iloc[0])
                        ext_archivo = str(dfProceso.loc[dfProceso["cod_ins_super"] == cod_ins_super, "ext_archivo"].iloc[0])
                        
                        logger(f'02__[{cod_ins_super}]__[{glosa}]____VALIDACIÓN CONTENIDO ARCHIVO, SALIDAS SFTP E INFORME. (id_instancia={id_ins_validacion})')
                        
                        if rutaLocal:
                            fileName = str(dfProceso.loc[dfProceso["cod_ins_super"] == cod_ins_super, "nombreArchivoEntrada"].iloc[0])
                            total, df_valid, df_invalid, log = file.validar_contenido(rutaLocal, fileName)#, s = '0000000000000000000' + str(random.randrange(100000, 300000)))
                            
                            if df_valid is not None and not df_valid.empty:
                                log = file.guardar_salida_local(df_valid, cod_ins_super, ext_archivo, True, fileName)    
                                
                                if str(log).startswith('EXCEPTION'):
                                    notificar_excepcion(cod_ins_super, glosa, log, dfContactos)
                                
                            if df_invalid is not None and not df_invalid.empty:
                                log = file.guardar_salida_local(df_invalid, cod_ins_super, ext_archivo, False)
                                
                                if not str(log).startswith('EXCEPTION'):
                                    _, nombreArchivo, log = sftp.connect('SALIDA_ARCHIVO_REGISTROS_NO_VALIDOS', cod_ins_super, ext_archivo)
                                    if str(log).startswith('EXCEPTION'):
                                        notificar_excepcion(cod_ins_super, glosa, log,dfContactos)
                                    
                                else:
                                    notificar_excepcion(cod_ins_super, glosa, log, dfContactos)
                                    
                            if not str(log).startswith('EXCEPTION'):
                                cant_leidos= str( int(len(df_valid)) + int(len(df_invalid)) )
                                cant_validos= str(len(df_valid))
                                cant_rechazados= str(len(df_invalid))
                                local_input_path = constant.LOCAL_INPUT_FULL_PATH.format(cod_ins_super=cod_ins_super) + fileName
                                local_proccesed_path = constant.LOCAL_PROCESSED_FULL_PATH.format(cod_ins_super=cod_ins_super) + fileName
                                mail.send(
                                    type = 'INFORME_VALIDACION', 
                                    ext =             ext_archivo,
                                    cod_ins_super =   cod_ins_super, 
                                    glosa =           glosa,
                                    nombre_archivo =  fileName,
                                    cant_enviados =   str(format(int(total), ',')).replace(',', '.'),
                                    cant_leidos =     str(format(int(cant_leidos), ',')).replace(',', '.'),
                                    cant_validos =    str(format(int(cant_validos), ',')).replace(',', '.'),
                                    cant_rechazados = str(format(int(cant_rechazados), ',')).replace(',', '.'),
                                    fecha_ejecucion = fecha_ejecucion,
                                    dfContactos =     dfContactos
                                    )
                                os.rename(local_input_path, local_proccesed_path)
                                
                            else:
                                notificar_excepcion(cod_ins_super, glosa, log, dfContactos)
                        
                        else:
                            logger(f'\t\t\t\t__ADVERTENCIA__No existe una ruta local por procesar para esta institución.')
                        
                        bitacora.registrar(
                            id = ID_PROCESO_CONTENIDO_ARCHIVO,
                            estado = 1 if not str(log).startswith('EXCEPTION') else 0,
                            instance_id = id_ins_validacion,
                            log= log if rutaLocal else '[ADVERTENCIA] No existe una ruta local por procesar para esta institución.',
                            nombre_archivo_nok= fileName,
                            ruta_sftp= str(constant.REMOTE_OUTPUT_PATH).format(cod_ins_super= cod_ins_super),
                            cant_total= str( int(len(df_valid)) + int(len(df_invalid)) ),
                            cant_ok= str(len(df_valid)),
                            cant_err= str(len(df_invalid)),
                            )
                    
                    bitacora.registrar(id = ID_PROCESO_PADRE, estado = 1, instance_id = id_ins_general)
                    logger(FIN_SCRIPT_OK)
                    
                else:
                    bitacora.registrar(id = ID_PROCESO_PADRE, estado = 0, instance_id = id_ins_general, log= log)
                    notificar_excepcion('ERROR GENÉRICO', 'ERROR GENÉRICO', log, dfContactos)
                            
            except Exception as e:
                log= str(e) + str(traceback.format_exc(limit=1))
                bitacora.registrar(id = ID_PROCESO_PADRE,estado = 0, instance_id = id_ins_general, log= log)
                notificar_excepcion('ERROR GENÉRICO', 'ERROR GENÉRICO', log, dfContactos)
        
        #...Proceso de carga de datos validos dentro de la base de datos.        
        elif sys.argv[1] == PARAMETRO_CARGA_DATOS:
            try:
                id_ins_general, _ = bitacora.registrar(ID_PROCESO_PADRE, 'start')
                dfProceso, log= db.connect_pymssql(constant.SERVER_GESTION, constant.BD_INTEGRA, constant.script_01, type='pandas')
                dfContactos, log= db.connect_pymssql(constant.SERVER_GESTION, constant.BD_INTEGRA, constant.script_02, type='pandas')
                
                if not str(log).startswith('EXCEPTION'):
                    diccionario= dict(zip(dfProceso['cod_ins_super'].apply(lambda x: str(x).strip()), dfProceso['glosa'].apply(lambda x: str(x).strip())))  
                    
                    for cod_ins_super, glosa in diccionario.items():
                        
                        id_ins_carga, _ = bitacora.registrar(ID_PROCESO_CARGA, 'start')
                        logger(f'01__[{cod_ins_super}]__[{glosa}]____PROCESO DE CARGA DE DATOS VALIDOS EN BASE DE DATOS. (id_instancia={id_ins_carga})')
                        ruta_salida_local = constant.LOCAL_OUTPUT_PATH.format(cod_ins_super=cod_ins_super)     
                        df_valid, archivos_validos, log = file.obtener_df_validos(ruta_salida_local, PARAMETRO_BUSQUEDA)
                        
                        if not str(log).startswith('EXCEPTION'):
                            if df_valid is not None and not df_valid.empty:
                                log = db.connect_alchemy(df_valid)
                                if str(log).startswith('EXCEPTION'):
                                    bitacora.registrar(id = ID_PROCESO_CARGA, estado = 0, instance_id = id_ins_carga, log= log)
                                    notificar_excepcion(cod_ins_super, glosa, log, dfContactos)
                                else:
                                    bitacora.registrar(
                                        id = ID_PROCESO_CARGA,
                                        estado = 1,
                                        instance_id = id_ins_carga,
                                        log= log,
                                        cant_total= len(df_valid),    
                                        cant_ok= len(df_valid),
                                        nombre_archivo_ok= archivos_validos,
                                    )
                            else:
                                bitacora.registrar(id = ID_PROCESO_CARGA, estado = 1, instance_id = id_ins_carga, log= log)
                            
                        else:
                            bitacora.registrar(id = ID_PROCESO_CARGA, estado = 0, instance_id = id_ins_carga, log= log)
                            notificar_excepcion(cod_ins_super, glosa, log, dfContactos)
                    
                    bitacora.registrar(id = ID_PROCESO_PADRE, estado = 1, instance_id = id_ins_general)
                    logger(FIN_SCRIPT_OK)
                else:
                    bitacora.registrar(id = ID_PROCESO_PADRE, estado = 0, instance_id = id_ins_general, log= log)
                    notificar_excepcion('ERROR GENÉRICO', 'ERROR GENÉRICO', log, dfContactos) 
                
            except Exception as e:
                log= str(e) + str(traceback.format_exc(limit=1))
                bitacora.registrar(id = ID_PROCESO_PADRE, estado = 0, instance_id = id_ins_general, log= log)
                notificar_excepcion('ERROR GENÉRICO', 'ERROR GENÉRICO', log, dfContactos)

        else:
            logger('.::El script a finalizado con un parámetro inválido::.')
        
    elif CERCANO_A_LUNES == PARAMETRO_VALIDAR_EJECUCION and sys.argv[1] == PARAMETRO_CONSOLIDADO:
        logger(f'::[CONSOLIDADO] Día habil para ejecutar')
        #...Proceso de generación del archivo consolidado. 
        if sys.argv[1] == PARAMETRO_CONSOLIDADO:
            try:
                id_ins_general, _ = bitacora.registrar(ID_PROCESO_PADRE, 'start')
                spLST = constant.SPLST_CONSOLIDADO_MAESTRO_REZAGO.format(
                            fecha_carga_desde = fecha_lunes_pasado.strftime('%Y%m%d') , 
                            fecha_carga_hasta= fecha_domingo_pasado.strftime('%Y%m%d')
                            )
                spLST_Validar_Consolidado = constant.SPLST_IIPP_CONSOLIDADO_MAESTRO_REZAGO.format(
                            fecha_carga_desde = fecha_lunes_pasado.strftime('%Y%m%d') , 
                            fecha_carga_hasta= fecha_domingo_pasado.strftime('%Y%m%d')
                            )
                dfProceso, log= db.connect_pymssql(constant.SERVER_GESTION, constant.BD_INTEGRA, constant.script_01, type='pandas')
                dfContactos, log= db.connect_pymssql(constant.SERVER_GESTION, constant.BD_INTEGRA, constant.script_02, type='pandas')
                df_consolidado, log = db.connect_pymssql(constant.SERVER_GESTION, constant.BD_INTEGRA, spLST, type='pandas')
                df_valida_consolidado, log = db.connect_pymssql(constant.SERVER_GESTION, constant.BD_INTEGRA, spLST_Validar_Consolidado, type='pandas')
                
                dfProceso['CantidadRegistros'] = 0
                dfProceso['procesoSemanal'] = 0
                
                if not str(log).startswith('EXCEPTION'):
                    diccionario= dict(zip(dfProceso['cod_ins_super'].apply(lambda x: str(x).strip()), dfProceso['glosa'].apply(lambda x: str(x).strip())))  
                    
                    for cod_ins_super, glosa in diccionario.items():
                        dfProceso.loc[dfProceso["cod_ins_super"] == cod_ins_super, 'CantidadRegistros'] = str(len(df_consolidado[df_consolidado['cod_afp'] == cod_ins_super]))
                        dfProceso.loc[dfProceso["cod_ins_super"] == cod_ins_super, 'procesoSemanal'] = 1 if not df_valida_consolidado[df_valida_consolidado['cod_afp'] == cod_ins_super].empty else 0
                    
                    for cod_ins_super, glosa in diccionario.items():
                        
                        id_ins_consolidado, _ = bitacora.registrar(ID_PROCESO_CONSOLIDADO, 'start')
                        ruta_procesados = os.listdir(constant.LOCAL_PROCESSED_FULL_PATH.format(cod_ins_super = cod_ins_super)) 
                        consolidado = False
                        logger(f'01__[{cod_ins_super}]__[{glosa}]____PROCESO GENERACIÓN ARCHIVOS CONSOLIDADOS. (id_instancia={id_ins_consolidado})')
                        
                        procesoSemanal = int(dfProceso[dfProceso["cod_ins_super"] == cod_ins_super]['procesoSemanal'])
                        
                        if procesoSemanal == 1:
                            consolidado = True 
                            logger(f'\t\t\t\tParticipación del proceso consolidado')
                        
                        if not str(log).startswith('EXCEPTION'):
                            if consolidado:
                                if df_consolidado is not None and not df_consolidado.empty:
                                    log = file.guardar_consolidado_local(df_consolidado, cod_ins_super)
                                    
                                    if not str(log).startswith('EXCEPTION'):  
                                        _, nombreArchivo, log = sftp.connect('SALIDA_ARCHIVO_CONSOLIDADO', cod_ins_super)
                                        
                                        if not str(log).startswith('EXCEPTION'):
                                            cant_registros = [str(format(int(c), ',')).replace(',', '.') for c in list(dfProceso['CantidadRegistros'])]
                                            resumen = {
                                                'id_instancia' : [id_ins_consolidado],
                                                'cod_inst_super' : [cod_ins_super],
                                                'fecha_hora_ejecucion' : [fecha_ejecucion],
                                                'periodo_facturacion' : [datetime.strptime(fecha_consolidado, '%Y%m%d').date().strftime('%Y%m')],
                                                'cantidad_total' : [len(df_consolidado)],
                                                'cantidad_reg_afp' : [int(dfProceso[dfProceso['cod_ins_super'] == cod_ins_super]['CantidadRegistros'])],
                                            }
                                            mail.send(
                                                type='CONSOLIDADO', 
                                                cod_ins_super = cod_ins_super,
                                                fecha_desde = datetime.strptime(CERCANO_A_VIERNES_PASADO, '%Y%m%d').date().strftime('%d-%m'),
                                                fecha_hasta = FECHA_OFICIAL_ACTUAL.strftime('%d-%m'),
                                                glosas_afps = list(dfProceso['glosa']), 
                                                cods_afps = list(dfProceso['cod_ins_super']), 
                                                cant_registros = cant_registros,
                                                cant_total = str(format(int(len(df_consolidado)), ',')).replace(',', '.'),
                                                dfContactos = dfContactos
                                                )
                                            bitacora.registrar(
                                                id = ID_PROCESO_CONSOLIDADO,
                                                estado = 1,
                                                instance_id = id_ins_consolidado,
                                                nombre_archivo_ok = nombreArchivo,
                                                ruta_sftp = str(constant.REMOTE_OUTPUT_PATH).format(cod_ins_super= cod_ins_super),
                                                log= log,
                                                cant_total= len(df_consolidado),
                                                cant_ok= int(dfProceso[dfProceso['cod_ins_super'] == cod_ins_super]['CantidadRegistros']),
                                            )
                                            df_resumen = pd.DataFrame(resumen)
                                            print(df_resumen)
                                            db.connect_alchemy(df_resumen, constant.TBL_RESUMEN_CONSOLIDADO)
                                        else:
                                            bitacora.registrar(id = ID_PROCESO_CONSOLIDADO, estado = 0, instance_id = id_ins_consolidado, log= log)
                                            notificar_excepcion(cod_ins_super, glosa, log, dfContactos)
                                    else:
                                        bitacora.registrar(id = ID_PROCESO_CONSOLIDADO, estado = 0, instance_id = id_ins_consolidado, log= log)
                                        notificar_excepcion(cod_ins_super, glosa, log, dfContactos)
                                else:
                                    bitacora.registrar(id = ID_PROCESO_CONSOLIDADO, estado = 1, instance_id = id_ins_consolidado, log= '[Consolidado sin registros]')
                            else:
                                logger('\t\t\t\t\t[SIN CONSOLIDADO] Esta institución no participa del proceso semanal.')
                                bitacora.registrar(id = ID_PROCESO_CONSOLIDADO, estado = 1, instance_id = id_ins_consolidado, log= f'[No participa del proceso semanal] cod_ins_super:{str(cod_ins_super)}')
                        else:
                            bitacora.registrar(id = ID_PROCESO_CONSOLIDADO, estado = 0, instance_id = id_ins_consolidado, log= log)
                            notificar_excepcion(cod_ins_super, glosa, log, dfContactos)
                            
                    bitacora.registrar(id = ID_PROCESO_PADRE, estado = 1, instance_id = id_ins_general, log= log)
                    logger(FIN_SCRIPT_OK)
                else:
                    bitacora.registrar(id = ID_PROCESO_PADRE, estado = 0, instance_id = id_ins_general, log= log)
                    notificar_excepcion('ERROR GENÉRICO', 'ERROR GENÉRICO', log, dfContactos) 
                
            except Exception as e:
                log= str(e) + str(traceback.format_exc(limit=1))
                bitacora.registrar(id = ID_PROCESO_PADRE, estado = 0, instance_id = id_ins_general, log= log)
                notificar_excepcion('ERROR GENÉRICO', 'ERROR GENÉRICO', log, dfContactos)
            #Consulta base de datos y deposito de archivos
            
        else:
            logger('.::El script a finalizado con un parámetro inválido::.')    
    else:
        logger(f'::[{sys.argv[1]}] Hoy día {PARAMETRO_VALIDAR_EJECUCION} no corresponde ejecutar::. Consolidado: {CERCANO_A_LUNES} | Validación_Carga: {CERCANO_A_VIERNES}') 
        
else:
    logger('.::El script a finalizado sin recibir ningún parámetro::.')
 


