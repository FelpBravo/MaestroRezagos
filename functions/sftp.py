import pysftp

from paramiko import AuthenticationException, SSHException
from config.log import logger
import config.config as constant
import os
import traceback

cnopts= pysftp.CnOpts()
cnopts.hostkeys= None 

TIPO_ENTRADA_REZ= 'ENTRADA_ARCHIVO_REZ'
TIPO_SALIDA_NO_VALIDOS= 'SALIDA_ARCHIVO_REGISTROS_NO_VALIDOS'
TIPO_SALIDA_CONSOLIDADO= 'SALIDA_ARCHIVO_CONSOLIDADO'

def connect(type, cod_ins_super='', ext_afp='') :
    logger(f'\t\t\t\t[SFTP] Iniciando conexión al servidor "{constant.host}"')
    
    informacion= {
        'Validacion': '', 
        'Excepcion' : '', 
        'Log': 'Sin información',
        'fullLocalPath': '',
        'nombreArchivo': ''
        }
    
    try:
        
        with pysftp.Connection(
            host=constant.host, username=constant.username, password=constant.password, port=constant.port, cnopts=cnopts
            ) as sftp:
            
            logger(f'\t\t\t\t\tConexión existosa! "{constant.host}"')
            
            if type == TIPO_ENTRADA_REZ:
                logger(f'\t\t\t\t\t[COPIA LOCAL ARCHIVO ENTRADA REZ] Rescatando y validando archivo de entrada.')
                hora= []
                fileName= ''
                pathRemote= str(constant.REMOTE_INPUT_FULL_PATH).format(cod_ins_super=cod_ins_super)
                pathLocal=  str(constant.LOCAL_INPUT_FULL_PATH).format(cod_ins_super=cod_ins_super)
                if len(sftp.listdir(pathRemote)) > 0: #..................................Validar si hay archivos para listar en el directorio.
                    for file in sftp.listdir(pathRemote):               
                        if file.startswith(constant.INPUT_FILE):  #......................Validar si hay archivo que comienze con ese patrón de búsqueda.
                            if len(file) == 21 and '_' in file:#.........................Valida largo del nombre archivo de 21 y que contenga un guión(-).
                                nombre_y_horaExtension= file.split('_')
                                if len(nombre_y_horaExtension) == 2:    
                                    if '.' in nombre_y_horaExtension[1]: #...............Valida un punto(.) dentro del string.
                                        hora_y_extension= nombre_y_horaExtension[1].split('.')
                                        HHmm= hora_y_extension[0]
                                        extension= '.' + hora_y_extension[1]
                                        if extension==ext_afp: #.........................Valida extensión archivo.
                                            if len(HHmm) == 4 and HHmm.isdigit(): #......Valida hora numerica y largo de 4.
                                                hora.append(int(hora_y_extension[0]))
                                            else:
                                                informacion['nombreArchivo'] = file
                                                informacion['Validacion']= f'[NOK] Formato o largo de caracteres de hora no es correcto.' 
                                        else:
                                            informacion['nombreArchivo'] = file
                                            informacion['Validacion']= f'[NOK] Extensión de archivo no válida.'                                   
                                    else:
                                        informacion['nombreArchivo'] = file
                                        informacion['Validacion']= f'[NOK] No se reconoce la estructura del archivo.'            
                                else:
                                    informacion['nombreArchivo'] = file
                                    informacion['Validacion']= f'[NOK] No se reconoce la estructura del archivo.'            
                            else:
                                informacion['nombreArchivo'] = file
                                informacion['Validacion']= f'[NOK] Largo de archivo no corresponde.'   
                        else:
                            informacion['Validacion']= f'[NOT] Archivo CREZ no se encuentra disponible en casilla SFTP.'
                else:
                    informacion['Validacion']= f'[NOT] Archivo CREZ no se encuentra disponible en casilla SFTP.'

                if len(hora) > 0:
                    fileName= constant.INPUT_FILE + str(max(hora)).zfill(4) + ext_afp
                    pathLocalProccesed = str(constant.LOCAL_PROCESSED_FULL_PATH).format(cod_ins_super=cod_ins_super)
                    if not os.path.isfile( pathLocalProccesed + fileName ): #...*****CAMBIAR******Tener en cuenta que esta validación debe cambiar a carpeta /Procesado ***********
                        sftp.get(str(pathRemote+fileName), str(pathLocal+fileName))
                        informacion['Validacion']= f'[OK] Archivo copiado en ruta local [ruta: {str(pathLocal+fileName)}] [nombre: {fileName}].'
                        informacion['fullLocalPath']= str(pathLocal+fileName)
                        informacion['nombreArchivo']= str(fileName)
                        informacion['Log'] = informacion['Validacion']
                    else:
                        informacion['Validacion']= f'[NOT] Archivo CREZ no se encuentra disponible en casilla SFTP.'
                        informacion['Log'] = informacion['Validacion']
                else:        
                    informacion['Log'] = informacion['Validacion']
            
            if type == TIPO_SALIDA_NO_VALIDOS:
                logger(f'\t\t\t\t\t[COPIA REMOTA NO VALIDOS] Copiando archivo con datos no validos.')
                nombreArchivo = constant.OUTPUT_NOK_FILE.format(extafp = ext_afp)
                informacion['nombreArchivo'] = nombreArchivo
                pathLocal =  str(constant.LOCAL_OUTPUT_PATH).format(cod_ins_super = cod_ins_super) + nombreArchivo
                pathRemote =  str(constant.REMOTE_OUTPUT_PATH).format(cod_ins_super = cod_ins_super) + nombreArchivo

                sftp.put(pathLocal, pathRemote)
                informacion['Log'] = f'[OK] Archivo con datos no validos copiado correctamente en el servidor SFTP {pathRemote}'
                
            if type == TIPO_SALIDA_CONSOLIDADO:
                nombreArchivo = constant.OUTPUT_CONSOLIDADO_FILE
                informacion['nombreArchivo'] = nombreArchivo
                pathLocal =  str(constant.LOCAL_OUTPUT_PATH.format(cod_ins_super = cod_ins_super) + nombreArchivo)
                pathRemote =  str(constant.REMOTE_OUTPUT_PATH.format(cod_ins_super = cod_ins_super) + nombreArchivo)

                sftp.put(pathLocal, pathRemote)
                informacion['Log'] = f'[OK] Archivo CONSOLIDADO copiado correctamente en el servidor SFTP {pathRemote}'
                
            sftp.close()

    except (AuthenticationException, SSHException, PermissionError, TypeError, NameError, ImportError, IndexError) as e:
        informacion['Excepcion']= "EXCEPTION_SFTP: " + str(e) + str(traceback.format_exc(limit=1))
        informacion['Log'] = informacion['Excepcion']
        
    finally:
        fullLocalPath = informacion['fullLocalPath']
        nombreArchivo = informacion['nombreArchivo']
        log = informacion['Log']
        logger('\t\t\t\t\t' + str(log))
        
        return fullLocalPath, nombreArchivo, log
