
import functions.db as db
import config.config as constant
from config.log import logger

import traceback
import xml.etree.ElementTree as ET
from xml.dom import minidom


def registrar(
    id=constant.ID_PROCESO, type='', estado=1, instance_id=0, log='', nombre_archivo_ok='', ruta_sftp='', 
    nombre_archivo_nok='' , cant_total='0', cant_ok='0', cant_err='0'):
    
    try:
        if estado == 1:
            estadoBoolean=True
        else:
            estadoBoolean=False

        if type == 'start':
            instance_id= db.connect_pymssql(constant.SERVER_GESTION, constant.BD_INTEGRA, constant.SP_INS.format(id, 0))
            
            return instance_id
        
        else:
            resultado= to_xml(
                nombre_archivo= nombre_archivo_ok,
                nombre_archivo_error= nombre_archivo_nok,
                status= estadoBoolean, 
                log= str(log).replace("'",""), 
                id_proceso= id, 
                proceso_padre= constant.ID_PROCESO, 
                rutaSFTP= ruta_sftp
                )
            sp_end= f'''{constant.SP_UPD} {instance_id}, {str(estado)}, '{resultado}', {cant_total}, {cant_ok}, {cant_err}, NULL'''

            db.connect_pymssql(constant.SERVER_GESTION, constant.BD_INTEGRA, sp_end)
    except Exception as e:
        error= 'EXCEPCIÓN BITÁCORA ' + str(e) + str(traceback.format_exc(limit=1))
        logger(error)
        
def to_xml(nombre_archivo='', nombre_archivo_error='', status=True, id_proceso='', proceso_padre='', log= '', rutaSFTP=''):
   root = ET.Element('Resultado')
   info_proceso= ET.SubElement(root, 'informaciónProceso')

   if id_proceso == proceso_padre:
       if status:
            ET.SubElement(info_proceso, 'Estado').text= 'OK'
       else:
           ET.SubElement(info_proceso, 'Estado').text= 'ERROR'
           ET.SubElement(info_proceso, 'Mensaje').text= log 
   else:
       if status:
            detalle= ET.SubElement(root, 'Detalle')
            ET.SubElement(detalle, 'ServerSFTP').text= constant.host
            ET.SubElement(detalle, 'RutaSFTP').text= rutaSFTP
            ET.SubElement(detalle, 'nombreArchivo').text= nombre_archivo
            ET.SubElement(detalle, 'nombreArchivo_NoValido').text= nombre_archivo_error
            ET.SubElement(info_proceso, 'Estado').text= 'OK'
            ET.SubElement(info_proceso, 'Mensaje').text= log 
       else:
           ET.SubElement(info_proceso, 'Estado').text= 'ERROR'
           ET.SubElement(info_proceso, 'Mensaje').text= log 

   rough_string = ET.tostring(root, 'utf-8')
   reparsed = minidom.parseString(rough_string)
    
   return reparsed.toprettyxml(indent="  " )