
from time import sleep
from config.log import logger
import config.config as constant
import sys
import os
import smtplib
import traceback
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

#Consideraciones.....
#Falta acomodar esta función para que rescate los usuarios de base de datos. Luego de esto se deben ajustar alguna variables.

TIPO_RECORDATORIO = 'ARCHIVO_REZ_NO_ENCONTRADO'
TIPO_NOK_NOMBREARCHIVO = 'FORMATO_NOMBRE_ARCHIVO_REZ_INCORRECTO'
TIPO_INFORME_VALIDACION = 'INFORME_VALIDACION'
TIPO_CONSOLIDADO = 'CONSOLIDADO'
TIPO_EXCEPCION = 'EXCEPTION'
CONTACTO_OPERACIONES = 'Operaciones'
CONTACTO_CONTROLESDB = 'ControlesDB'

path_templates_html= os.getcwd() + '/tools/html/'

def resolver_contactos(tipo_mail, cod_inst,  df):
    contactos = []
    if tipo_mail == TIPO_EXCEPCION:
        condicion = (df['a_materno'] == CONTACTO_CONTROLESDB)
        contactos = list(df[condicion]['email']) 
        
    else:   
        condicion = (df['a_materno'] == str(cod_inst)) | (df['a_materno'] == CONTACTO_OPERACIONES) | (df['a_materno'] == CONTACTO_CONTROLESDB)
        contactos = list(df[condicion]['email']) 

    return contactos
    
def send(
    type, cod_ins_super='', glosa='', ext='', msg='', nombre_archivo='',cant_enviados='', cant_leidos='', cant_validos='', 
    cant_rechazados='', fecha_ejecucion='', fecha_desde='', fecha_hasta='', glosas_afps='', cods_afps='', cant_registros='',
    dfContactos=None, cant_total='', df_validar_ejecucion=None):

    logger(f'\t\t\t\t[MAIL] "{type}" Iniciando función mail de tipo.')
    
    try:
        contactos = resolver_contactos(type, cod_ins_super, dfContactos)
        env = Environment(loader=FileSystemLoader(path_templates_html))
        content= ''
        
        if type == TIPO_RECORDATORIO:
            
            template = env.get_template('Template_Recordatorio.html')
            message = template.render(
                glosa = glosa, 
                msg = msg,
                validar_ejecucion = True if len(df_validar_ejecucion) > 0 else False
                )
            content = MIMEMultipart("alternative")
            content['From'] = constant.FROM_USER
            content['To'] = ','.join(contactos)
            content['Subject'] = 'Recordatorio Proceso Maestro Rezagos'
            content.attach(MIMEText(message, "html"))
            connect_mail(content, constant.FROM_USER, contactos)
            
        elif type == TIPO_NOK_NOMBREARCHIVO:
            template = env.get_template('Template_Valiacion_Nombre_Archivo.html')
            message = template.render(
                nombre_archivo = nombre_archivo,
                glosa = glosa, 
                fecha_ejecucion = fecha_ejecucion, 
                msg = msg, 
                extension = ext,
                )
            content = MIMEMultipart("alternative")
            content['From'] = constant.FROM_USER
            content['To'] = ','.join(contactos)
            content['Subject'] = 'Validación Archivo CREZ'
            content.attach(MIMEText(message, "html"))
            connect_mail(content, constant.FROM_USER, contactos)
            
        elif type == TIPO_INFORME_VALIDACION:
            template = env.get_template('Template_Informe_Validacion.html')
            message = template.render(
                glosa = glosa, 
                date = fecha_ejecucion, 
                nombre_archivo = nombre_archivo, 
                cant_enviados = cant_enviados, 
                cant_leidos = cant_leidos,
                cant_validos = cant_validos,
                cant_rechazados = cant_rechazados,
                nombre_archivo_no_valido = constant.OUTPUT_NOK_FILE.format(extafp = ext)
                )
            content = MIMEMultipart("alternative")
            content['From'] = constant.FROM_USER
            content['To'] = ','.join(contactos)
            content['Subject'] = 'Validación Archivo CREZ'
            content.attach(MIMEText(message, "html"))
            connect_mail(content, constant.FROM_USER, contactos)
        
        elif type == TIPO_CONSOLIDADO:
            template = env.get_template('Template_Consolidado.html')
            message = template.render(
                nombre_archivo_consolidado = constant.OUTPUT_CONSOLIDADO_FILE, 
                fecha_desde = fecha_desde, 
                fecha_hasta = fecha_hasta, 
                glosas_afps = glosas_afps, 
                cods_afps = cods_afps, 
                cant_registros = cant_registros,
                cant_total = cant_total,
                largo_tabla= len(glosas_afps)
                )
            content = MIMEMultipart("alternative")
            content['From'] = constant.FROM_USER
            content['To'] = ','.join(contactos)
            content['Subject'] = 'Consolidado Maestro Rezagos'
            content.attach(MIMEText(message, "html"))
            connect_mail(content, constant.FROM_USER, contactos)
            
        elif type == TIPO_EXCEPCION:
            template = env.get_template('template_exception.html')
            message = template.render(
                glosa=glosa, 
                cod=cod_ins_super, 
                msg=msg, 
                date=datetime.today().strftime('%Y/%m/%d')
                )
            content = MIMEMultipart("alternative")
            content['From'] = constant.FROM_USER
            content['To'] = ','.join(contactos)
            content['Subject'] = '[EXCEPCIÓN] Servicio Maestro de Rezagos'   
            content.attach(MIMEText(message, "html"))
            connect_mail(content, constant.FROM_USER, contactos)
        
    except (FileNotFoundError, AttributeError, KeyError) as e:
        logger("\t\t\t\t\tEXCEPTION_MAIL: " + str(e) + str(traceback.format_exc(limit=1)))


def connect_mail(content, from_u, to_u):
    logger('\t\t\t\t\tIniciando conexión de EMAIL con el servidor ' + constant.SMTP_SERVER)
    try:

        with smtplib.SMTP(constant.SMTP_SERVER, constant.PORT) as smtp:
            logger('\t\t\t\t\tEsperando conexión con el servidor ' + constant.SMTP_SERVER)
            smtp.connect(constant.SMTP_SERVER, constant.PORT)
            logger('\t\t\t\t\tConectado a  ' + constant.SMTP_SERVER + '...')
            if sys.platform.startswith('win32'):
                smtp.ehlo()
                smtp.starttls()
            logger(f'\t\t\t\t\tiniciando sesión en {from_u}...')
            smtp.login(constant.USER, constant.PASS)
            #En caso de que se requieran agregar con CC#
            #smtp.sendmail(from_addr=from_u, to_addrs=to_u + cc_u, msg=content.as_string())
            
            logger(f'\t\t\t\t\tEnviando a los siguientes contactos : {to_u}')
            smtp.sendmail(from_addr=from_u, to_addrs=to_u, msg=content.as_string()) 
            
            logger('\t\t\t\t\t[OK] Correo enviado con éxito')
            
            smtp.quit()
        logger('\t\t\t\t\tCerrando conexión Mail.')
        sleep(3)
        
        return 

    except (smtplib.SMTPAuthenticationError, smtplib.SMTPResponseException) as e:
        logger("\t\t\t\t\tEXCEPTION_MAIL: " + str(e) + str(traceback.format_exc(limit=1)))
