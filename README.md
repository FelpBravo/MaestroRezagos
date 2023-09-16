# Proyecto Maestro de Rezagos

El proyecto Maestro de Rezagos es un desarrollo Python diseñado para sistemas unix, ya que se ejecuta automáticamente a través de un crontab. Este proyecto tiene como objetivo principal validar, cargar datos y generar un archivo consolidado. Está destinado a trabajar con tablas que contienen millones de registros y se basa en el uso de diversas tecnologías y herramientas, incluyendo Python, Pandas, Pysftp, SQLAlchemy, servidor de correos y Bash.

## Descripción del Proyecto

El proyecto Maestro de Rezagos es un sistema complejo que se ejecuta en un entorno de producción específico. Aquí se describen los aspectos clave del proyecto:

- **Modos de Ejecución**: El proyecto tiene tres modos de ejecución diferentes, dependiendo de los parámetros proporcionados al script. Estos modos permiten validar datos, cargar datos y generar un archivo consolidado.

- **Calendario Laboral**: La ejecución del proyecto está programada para días laborables específicos. Esto se controla utilizando una tabla llamada "calendario" que se encuentra en la base de datos. La ejecución fuera de los días hábiles adecuados no se llevará a cabo.

- **Rendimiento y Duración**: Debido a la gran cantidad de datos que maneja el proyecto, la ejecución puede llevar bastante tiempo. La duración depende de la cantidad de datos y del rendimiento del servidor en el que se ejecute.

## Instalación

Para instalar y configurar el proyecto correctamente, sigue estos pasos:

### Requisitos Previos

- Python 3.6 o superior debe estar instalado.
- Se requiere la librería VirtualEnv en su versión 20.16 o superior.

### Pasos de Instalación

1. Descomprime el proyecto en la ruta `/previred/MaestroRezagos`.
2. Accede a la ruta de instalación del proyecto, por ejemplo: `cd /previred/MaestroRezagos`.
3. Crea un entorno virtual con el comando: `/usr/bin/python3.9 -m venv .env`.
4. Activa el entorno virtual con el comando: `.env/Scripts/activate.bat`.
5. Dentro del entorno virtual, instala las librerías necesarias siguiendo las instrucciones detalladas en el archivo `README.txt` ubicado en el directorio `/packages`.

Una vez que todas las librerías estén instaladas, el proyecto estará listo para su ejecución.

## Ejecución

El proyecto tiene tres tipos de ejecución, cada uno asociado a un día hábil específico. Asegúrate de proporcionar el parámetro de ejecución correcto al archivo `main.py` para que el script pueda reconocer el tipo de ejecución deseado. Los tipos de ejecución son los siguientes:

- `python main.py 'proceso_validacion'`: Solo se ejecuta los viernes hábiles.
- `python main.py 'proceso_carga_datos'`: Solo se ejecuta los viernes hábiles.
- `python main.py 'proceso_consolidado'`: Solo se ejecuta los lunes hábiles.

El Crontab está configurado para que el proyecto se ejecute de lunes a viernes.

## Detalle de la Estructura del Proyecto

El proyecto está organizado en las siguientes carpetas:

- **config**: Contiene archivos de configuración y variables.
- **functions**: Contiene módulos con lógica de registro, conexiones a bases de datos, manipulación de archivos y más.
- **packages**: Contiene librerías y las instrucciones de instalación.
- **reports**: Almacena archivos de entrada, procesados y de salida.
- **tools**: Contiene documentación y controles de cambios.

## Configuraciones para QA y Producción

Antes de ejecutar el proyecto en entornos de prueba (QA) o producción, asegúrate de configurar correctamente las variables en el archivo `config.py`. Modifica las constantes según corresponda para cada entorno.

## 
**Nota Importante**: Los archivos sensibles no han sido subidos al repositorio o han sido ocultados por razones de seguridad.

Estos archivos contienen información confidencial, como contraseñas o claves de acceso, y no deben estar disponibles públicamente en un repositorio de código abierto.

