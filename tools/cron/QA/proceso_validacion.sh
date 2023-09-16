#!/usr/bin/bash

echo "Activando entorno virtual"
source /home/app_proc_qa/ProyectoMaestroRezagos/.env/bin/activate

echo "Dirijiendo a directorio /home/app_proc_qa/ProyectoMaestroRezagos"
cd /home/app_proc_qa/ProyectoMaestroRezagos

echo "Iniciando script MaestroRezagos (proceso_validacion)"
python main.py 'proceso_validacion' 

echo "Desactivando entorno virtual"
deactivate
