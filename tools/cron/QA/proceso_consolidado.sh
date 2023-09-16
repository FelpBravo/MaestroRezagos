#!/usr/bin/bash

echo "Activando entorno virtual"
source /home/app_proc_qa/ProyectoMaestroRezagos/.env/bin/activate

echo "Dirijiendo a directorio /home/app_proc_qa/ProyectoMaestroRezagos"
cd /home/app_proc_qa/ProyectoMaestroRezagos

echo "Iniciando script MaestroRezagos (proceso_consolidado)"
python main.py 'proceso_consolidado'

echo "Desactivando entorno virtual"
deactivate
