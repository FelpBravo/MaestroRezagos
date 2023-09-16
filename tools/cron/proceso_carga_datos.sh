#!/usr/bin/bash

echo "Activando entorno virtual"
source /previred/MaestroRezagos/.env/bin/activate

echo "Dirijiendo a directorio /previred/MaestroRezagos"
cd /previred/MaestroRezagos

echo "Iniciando script MaestroRezagos (proceso_carga_datos)"
python main.py 'proceso_carga_datos'

echo "Desactivando entorno virtual"
deactivate
