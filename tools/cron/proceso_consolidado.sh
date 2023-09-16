#!/usr/bin/bash

echo "Activando entorno virtual"
source /previred/MaestroRezagos/.env/bin/activate

echo "Dirijiendo a directorio /previred/MaestroRezagos"
cd /previred/MaestroRezagos

echo "Iniciando script MaestroRezagos (proceso_consolidado)"
python main.py 'proceso_consolidado'

echo "Desactivando entorno virtual"
deactivate
