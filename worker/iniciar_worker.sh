#!/bin/bash

# Verifica que pasaron un parámetro
if [ -z "$1" ]; then
    echo "Uso: ./iniciar_worker.sh <ID>"
    echo "Ejemplo: ./iniciar_worker.sh 2"
    exit 1
fi

# ID del worker
ID="$1"

# Archivo de configuración del worker correspondiente
CONF="worker${ID}.config"

# Ruta al ejecutable (desde la carpeta worker/)
BIN="./bin/worker"

# Validaciones
if [ ! -f "$CONF" ]; then
    echo "❌ No existe el archivo de configuración: $CONF"
    exit 1
fi

if [ ! -f "$BIN" ]; then
    echo "❌ No se encontró el ejecutable en: $BIN"
    exit 1
fi

echo "Iniciando Worker..."
echo "  Ejecutable: $BIN"
echo "  Config:     $CONF"
echo "  ID:         $ID"
echo

$BIN "$CONF" "$ID"
