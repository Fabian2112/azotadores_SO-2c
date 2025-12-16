#!/bin/bash

# -----------------------------------------
# Script para ejecutar el Storage
# Debe ejecutarse desde el directorio /storage
# Ejecuta: ./bin/storage storage.config
# -----------------------------------------

# Verifica que se ejecuta del directorio correcto
CURRENT_DIR=$(basename "$PWD")

if [[ "$CURRENT_DIR" != "storage" ]]; then
    echo "ERROR: Este script debe ejecutarse dentro del directorio 'storage'"
    exit 1
fi

# Ruta del ejecutable
BIN="./bin/storage"

# Archivo de configuración por defecto
CONF="storage.config"

# Si el usuario pasa otro archivo de configuración, usarlo
if [[ -n "$1" ]]; then
    CONF="$1"
fi

# Verifica ejecutable
if [[ ! -f "$BIN" ]]; then
    echo "ERROR: No se encontró el ejecutable en $BIN"
    exit 1
fi

# Verifica archivo de configuración
if [[ ! -f "$CONF" ]]; then
    echo "ERROR: Archivo de configuración no encontrado: $CONF"
    exit 1
fi

echo "----------------------------------------"
echo " Iniciando STORAGE"
echo " Ejecutable: $BIN"
echo " Config:     $CONF"
echo "----------------------------------------"
echo

$BIN "$CONF"
