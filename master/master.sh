#!/bin/bash

# -----------------------------------------
# Script para ejecutar el Master
# Debe ser ejecutado desde la carpeta /master
# Ejecuta: ./bin/master master.config
# -----------------------------------------

# Obtener directorio actual
CURRENT_DIR=$(basename "$PWD")

if [[ "$CURRENT_DIR" != "master" ]]; then
    echo "ERROR: Este script debe ejecutarse dentro del directorio 'master'"
    exit 1
fi

# Ruta al ejecutable
BIN="./bin/master"

# Archivo de configuración por defecto
CONF="master.config"

# Si el usuario pasa un archivo de conf, usarlo
if [[ -n "$1" ]]; then
    CONF="$1"
fi

# Validar existencia del ejecutable
if [[ ! -f "$BIN" ]]; then
    echo "ERROR: No se encontró el ejecutable en $BIN"
    exit 1
fi

# Validar existencia del archivo de configuración
if [[ ! -f "$CONF" ]]; then
    echo "ERROR: Archivo de configuración no encontrado: $CONF"
    exit 1
fi

echo "----------------------------------------"
echo " Iniciando MASTER"
echo " Ejecutable: $BIN"
echo " Config:     $CONF"
echo "----------------------------------------"
echo

$BIN "$CONF"
