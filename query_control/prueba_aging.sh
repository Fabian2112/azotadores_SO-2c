#!/bin/bash

# Validaciรณn de parรกmetros
if [ $# -ne 2 ]; then
    echo "Uso: $0 <N_prueba> <Prioridad>"
    echo "Ejemplo: $0 1 4"
    exit 1
fi

N=$1
PRIORIDAD=$2
PRUEBA="AGING_${N}"

# Directorio actual (donde estรก este script - query_control/)
SCRIPT_DIR="$(pwd)"
echo "Script directory: $SCRIPT_DIR"

# Verificar si estamos en query_control/ o en otra ubicaciรณn
if [[ "$SCRIPT_DIR" == *"query_control" ]]; then
    # Estamos en query_control/
    QUERY_EXEC="./bin/query"
    CONFIG="./query.config"
    BASE_DIR="$SCRIPT_DIR"
else
    # Estamos en otra ubicaciรณn, ajustar rutas
    QUERY_EXEC="$SCRIPT_DIR/query_control/bin/query"
    CONFIG="$SCRIPT_DIR/query_control/query.config"
    BASE_DIR="$SCRIPT_DIR/query_control"
fi

echo "=== Ejecutando prueba ==="
echo "Directorio base: $BASE_DIR"
echo "Prueba: $PRUEBA"
echo "Prioridad: $PRIORIDAD"
echo "Ejecutable: $QUERY_EXEC"
echo "Configuraciรณn: $CONFIG"

# Verificar ejecutable
if [ ! -x "$QUERY_EXEC" ]; then
    echo "WARN: Ejecutable no encontrado, compilando..."
    
    # Ir al directorio del proyecto para compilar
    if [ -f "$BASE_DIR/../Makefile" ]; then
        cd "$BASE_DIR/.." && make query
    elif [ -f "$BASE_DIR/Makefile" ]; then
        cd "$BASE_DIR" && make query
    else
        echo "ERROR: No se encontrรณ Makefile"
        exit 1
    fi
    
    # Verificar nuevamente
    if [ ! -x "$QUERY_EXEC" ]; then
        echo "ERROR: No se pudo compilar el ejecutable"
        exit 1
    fi
fi

# Buscar archivo de prueba
PRUEBA_PATH=""
LOCATIONS=(
    "../utils/pruebas/${PRUEBA}"
    "../../utils/pruebas/${PRUEBA}"
    "utils/pruebas/${PRUEBA}"
    "${PRUEBA}"
    "../${PRUEBA}"
)

echo "Buscando archivo de prueba..."
cd "$BASE_DIR" || exit 1

for loc in "${LOCATIONS[@]}"; do
    if [ -f "$loc" ]; then
        PRUEBA_PATH="$loc"
        echo "โ�� Archivo encontrado: $PRUEBA_PATH"
        break
    else
        echo "  Probando: $loc"
    fi
done

if [ -z "$PRUEBA_PATH" ]; then
    echo "ERROR: No se encontrรณ el archivo de prueba '$PRUEBA'"
    echo "Directorio actual: $(pwd)"
    echo "Contenido:"
    ls -la
    exit 1
fi

# Verificar que el archivo de configuraciรณn exista
if [ ! -f "$CONFIG" ]; then
    echo "ERROR: Archivo de configuraciรณn no encontrado: $CONFIG"
    exit 1
fi

# Ejecutar
echo "Ejecutando: $QUERY_EXEC $CONFIG \"$PRUEBA_PATH\" $PRIORIDAD"
"$QUERY_EXEC" "$CONFIG" "$PRUEBA_PATH" "$PRIORIDAD"

EXIT_CODE=$?
echo "---------------------------------------"
echo "Cรณdigo de salida: $EXIT_CODE"
exit $EXIT_CODE

