#!/bin/bash

# Lista de carpetas a compilar (en orden correcto)
carpetas=("utils" "query_control" "storage" "worker" "master")

# Función para compilar un módulo
compilar_modulo() {
    local carpeta=$1
    echo "Compilando $carpeta..."
    
    cd "$carpeta" || { echo "❌ Error al entrar en $carpeta"; exit 1; }
    
    # Solo hacer clean si el directorio obj/ existe
    if [ -d "obj/" ]; then
        make clean || { echo "❌ Error al limpiar $carpeta"; exit 1; }
    fi

    # Intentar compilar con "make debug"
    if make debug 2>/dev/null; then
        echo "✅ Compilado en modo debug."
    else
        echo "⚠️ No se encontró 'debug', intentando 'make all'..."
        if make all; then
            echo "✅ Compilado con make all."
        else
            echo "❌ Error al compilar $carpeta."
            exit 1
        fi
    fi

    cd ..
    echo "✅ Compilación de $carpeta completada."
    echo "-----------------------------------"
}

# Compilar utils primero (como biblioteca estática)
compilar_modulo "utils"

# Luego compilar los demás módulos
for carpeta in "${carpetas[@]:1}"; do
    compilar_modulo "$carpeta"
done

echo "✅ Todas las compilaciones finalizaron correctamente."
