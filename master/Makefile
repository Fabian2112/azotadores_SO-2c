# =========================
# Makefile para Master
# =========================

# Compilador y flags
CC = gcc
CFLAGS = -g -Wall -Wextra -I./include -I../utils/include

# Nota: orden de librerías importante, y agregamos OpenSSL (-lcrypto)
LDFLAGS = -L../utils/lib -lcommons -lutils -lpthread -lcrypto

# Carpetas
SRC_DIR = src
OBJ_DIR = obj
BIN_DIR = bin

# Archivos
MASTER_SRC = $(SRC_DIR)/master.c
MASTER_OBJ = $(OBJ_DIR)/master.o
MASTER_BIN = $(BIN_DIR)/master

# ==================================
# Reglas
# ==================================

all: $(MASTER_BIN)

# =========================
# Compilar master
# =========================
$(MASTER_BIN): $(MASTER_OBJ)
	@mkdir -p $(BIN_DIR)
	$(CC) $^ -o $@ $(LDFLAGS)
	@echo "🔧 Master enlazado"

$(MASTER_OBJ): $(MASTER_SRC)
	@mkdir -p $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

# =========================
# Limpiar
# =========================
clean:
	rm -rf $(OBJ_DIR)/* $(BIN_DIR)/*
	@echo "🧹 Limpieza completada"

.PHONY: all clean
