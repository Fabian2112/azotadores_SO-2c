########################################################
# Makefile para utils
########################################################

# Compilador y flags
CC = gcc
CFLAGS = -g -Wall -Wextra -I./include
AR = ar
ARFLAGS = rcs

# Directorios
SRC_DIR = src
INCLUDE_DIR = include
OBJ_DIR = obj
LIB_DIR = lib

# Archivos fuente y objetos
SRCS = $(wildcard $(SRC_DIR)/*.c)
OBJS = $(patsubst $(SRC_DIR)/%.c,$(OBJ_DIR)/%.o,$(SRCS))

# Librería final
LIB = $(LIB_DIR)/libutils.a

########################################################
# Targets
########################################################

.PHONY: all clean

all: $(LIB)

# Generar la librería estática
$(LIB): $(OBJS) | $(LIB_DIR)
	$(AR) $(ARFLAGS) $@ $^

# Compilar objetos
$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

# Crear directorios si no existen
$(OBJ_DIR):
	mkdir -pv $@

$(LIB_DIR):
	mkdir -pv $@

# Limpiar todo
clean:
	-rm -rf $(OBJ_DIR)/*.o
	-rm -rf $(LIB_DIR)/libutils.a
