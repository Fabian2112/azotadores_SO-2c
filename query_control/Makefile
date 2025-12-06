# Makefile para Query Control

CC = gcc
CFLAGS = -g -Wall -Wextra -I./include -I../utils/include
LDFLAGS = -lcommons -lpthread -lreadline -lcrypto

# Directorios
SRC_DIR = src
OBJ_DIR = obj
BIN_DIR = bin
INCLUDE_DIR = include
UTILS_DIR = ../utils/src
UTILS_INCLUDE_DIR = ../utils/include

# Archivos
TARGET = $(BIN_DIR)/query

OBJECTS = \
    $(OBJ_DIR)/query.o \
    $(OBJ_DIR)/cliente.o \
    $(OBJ_DIR)/server.o \
    $(OBJ_DIR)/conexion.o

# Regla principal
all: directories $(TARGET)

directories:
	@mkdir -p $(OBJ_DIR)
	@mkdir -p $(BIN_DIR)

$(TARGET): $(OBJECTS)
	$(CC) $(OBJECTS) -o $(TARGET) $(LDFLAGS)
	@echo "Query Control compilado exitosamente"

$(OBJ_DIR)/query.o: $(SRC_DIR)/query.c $(INCLUDE_DIR)/query.h
	$(CC) $(CFLAGS) -c $(SRC_DIR)/query.c -o $(OBJ_DIR)/query.o

$(OBJ_DIR)/cliente.o: $(UTILS_DIR)/cliente.c $(UTILS_INCLUDE_DIR)/cliente.h
	$(CC) $(CFLAGS) -c $(UTILS_DIR)/cliente.c -o $(OBJ_DIR)/cliente.o

$(OBJ_DIR)/server.o: $(UTILS_DIR)/server.c $(UTILS_INCLUDE_DIR)/server.h
	$(CC) $(CFLAGS) -c $(UTILS_DIR)/server.c -o $(OBJ_DIR)/server.o

$(OBJ_DIR)/conexion.o: $(UTILS_DIR)/conexion.c $(UTILS_INCLUDE_DIR)/conexion.h
	$(CC) $(CFLAGS) -c $(UTILS_DIR)/conexion.c -o $(OBJ_DIR)/conexion.o

clean:
	rm -rf $(OBJ_DIR) $(BIN_DIR)
	rm -f query.log

.PHONY: all clean
