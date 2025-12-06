#ifndef WORKER_H_
#define WORKER_H_

#include <conexion.h>
#include <cliente.h>
#include <server.h>

// Códigos de operación
#define GET_BLOCK_SIZE 100
#define BLOCK_SIZE 101
#define HANDSHAKE_WORKER 102
#define CONFIRMATION 103
#define ASSIGN_QUERY 104

// Definir la estructura t_query
typedef struct {
    uint32_t id;
    uint32_t pc;
    char* path;
} t_query;

// Variables globales que usa worker.c
extern t_log* logger;
extern t_config* config;
extern int socket_storage;
extern int socket_master;
extern uint32_t WORKER_BLOCK_SIZE;
extern char* WORKER_ID;

// ---- Inicialización ----
void iniciar_worker(char* config_path, char* log_path, char* worker_id);
void iniciar_logger_worker(void);
t_config* iniciar_config(char* config_path);
void iniciar_memoria();

// ---- Conexión y handshake con Storage ----
int conectar_storage(void);
int handshake_storage_pedir_blocksize(void);

// ---- Conexión y handshake con Master ----
int conectar_master(void);
int handshake_master_enviar_id(void);

// ---- Loop de escucha ----
void bucle_escuchar_master(void);

// ---- Ejecutar Query ----
void ejecutar_query(t_query* q);
void enviar_notificacion_lectura_master(uint32_t query_id);
void enviar_finalizacion_exitosa_master(uint32_t query_id);
void enviar_error_a_master(uint32_t query_id, const char* mensaje_error);
bool ejecutar_instruccion(uint32_t id, const char* line, uint32_t pc);

// ---- Finalizar ----
void finalizar_worker(void);

// ---- Funciones de memoria ----
void memory_destroy(void);

#endif 