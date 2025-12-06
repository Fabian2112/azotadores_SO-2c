#ifndef STORAGE_H
#define STORAGE_H

#include <server.h>
#include <conexion.h>
#include "bitmap.h"

typedef enum {
    WORK_IN_PROGRESS,
    COMMITED
} file_status_t;

// Estructura para worker en storage
typedef struct {
    int worker_id;
    uint32_t query_id;
    char* worker_id_str; // ID como string (opcional)
    int socket_worker;
    bool conectado;
} t_worker_storage;

extern t_log* logger;

// Operaciones del Storage
storage_t* inicializar_storage(const char* config_path);
void storage_destroy(storage_t* storage);
void iniciar_servidor_storage();
void* atender_worker_storage(void* args);

char* recibir_string_del_worker(int socket);

int allocate_physical_block(storage_t* storage, uint32_t query_id);
char* get_physical_block_path(storage_t* storage, int block_num);
unsigned char* calculate_block_hash(const void* data, size_t size, unsigned char* hash_out);
int find_block_by_hash(storage_t* storage, const char* hash);

int create_file_structure(storage_t* storage, const char* filename, const char* tag);
int storage_create_file(storage_t* storage, const char* filename, const char* tag);

int storage_commit_tag(storage_t* storage, const char* filename, const char* tag, uint32_t query_id);

void manejar_create_file(int socket_cliente, uint32_t query_id);
void manejar_write_file(int socket_cliente, uint32_t query_id);
void manejar_read_page(int socket_cliente, uint32_t query_id);
void manejar_truncate_file(int socket_cliente, uint32_t query_id);
void manejar_delete_file(int socket_cliente, uint32_t query_id);
void manejar_tag_file(int socket_cliente, uint32_t query_id);
void manejar_commit_file(int socket_cliente, uint32_t query_id);
void manejar_flush_file(int socket_cliente, uint32_t query_id);


// Funciones auxiliares
int initialize_fresh_storage(storage_t* storage);
int load_existing_storage(storage_t* storage);
void apply_operation_delay(storage_t* storage);
void apply_block_access_delay(storage_t* storage, size_t block_count);

int reservar_bloque_libre(storage_t* storage, uint32_t query_id);
void free_physical_block(storage_t* storage, int block_num, uint32_t query_id);

// Declaraciones de funciones de logging
void logging_conexion_worker_storage(t_worker_storage* worker, int cantidad);
void logging_desconexion_worker_storage(int worker_id, int cantidad);
void logging_file_creado(uint32_t query_id, const char* filename, const char* tag);
void logging_file_truncado(uint32_t query_id, const char* filename, const char* tag, size_t tamanio);
void logging_tag_creado(uint32_t query_id, const char* filename, const char* tag);
void logging_commit_tag(uint32_t query_id, const char* filename, const char* tag);
void logging_tag_eliminado(uint32_t query_id, const char* filename, const char* tag);
void logging_bloque_logico_leido(uint32_t query_id, const char* filename, const char* tag, size_t bloque);
void logging_bloque_logico_escrito(uint32_t query_id, const char* filename, const char* tag, size_t bloque);
void logging_bloque_fisico_reservado(uint32_t query_id, int bloque);
void logging_bloque_fisico_liberado(uint32_t query_id, int bloque);
void logging_hard_link_agregado(uint32_t query_id, const char* filename, const char* tag, 
                                 size_t bloque_logico, int bloque_fisico);
void logging_hard_link_eliminado(uint32_t query_id, const char* filename, const char* tag,
                                  size_t bloque_logico, int bloque_fisico);
void logging_deduplicacion_bloque(uint32_t query_id, const char* filename, const char* tag,
                                   size_t bloque_logico, int bloque_actual, int bloque_nuevo);


#endif