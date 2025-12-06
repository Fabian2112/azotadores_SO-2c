#ifndef CONEXION_H_
#define CONEXION_H_

#include <commons/config.h>

#include <stdio.h>          // printf, fprintf, perror
#include <stdlib.h>         // malloc, free
#include <fcntl.h>        // fcntl, O_NONBLOCK
#include <stdint.h>

#include <commons/log.h>  // Necesario para t_log*
#include <commons/collections/dictionary.h>
#include <commons/collections/list.h>

#include <commons/string.h>
#include <commons/temporal.h>
#include <commons/crypto.h>
#include <commons/collections/queue.h>
#include <semaphore.h>

#include <readline/readline.h>
#include <signal.h>
#include <unistd.h>         // close, sleep
#include <sys/ioctl.h>    // Para ioctl()
#include <sys/mman.h>
#include <sys/types.h>	 	// ssize_t
#include <sys/socket.h>     // socket, bind, listen, connect, setsockopt
#include <sys/stat.h>
#include <netdb.h>          // struct addrinfo, getaddrinfo
#include <string.h>         // memset, strerror
#include <assert.h>
#include <stdbool.h>
#include <pthread.h>        // pthread_create, pthread_join
#include <libgen.h>  // Para dirname()
#include <dirent.h>

#include <errno.h>          // errno
#include <limits.h>
#include <arpa/inet.h>
#include <linux/limits.h>
#include <sys/time.h>

#include <openssl/evp.h>
#include <openssl/sha.h>
#include <commons/bitarray.h>  // Para bitarray functions


// Constantes que podrían usar otros módulos
#define OK 0
#define ERROR -1
#define MAX_PATH_LENGTH 8192

#define PORT 4444
#define MAX_CLIENTS 10

#define BLOCK_FILENAME_FORMAT "block%04d.dat"
#define METADATA_FILENAME "metadata.config"
#define LOGICAL_BLOCKS_DIR "logical_blocks"
#define PHYSICAL_BLOCKS_DIR "physical_blocks"
#define FILES_DIR "files"


// Variables compartidas
extern char* instruccion_global;
extern bool instruccion_disponible;
extern bool conexion_activa;
extern bool syscall_en_proceso;
extern t_log* logger;
extern t_dictionary* procesos_activos;  // Diccionario de procesos activos (key: PID, value: t_proceso*)
extern t_dictionary* procesos_suspendidos; // Diccionario de procesos suspendidos
extern void* espacio_usuario;           // Espacio contiguo de storage
extern t_list* tablas_paginas;          // Lista de tablas de páginas
extern t_list* operaciones_query_activas;
extern FILE* swap_file;                 // Archivo de swap
extern bool* marcos_libres;             // Array para control de marcos libres
extern int total_marcos;                // Total de marcos disponibles
extern char* storage_fisica;

// Mecanismos de sincronización
extern pthread_mutex_t mutex_instruccion;
extern pthread_cond_t cond_instruccion;
extern pthread_mutex_t mutex_archivo;
extern pthread_mutex_t mutex_path;
extern pthread_cond_t cond_archivo;
extern pthread_mutex_t mutex_envio_worker;
extern pthread_mutex_t mutex_comunicacion_storage;
extern pthread_mutex_t mutex_syscall;
extern pthread_mutex_t mutex_syscall_master;
extern pthread_mutex_t mutex_instruccion_pendiente;
extern pthread_mutex_t mutex_archivo_rewind;
extern pthread_mutex_t mutex_archivo_instrucciones;
extern pthread_mutex_t mutex_archivo_cierre;
extern pthread_mutex_t mutex_swap;      // Mutex para operaciones de swap
extern pthread_mutex_t mutex_marcos;    // Mutex para control de marcos libres
extern pthread_mutex_t mutex_procesos; // Mutex para acceso a procesos activos y suspendidos
extern pthread_mutex_t mutex_query_activas; 

// Semáforos para sincronización entre módulos
extern sem_t sem_master_storage_hs;       // Master espera handshake de storage
extern sem_t sem_master_worker_hs;           // Master espera handshake de Worker
extern sem_t sem_storage_master_ready;    // Storage lista para recibir archivo
extern sem_t sem_storage_worker_hs;          // Storage espera handshake de Worker
extern sem_t sem_worker_storage_hs;          // Worker espera handshake de storage
extern sem_t sem_worker_master_hs;           // Worker espera handshake de master
extern sem_t sem_archivo_listo;           // Archivo listo para ser procesado
extern sem_t sem_instruccion_lista;       // Instrucción lista para ser enviada
extern sem_t sem_servidor_worker_listo;     // Servidor Worker listo para recibir conexiones
extern sem_t sem_instruccion_disponible; // Instrucción disponible para ser procesada
extern sem_t sem_instruccion_worker_master;
extern sem_t sem_modulos_conectados;      // Módulos conectados y listos para operar
extern sem_t sem_instruccion_procesada; // Sincronización para indicar que una instrucción ha sido procesada
extern sem_t sem_syscall_procesada; // Sincronización para indicar que una syscall ha sido procesada
extern sem_t sem_query_completada; // Sincronización para indicar que una operación de I/O ha sido completada


typedef struct {
    uint8_t* data;
    size_t size;        // Tamaño en bytes del bitmap
    size_t bits_count;  // Cantidad total de bits/bloques
    int fd;             // File descriptor del archivo
} bitmap_t;

typedef struct {
    t_config* superblock;
    t_dictionary* blocks_hash_index;
    t_config* storage_config;
    char root_path[MAX_PATH_LENGTH];
    size_t block_size;
    size_t total_blocks;
    size_t fs_size;
    pthread_mutex_t mutex;
    void* bitmap;
} storage_t;


typedef struct {
    char* nombre_archivo;
    int tamanio_archivo;
} t_datos_master;

typedef struct {
    char instruccion[256];
    bool es_syscall;
    bool procesada;
} t_instruccion_pendiente;

// Estructura para tabla de páginas (simplificada)
typedef struct {
    int nivel;
    int entradas_usadas;
    void** entradas; // Array de punteros a siguiente nivel o marcos
} t_tabla_paginas;

typedef struct {
    bool presente;
    int marco;
    bool referenciada;
    bool modificada;
} t_entrada_pagina;


typedef struct {
    char* dispositivo;
    int tiempo_ms;
    pthread_t hilo_query;
    bool completada;
} t_operacion_query;

// Códigos de operación para mensajes
typedef enum
{
    //Handshakes
    GET_BLOCK_SIZE = 100,
    BLOCK_SIZE = 101,
    HANDSHAKE_WORKER = 102,
    CONFIRMATION = 103,
    HANDSHAKE_QUERY_CONTROL = 105,

    //Operaciones Archivos
    OP_CREATE = 200,
    END = 201, 
    OP_READ = 202, 
    OP_WRITE = 203,
    OP_TRUNCATE = 204,
    OP_DELETE = 205,
    OP_TAG = 206,
    OP_COMMIT = 207,
    OP_FLUSH = 208,
    OP_END = 209,  // Para Storage

    //Respuestas
    OP_OK = 210,
    OP_ERROR = 211,

    RESULTADO_READ = 212,
    OP_PROGRAM_COUNTER = 300,
    MENSAJE_LECTURA = 303,
    QUERY_FINALIZADA = 304,
    DESALOJAR_QUERY = 305,
    EJECUTAR_QUERY = 306,
    ERROR_EJECUCION = 307,
    
    //Identificadores de módulos
    MENSAJE = 10,
    PAQUETE = 11,
    MASTER = 12,
    WORKER = 13,
    STORAGE = 14,
    QUERY_CONTROL = 15,
    
    //Otros
    ASSIGN_QUERY = 104,
    READ = 400,
    NUEVA_QUERY = 401,
    DUMP_MEMORY,    
    EXIT,
    SYSCALL_ERROR,
    SYSCALL_OK
}op_code;



typedef struct
{
	int size;
	void* stream;
} t_buffer;


typedef struct
{
	int pid;
	int pc;
	t_list ME;
	t_list MT;
} t_pcb;


// Estructura para métricas por proceso
typedef struct {
    int accesos_tablas_paginas;
    int instrucciones_solicitadas;
    int bajadas_swap;
    int subidas_storage;
    int lecturas_storage;
    int escrituras_storage;
} t_metricas_proceso;

// Estructura para proceso
typedef struct {
    int pid;
    int tamanio;
    FILE* archivo_pseudocodigo;
    t_list* instrucciones;
    t_list* paginas; // Lista de páginas asignadas
    t_list* paginas_swap; // Lista de t_pagina_swap para páginas en swap
    t_metricas_proceso metricas;
    t_tabla_paginas* tabla_paginas; // Estructura de tablas de páginas multinivel
} t_proceso;


// Estructura para información de páginas en swap
typedef struct {
    int pid;
    int pagina;
    int posicion_swap; // Posición en el archivo swap
} t_pagina_swap;

// Estructura para intercambiar instrucciones
typedef struct {
    char instruccion[256];
    int longitud;
} t_instruccion;


typedef struct
{
	op_code codigo_operacion;
	t_buffer* buffer;
} t_paquete;


void saludar(char* quien);
void eliminar_paquete(t_paquete* paquete);
void iniciar_logger(t_log* logger);
void print_lista(void* valor);
void inicializar_sincronizacion();
void destruir_sincronizacion();
void inicializar_semaforos();
void destruir_semaforos();
void limpiar_buffer_comunicacion(int fd);
bool limpiar_buffer_completo(int socket_fd);
bool limpiar_buffer_antes_operacion(int socket_fd, const char* operacion);

#endif // CONEXION_H_