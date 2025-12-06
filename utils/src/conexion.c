#include "conexion.h"

// Definición de las variables compartidas
char* instruccion_global = NULL;
bool instruccion_disponible = false;
bool conexion_activa = true;
bool syscall_en_proceso = false;
t_log* logger_conexiones = NULL;
int total_marcos;
bool* marcos_libres;
t_dictionary* procesos_activos;
t_dictionary* procesos_suspendidos; 
FILE* swap_file;
char* memoria_fisica = NULL;
t_list* operaciones_io_activas;

// Definición e inicialización de los mutex
pthread_mutex_t mutex_instruccion = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_archivo = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_path = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_envio_cpu = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_comunicacion_memoria = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_syscall = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_syscall_kernel = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_instruccion_pendiente = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_archivo_rewind = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_archivo_instrucciones = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_archivo_cierre = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_swap = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_marcos = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_procesos = PTHREAD_MUTEX_INITIALIZER; // Mutex para acceso a procesos activos y suspendidos
pthread_mutex_t mutex_io_activas = PTHREAD_MUTEX_INITIALIZER;
// Definición e inicialización de variables de condición
pthread_cond_t cond_instruccion = PTHREAD_COND_INITIALIZER;

sem_t sem_kernel_memoria_hs;
sem_t sem_kernel_cpu_hs;
sem_t sem_memoria_kernel_ready;
sem_t sem_memoria_cpu_hs;
sem_t sem_cpu_memoria_hs;         
sem_t sem_cpu_kernel_hs;
sem_t sem_archivo_listo;
sem_t sem_instruccion_lista;
sem_t sem_servidor_cpu_listo;
sem_t sem_instruccion_disponible;
sem_t sem_instruccion_cpu_kernel;
sem_t sem_modulos_conectados;
sem_t sem_instruccion_procesada;
sem_t sem_syscall_procesada; // Sincronización para indicar que una syscall ha sido procesada
sem_t sem_io_completada; // Sincronización para indicar que una operación de I/O ha sido completada


void inicializar_sincronizacion() {
    // Inicialización explícita (aunque los INITIALIZER ya lo hacen)
    pthread_mutex_init(&mutex_instruccion, NULL);
    pthread_mutex_init(&mutex_archivo, NULL);
    pthread_mutex_init(&mutex_path, NULL);
    pthread_cond_init(&cond_instruccion, NULL);
    pthread_mutex_init(&mutex_envio_cpu, NULL);
    pthread_mutex_init(&mutex_comunicacion_memoria, NULL);
    pthread_mutex_init(&mutex_syscall, NULL);
    pthread_mutex_init(&mutex_syscall_kernel, NULL);
    pthread_mutex_init(&mutex_instruccion_pendiente, NULL);
    pthread_mutex_init(&mutex_archivo_rewind, NULL);
    pthread_mutex_init(&mutex_archivo_instrucciones, NULL);
    pthread_mutex_init(&mutex_archivo_cierre, NULL);
    pthread_mutex_init(&mutex_swap, NULL);
    pthread_mutex_init(&mutex_marcos, NULL);
    pthread_mutex_init(&mutex_procesos, NULL); // Mutex para acceso a procesos activos y suspendidos
    pthread_mutex_init(&mutex_io_activas, NULL);
}

void destruir_sincronizacion() {
    pthread_mutex_destroy(&mutex_instruccion);
    pthread_mutex_destroy(&mutex_archivo);
    pthread_mutex_destroy(&mutex_path);
    pthread_cond_destroy(&cond_instruccion);
}

void inicializar_semaforos() {
    // Inicializar todos los semáforos en 0 (bloqueados)
    sem_init(&sem_kernel_memoria_hs, 0, 0);
    sem_init(&sem_kernel_cpu_hs, 0, 0);
    sem_init(&sem_memoria_kernel_ready, 0, 0);
    sem_init(&sem_memoria_cpu_hs, 0, 0);
    sem_init(&sem_cpu_memoria_hs, 0, 0);
    sem_init(&sem_cpu_kernel_hs, 0, 0);
    sem_init(&sem_archivo_listo, 0, 0);
    sem_init(&sem_instruccion_lista, 0, 0);
    sem_init(&sem_servidor_cpu_listo, 0, 0);
    sem_init(&sem_instruccion_disponible, 0, 0);
    sem_init(&sem_instruccion_cpu_kernel, 0, 1);
    sem_init(&sem_modulos_conectados, 0, 0);
    sem_init(&sem_instruccion_procesada, 0, 0); // Inicializado en 0 para esperar instrucciones
    sem_init(&sem_syscall_procesada, 0, 0); // Inicializado en 0 para esperar syscalls
    sem_init(&sem_io_completada, 0, 0); // Inicializado en 0 para esperar operaciones de I/O
}

void destruir_semaforos() {
    sem_destroy(&sem_kernel_memoria_hs);
    sem_destroy(&sem_kernel_cpu_hs);
    sem_destroy(&sem_memoria_kernel_ready);
    sem_destroy(&sem_memoria_cpu_hs);
    sem_destroy(&sem_cpu_memoria_hs);
    sem_destroy(&sem_cpu_kernel_hs);
    sem_destroy(&sem_archivo_listo);
    sem_destroy(&sem_instruccion_lista);
    sem_destroy(&sem_servidor_cpu_listo);
    sem_destroy(&sem_instruccion_disponible);
    sem_destroy(&sem_instruccion_cpu_kernel);
    sem_destroy(&sem_modulos_conectados);
}

void iniciar_logger(t_log* logger) {
    logger_conexiones = logger;
}

// Función para liberar un paquete
void eliminar_paquete(t_paquete* paquete) {
    if (paquete != NULL) {
        if (paquete->buffer != NULL) {
            if (paquete->buffer->stream != NULL) {
                free(paquete->buffer->stream);
            }
            free(paquete->buffer);
        }
        free(paquete);
    }
}


void print_lista(void* valor) {
    if (valor != NULL) {
        log_info(logger_conexiones, "%s", (char*)valor);
    } else {
        log_warning(logger_conexiones, "Valor nulo recibido en print_lista");
    }
}


void limpiar_buffer_comunicacion(int fd) {
    char buffer_temp[1024];
    int bytes_leidos;
    
    log_info(logger, "Limpiando buffer de comunicación...");
    
    // Leer todos los datos pendientes del socket de forma no bloqueante
    while((bytes_leidos = recv(fd, buffer_temp, sizeof(buffer_temp), MSG_DONTWAIT)) > 0) {
        log_warning(logger, "Limpiados %d bytes del buffer", bytes_leidos);
    }
    
    if(bytes_leidos == -1 && errno != EAGAIN && errno != EWOULDBLOCK) {
        log_error(logger, "Error al limpiar buffer: %s", strerror(errno));
    } else {
        log_info(logger, "Buffer limpiado exitosamente");
    }
}

void liberar_paquete(t_paquete* paquete) {
    if (paquete != NULL) {
        if (paquete->buffer != NULL) {
            if (paquete->buffer->stream != NULL) {
                free(paquete->buffer->stream);
            }
            free(paquete->buffer);
        }
        free(paquete);
    }
}

bool limpiar_buffer_completo(int socket_fd) {
    char buffer[1024];
    int bytes_residuales = 0;
    int total_limpiado = 0;
    
    // Usar MSG_DONTWAIT para no bloquear
    while((bytes_residuales = recv(socket_fd, buffer, sizeof(buffer), MSG_DONTWAIT)) > 0) {
        total_limpiado += bytes_residuales;
        log_debug(logger, "Limpiados %d bytes residuales", bytes_residuales);
    }
    
    if(total_limpiado > 0) {
        log_info(logger, "Buffer limpiado completamente: %d bytes total", total_limpiado);
    }
    
    return total_limpiado > 0;
}

// Función específica para limpiar buffer antes de operaciones críticas
bool limpiar_buffer_antes_operacion(int socket_fd, const char* operacion) {
    bool habia_datos = limpiar_buffer_completo(socket_fd);
    
    if(habia_datos) {
        log_warning(logger, "Se encontraron datos residuales antes de %s en socket %d", operacion, socket_fd);
    }
    
    return habia_datos;
}