#ifndef MASTER_H_
#define MASTER_H_

#include <conexion.h>

// ORDEN CORRECTO para evitar deadlocks: queries -> workers -> planificacion
#define LOCK_QUERIES() pthread_mutex_lock(&mutex_queries)
#define LOCK_WORKERS() pthread_mutex_lock(&mutex_workers)  
#define LOCK_PLANIFICACION() pthread_mutex_lock(&mutex_planificacion)

#define UNLOCK_QUERIES() pthread_mutex_unlock(&mutex_queries)
#define UNLOCK_WORKERS() pthread_mutex_unlock(&mutex_workers)
#define UNLOCK_PLANIFICACION() pthread_mutex_unlock(&mutex_planificacion)


// ==================== ESTRUCTURAS ====================

typedef enum {
    QUERY_READY,
    QUERY_EXEC,
    QUERY_EXIT
} t_estado_query;

typedef struct {
    int query_id;
    int prioridad;
    int prioridad_original;
    char* path_query;
    int socket_query_control;
    int worker_asignado;
    t_estado_query estado;
    int pc;
    int64_t tiempo_ingreso_ready;
    bool cancelada;
    int ciclos_en_ready;
} t_query;

typedef struct {
    int worker_id;
    char* worker_id_str; // ID como string (opcional)
    int socket_worker;
    bool ocupado;
    bool conectado;
    int query_actual;
} t_worker;

typedef struct {
    int puerto_escucha;
    char* algoritmo_planificacion;
    int tiempo_aging;
    char* log_level;
} t_config_master;

typedef struct {
    int socket_cliente;
    op_code tipo_modulo;
} t_args_hilo;

// ==================== VARIABLES GLOBALES ====================

extern t_log* logger;
extern t_config_master* config_global;
extern t_list* lista_queries_ready;
extern t_list* lista_workers;
extern t_queue* cola_queries_ready;
extern int contador_query_id;
extern int contador_worker_id;
extern pthread_mutex_t mutex_queries;
extern pthread_mutex_t mutex_workers;
extern pthread_mutex_t mutex_planificacion;
extern pthread_t hilo_aging;
extern bool sistema_activo;

// ==================== CONFIGURACIÓN ====================

t_config_master* cargar_configuracion_master(char* path_config);
void destruir_config_master(t_config_master* config);

// ==================== INICIALIZACIÓN ====================

void inicializar_estructuras_master(void);
void destruir_estructuras_master(void);
void iniciar_servidor_master(void);

// ==================== HILOS ====================

void* atender_query_control(void* args);
void* atender_worker(void* args);
void* proceso_aging(void* args);

// ==================== PLANIFICACIÓN ====================

void planificar_siguiente_query(void);
void enviar_query_a_worker(t_query* query, t_worker* worker);
void desalojar_query_de_worker(t_worker* worker, const char* motivo);
t_worker* obtener_worker_libre(void);
t_worker* seleccionar_worker_prioridades(t_query* query_nueva);
t_worker* obtener_worker_con_menor_prioridad(void);
t_query* obtener_query_mayor_prioridad(void);
void actualizar_prioridad_query(t_query* query, int nueva_prioridad);

// ==================== GESTIÓN DE QUERIES ====================

t_query* crear_query(char* path_query, int prioridad, int socket_qc);
void agregar_query_a_ready(t_query* query);
void mover_query_a_exec(t_query* query, int worker_id);
void finalizar_query(t_query* query, const char* motivo);
void cancelar_query(t_query* query);
t_query* buscar_query_por_id(int query_id);
t_query* buscar_query_por_socket(int socket_qc);
void eliminar_query(t_query* query);

// ==================== GESTIÓN DE WORKERS ====================

void procesar_mensaje_lectura(t_worker* worker);
void procesar_resultado_read(t_worker* worker);
void procesar_end_worker(t_worker* worker);
void mostrar_resultado_read_consola(uint32_t query_id, int worker_id, const char* file_tag, uint32_t size_datos, char* datos);

t_worker* crear_worker(int socket_worker);
void agregar_worker(t_worker* worker);
void eliminar_worker(t_worker* worker);
t_worker* buscar_worker_por_id(int worker_id);
void procesar_mensajes_worker(t_worker* worker);
void procesar_lectura_worker(t_log* logger, t_worker* worker, void* buffer);
void procesar_finalizacion_worker(t_worker* worker);
void procesar_error_worker(t_worker* worker);
void manejar_desconexion_worker_inmediata(t_worker* worker);

// ==================== COMUNICACIÓN ====================

void recibir_solicitud_query_control(int socket_qc, char** path_query, int* prioridad);
void enviar_query_a_ejecutar(t_worker* worker, t_query* query);
void solicitar_desalojo_worker(t_worker* worker);
int recibir_contexto_desalojo(t_worker* worker);
void enviar_finalizacion_a_query_control(int socket_qc, const char* motivo);

// ==================== LOGGING ====================

void logging_conexion_query_control(t_query* query);
void logging_conexion_worker(t_worker* worker);
void logging_desconexion_query_control(t_query* query);
void logging_desconexion_worker(t_worker* worker, int query_id);
void logging_envio_query(t_query* query, int worker_id);
void logging_desalojo_query(t_query* query, int worker_id, const char* motivo);
void logging_cambio_prioridad(int query_id, int prioridad_anterior, int prioridad_nueva);
void logging_finalizacion_query(int query_id, int worker_id);
void logging_envio_lectura(int query_id, int worker_id);

// ==================== UTILIDADES ====================

int contar_queries_en_exec(void);
int64_t temporal_get_timestamp(void);

#endif /* MASTER_H_ */