#include "master.h"
#include <cliente.h>
#include <server.h>

// VARIABLES GLOBALES
t_log* logger = NULL;
t_config_master* config_global = NULL;
t_list* lista_queries_ready = NULL;
t_list* lista_workers = NULL;
t_queue* cola_queries_ready = NULL;
int contador_query_id = 0;
int contador_worker_id = 0;
pthread_mutex_t mutex_queries;
pthread_mutex_t mutex_workers;
pthread_mutex_t mutex_planificacion;
pthread_t hilo_aging;
bool sistema_activo = true;
t_list* lista_todas_queries = NULL;  // Lista para todas las queries

// FUNCIONES AUXILIARES
void* proceso_aging(void* args) {
    (void)args;  // no usado
    log_info(logger, "Proceso de aging iniciado");
    
    while (sistema_activo) {
        usleep(config_global->tiempo_aging * 1000); // Convertir ms a microsegundos
        
        pthread_mutex_lock(&mutex_queries);

        bool hubo_cambios = false;
        int cantidad_queries_ready = 0;
        
        // Incrementar ciclos y aplicar aging a queries en READY
        for (int i = 0; i < list_size(lista_queries_ready); i++) {
            t_query* query = list_get(lista_queries_ready, i);
            
            if (query->estado == QUERY_READY) {
                cantidad_queries_ready++;
                
                // Incrementar contador de ciclos
                query->ciclos_en_ready++;
                
                // Solo aplicar aging si tiene prioridad > 0
                if (query->prioridad > 0) {
                    // Aplicar aging cada ciclo (reducir en 1)
                    if (query->ciclos_en_ready >= 1) {
                        int prioridad_anterior = query->prioridad;
                        int nueva_prioridad = query->prioridad - 1;
                        
                        if (nueva_prioridad < 0) {
                            nueva_prioridad = 0;  // No puede ser menor que 0
                        }
                        
                        actualizar_prioridad_query(query, nueva_prioridad);
                        query->ciclos_en_ready = 0; // Reiniciar contador
                        
                        hubo_cambios = true;
                        
                        log_info(logger, "┌────────────────────────────────────────┐");
                        log_info(logger, "│         AGING APLICADO                 │");
                        log_info(logger, "├────────────────────────────────────────┤");
                        log_info(logger, "│ Query ID:  %-27d │", query->query_id);
                        log_info(logger, "│ Prioridad: %d -> %-21d │", prioridad_anterior, nueva_prioridad);
                        log_info(logger, "└────────────────────────────────────────┘");
                    }
                } else {
                    log_debug(logger, "Query %d ya tiene prioridad 0 (máxima)", query->query_id);
                }
            }
        }
        
        pthread_mutex_unlock(&mutex_queries);
        
        // Replanificar solo si hubo cambios de prioridad
        if (hubo_cambios) {
            log_info(logger, "Aging: Replanificando por cambios de prioridad...");
            planificar_siguiente_query();
        } else if (cantidad_queries_ready > 0) {
            log_debug(logger, "Aging: %d queries en READY pero sin cambios de prioridad", 
                     cantidad_queries_ready);
        }
    }
    
    log_info(logger, "Proceso de aging finalizado");
    return NULL;
}

t_query* obtener_query_mayor_prioridad(void) {
    if (list_is_empty(lista_queries_ready)) {
        return NULL;
    }
    
    t_query* query_mayor = NULL;
    int index_mayor = -1;
    int mayor_prioridad = 999999;
   
    // Buscar la query con MENOR número (mayor prioridad)
    // En caso de empate, se toma la primera (orden de llegada)
    for (int i = 0; i < list_size(lista_queries_ready); i++) {
        t_query* query = list_get(lista_queries_ready, i);
        
        if (query->estado == QUERY_READY && query->prioridad < mayor_prioridad) {
            mayor_prioridad = query->prioridad;
            query_mayor = query;
            index_mayor = i;
        }
    }
    
    if (query_mayor != NULL && index_mayor != -1) {
        list_remove(lista_queries_ready, index_mayor);
    }
    
    return query_mayor;
}

void actualizar_prioridad_query(t_query* query, int nueva_prioridad) {
    int prioridad_anterior = query->prioridad;
    query->prioridad = nueva_prioridad;
    
    logging_cambio_prioridad(query->query_id, prioridad_anterior, nueva_prioridad);
}


// Versión SIN LOCK - usar solo cuando ya se tiene el mutex
t_query* buscar_query_por_id_unsafe(int query_id) {
    for (int i = 0; i < list_size(lista_todas_queries); i++) {
        t_query* query = list_get(lista_todas_queries, i);
        if (query->query_id == query_id) {
            return query;
        }
    }
    return NULL;
}


void logging_desalojo(int query_id, int worker_id, int pc_recuperado, const char* motivo) {
    log_warning(logger, 
                "[DESALOJO] Query %d desalojada de Worker %d. Motivo: %s. Contexto guardado: PC=%d.",
                query_id, 
                worker_id, 
                motivo,
                pc_recuperado);
}


void desalojar_query_de_worker(t_worker* worker, const char* motivo_log) {
    int query_id_desalojada = worker->query_actual;
    int pc_recuperado = 0;

    log_warning(logger, "🚀 DESALOJANDO Query %d de Worker %d por motivo: %s",
                query_id_desalojada, worker->worker_id, motivo_log);

    // 1. Enviar señal de desalojo al Worker
    solicitar_desalojo_worker(worker); // Usa la función provista: envía DESALOJAR_QUERY

    // 2. Recibir el PC (contexto) desde el Worker
    pc_recuperado = recibir_contexto_desalojo(worker);

    if (pc_recuperado < 0) { // Manejo de error básico si no se recibió un PC válido
        log_error(logger, "Error: PC inválido (%d) recibido para Query %d. Reiniciando PC a 0.",
                   pc_recuperado, query_id_desalojada);
        pc_recuperado = 0;
    } else {
        log_info(logger, "PC recuperado para Query %d: %d", 
                 query_id_desalojada, pc_recuperado);
    }
    
    // 3. Actualizar el estado de la Query
    pthread_mutex_lock(&mutex_queries);
    t_query* query = buscar_query_por_id_unsafe(query_id_desalojada);

    if (query != NULL) {
        // A. Actualizar PC: ESTE ES EL PASO MÁS CRÍTICO
        query->pc = pc_recuperado; 
        
        // B. Devolver a READY
        query->estado = QUERY_READY;
        query->worker_asignado = -1;
        
        // C. Reingresar a la lista de queries listas (si ya no está, para asegurar su re-planificación)
        list_add(lista_queries_ready, query); 

        logging_desalojo(query_id_desalojada, worker->worker_id, pc_recuperado, motivo_log);
        
    } else {
        log_warning(logger, "Query %d no encontrada para desalojo (ya fue finalizada?)", 
                     query_id_desalojada);
    }
    pthread_mutex_unlock(&mutex_queries);

    // 4. Liberar el Worker para re-asignación inmediata
    worker->ocupado = false;
    worker->query_actual = -1;
    log_info(logger, "Worker %d liberado y listo para nueva asignación.", worker->worker_id);

    // El hilo llamador (probablemente planificar_siguiente_query o atender_worker)
    // continuará con la asignación de la query de mayor prioridad.
}

void debug_mostrar_estado_queries(void) {
    //pthread_mutex_lock(&mutex_queries);
    
    log_info(logger, "=== DEBUG: ESTADO COMPLETO DE QUERIES ===");
    log_info(logger, "Queries en READY: %d", list_size(lista_queries_ready));
    log_info(logger, "Queries totales: %d", list_size(lista_todas_queries));
    
    // Mostrar queries en READY con más detalle
    if (list_size(lista_queries_ready) > 0) {
        log_info(logger, "--- QUERIES EN READY ---");
        for (int i = 0; i < list_size(lista_queries_ready); i++) {
            t_query* q = list_get(lista_queries_ready, i);
            log_info(logger, "  [%d] Query %d: path='%s', prioridad=%d, estado=%d, worker=%d", 
                     i, q->query_id, q->path_query, q->prioridad, q->estado, q->worker_asignado);
        }
    } else {
        log_info(logger, "--- NO HAY QUERIES EN READY ---");
    }
    
    // Mostrar todas las queries globales
    if (list_size(lista_todas_queries) > 0) {
        log_info(logger, "--- TODAS LAS QUERIES ---");
        for (int i = 0; i < list_size(lista_todas_queries); i++) {
            t_query* q = list_get(lista_todas_queries, i);
            log_info(logger, "  [%d] Query %d: path='%s', prioridad=%d, estado=%d, worker=%d", 
                     i, q->query_id, q->path_query, q->prioridad, q->estado, q->worker_asignado);
        }
    } else {
        log_info(logger, "--- NO HAY QUERIES GLOBALES ---");
    }
    
    log_info(logger, "=== FIN DEBUG ===");
    
    //pthread_mutex_unlock(&mutex_queries);
}

// INICIALIZACION Y CLEANUP
t_config_master* cargar_configuracion_master(char* path_config) {
    
    // Primero intentar con la ruta tal cual
    FILE* test_file = fopen(path_config, "r");
    if (test_file != NULL) {
        fclose(test_file);
        printf("Archivo encontrado en: %s\n", path_config);
    } else {
        // Si no se encuentra, intentar en el directorio padre
        char path_alternativo[1024];
        snprintf(path_alternativo, sizeof(path_alternativo), "../%s", path_config);
        
        test_file = fopen(path_alternativo, "r");
        if (test_file != NULL) {
            fclose(test_file);
            printf("Archivo encontrado en: %s\n", path_alternativo);
            // Usar la ruta alternativa
            path_config = path_alternativo;
        } else {
            fprintf(stderr, "Error: No se puede encontrar el archivo '%s'\n", path_config);
            fprintf(stderr, "También se buscó en: %s\n", path_alternativo);
            return NULL;
        }
    }
    
    t_config* config = config_create(path_config);
    
    if (config == NULL) {
        fprintf(stderr, "Error: No se pudo abrir el archivo de configuración: %s\n", path_config);
        return NULL;
    }

    printf("Archivo de configuración cargado exitosamente: %s\n", path_config);
    
    t_config_master* config_master = malloc(sizeof(t_config_master));
    
    // Validar y cargar PUERTO_ESCUCHA
    if (!config_has_property(config, "PUERTO_ESCUCHA")) {
        fprintf(stderr, "Error: Falta la propiedad PUERTO_ESCUCHA en el archivo de configuración\n");
        free(config_master);
        config_destroy(config);
        return NULL;
    }
    config_master->puerto_escucha = config_get_int_value(config, "PUERTO_ESCUCHA");

    printf("PUERTO_ESCUCHA: %d\n", config_master->puerto_escucha);
    
    // Validar y cargar ALGORITMO_PLANIFICACION
    if (!config_has_property(config, "ALGORITMO_PLANIFICACION")) {
        fprintf(stderr, "Error: Falta la propiedad ALGORITMO_PLANIFICACION en el archivo de configuración\n");
        free(config_master);
        config_destroy(config);
        return NULL;
    }
    config_master->algoritmo_planificacion = strdup(config_get_string_value(config, "ALGORITMO_PLANIFICACION"));
    
    // Validar y cargar TIEMPO_AGING
    if (!config_has_property(config, "TIEMPO_AGING")) {
        fprintf(stderr, "Error: Falta la propiedad TIEMPO_AGING en el archivo de configuración\n");
        free(config_master->algoritmo_planificacion);
        free(config_master);
        config_destroy(config);
        return NULL;
    }
    config_master->tiempo_aging = config_get_int_value(config, "TIEMPO_AGING");

    printf("TIEMPO_AGING: %d ms\n", config_master->tiempo_aging);
    
    // Validar y cargar LOG_LEVEL
    if (!config_has_property(config, "LOG_LEVEL")) {
        fprintf(stderr, "Error: Falta la propiedad LOG_LEVEL en el archivo de configuración\n");
        free(config_master->algoritmo_planificacion);
        free(config_master);
        config_destroy(config);
        return NULL;
    }
    config_master->log_level = strdup(config_get_string_value(config, "LOG_LEVEL"));

    printf("LOG_LEVEL: %s\n", config_master->log_level);
    
    config_destroy(config);
    return config_master;
}

void destruir_config_master(t_config_master* config) {
    if (config != NULL) {
        if (config->algoritmo_planificacion != NULL) {
            free(config->algoritmo_planificacion);
        }
        if (config->log_level != NULL) {
            free(config->log_level);
        }
        free(config);
    }
}

void inicializar_estructuras_master(void) {
    lista_queries_ready = list_create();
    lista_workers = list_create();
    lista_todas_queries = list_create();
    
    // Verificar que las listas se crearon correctamente
    if (lista_queries_ready == NULL || lista_workers == NULL || lista_todas_queries == NULL) {
        log_error(logger, "Error crítico: No se pudieron crear las listas");
        exit(EXIT_FAILURE);
    }
    
    log_info(logger, "Estructuras inicializadas:");
    log_info(logger, "   - lista_queries_ready: %p (size: %d)", lista_queries_ready, list_size(lista_queries_ready));
    log_info(logger, "   - lista_todas_queries: %p (size: %d)", lista_todas_queries, list_size(lista_todas_queries));
    log_info(logger, "   - lista_workers: %p (size: %d)", lista_workers, list_size(lista_workers));
    
    pthread_mutex_init(&mutex_queries, NULL);
    pthread_mutex_init(&mutex_workers, NULL);
    pthread_mutex_init(&mutex_planificacion, NULL);
    
    log_info(logger, "Mutex inicializados");
}

void destruir_estructuras_master(void) {
pthread_mutex_lock(&mutex_queries);
    if (lista_queries_ready != NULL) {
        list_destroy_and_destroy_elements(lista_queries_ready, (void*)eliminar_query);
    }
    if (lista_todas_queries != NULL) { 
        list_destroy_and_destroy_elements(lista_todas_queries, (void*)eliminar_query);
    }
    if (cola_queries_ready != NULL) {
        queue_destroy(cola_queries_ready);
    }
    pthread_mutex_unlock(&mutex_queries);
    
    pthread_mutex_lock(&mutex_workers);
    if (lista_workers != NULL) {
        list_destroy_and_destroy_elements(lista_workers, (void*)eliminar_worker);
    }
    pthread_mutex_unlock(&mutex_workers);
    
    pthread_mutex_destroy(&mutex_queries);
    pthread_mutex_destroy(&mutex_workers);
    pthread_mutex_destroy(&mutex_planificacion);
    
    log_info(logger, "Estructuras del Master destruidas");
}

void iniciar_servidor_master(void) {
    int socket_servidor = iniciar_servidor(logger, config_global->puerto_escucha);
    
    if (socket_servidor == -1) {
        log_error(logger, "Error al iniciar el servidor Master");
        return;
    }
    
    log_info(logger, "Servidor Master iniciado en puerto %d", config_global->puerto_escucha);
    log_info(logger, "Esperando conexiones de Query Control y Workers...");
    
    while (sistema_activo) {
        int socket_cliente = esperar_cliente(logger, socket_servidor);
        
        if (socket_cliente == -1) {
            log_error(logger, "Error al aceptar cliente");
            continue;
        }

        log_info(logger, "Nuevo cliente conectado - Socket: %d", socket_cliente);
        
        // Recibir handshake para identificar tipo de módulo
        op_code cod_op;
        ssize_t bytes = recv(socket_cliente, &cod_op, sizeof(op_code), MSG_WAITALL);
        
        if (bytes <= 0) {
            log_warning(logger, "Cliente se desconectó antes del handshake");
            close(socket_cliente);
            continue;
        }

        log_info(logger, "Handshake recibido: código %d", cod_op);
        
        // Crear argumentos para el hilo
        t_args_hilo* args = malloc(sizeof(t_args_hilo));
        args->socket_cliente = socket_cliente;
        
        pthread_t hilo_cliente;
        const char* nombre_modulo = "Desconocido";
        
        switch (cod_op) {
            case QUERY_CONTROL:
                nombre_modulo = "Query Control";
                log_info(logger, "## Se conecta el Query Control - Socket: %d", socket_cliente);
                args->tipo_modulo = MASTER;
                pthread_create(&hilo_cliente, NULL, atender_query_control, args);
                pthread_detach(hilo_cliente);
                break;
                
            case WORKER:
                nombre_modulo = "Worker";
                log_info(logger, "## Se conecta un Worker - Socket: %d", socket_cliente);
                args->tipo_modulo = WORKER;
                pthread_create(&hilo_cliente, NULL, atender_worker, args);
                pthread_detach(hilo_cliente);
                break;

            case HANDSHAKE_WORKER:  // Código 102 - Handshake específico
                nombre_modulo = "Worker";
                log_info(logger, "## Se conecta un Worker (HANDSHAKE_WORKER=102) - Socket: %d", socket_cliente);
                args->tipo_modulo = WORKER;
                pthread_create(&hilo_cliente, NULL, atender_worker, args);
                pthread_detach(hilo_cliente);
                break;

            case HANDSHAKE_QUERY_CONTROL:
                nombre_modulo = "Query Control";
                log_info(logger, "## Se conecta el Query Control - Socket: %d", socket_cliente);

                // 1️. ENVIAR CONFIRMACION
                op_code conf = CONFIRMATION;
                send(socket_cliente, &conf, sizeof(op_code), 0);

                // 2️. Crear hilo
                args->tipo_modulo = QUERY_CONTROL;
                pthread_create(&hilo_cliente, NULL, atender_query_control, args);
                pthread_detach(hilo_cliente);
                break;
                
            default:
                log_warning(logger, "Tipo de módulo no reconocido: %d", cod_op);
                close(socket_cliente);
                free(args);
                break;
        }

        // Log adicional mostrando cantidad actual de módulos conectados
        if (cod_op == QUERY_CONTROL || cod_op == WORKER || cod_op == HANDSHAKE_WORKER) {
            pthread_mutex_lock(&mutex_workers);
            int cantidad_workers = list_size(lista_workers);
            pthread_mutex_unlock(&mutex_workers);
            
            log_info(logger, "## %s conectado - Cantidad de Workers: %d", 
                     nombre_modulo, cantidad_workers);
        }
    }
    
    close(socket_servidor);
}


// QUERY
void recibir_solicitud_query_control(int socket_qc, char** path_query, int* prioridad) {
    log_info(logger, "Recibiendo solicitud de Query Control...");
    
    // 1. Recibir tamaño del path Y CONVERTIR DE NETWORK BYTE ORDER
    uint32_t tam_path_network;
    ssize_t bytes_recv = recv(socket_qc, &tam_path_network, sizeof(uint32_t), MSG_WAITALL);
    
    if (bytes_recv <= 0) {
        if (bytes_recv == 0) {
            log_info(logger, "Query Control cerró la conexión");
        } else {
            log_error(logger, "Error al recibir tamaño del path: %s", strerror(errno));
        }
        *path_query = NULL;
        return;
    }
    
    uint32_t tam_path = ntohl(tam_path_network);  // ← AGREGAR CONVERSIÓN
    log_info(logger, "Tamaño del path recibido: %u bytes", tam_path);
    
    if (tam_path == 0 || tam_path > 10000) {
        log_error(logger, "Tamaño de path inválido: %u bytes", tam_path);
        *path_query = NULL;
        return;
    }
    
    // 2. Recibir el path
    *path_query = malloc(tam_path);
    bytes_recv = recv(socket_qc, *path_query, tam_path, MSG_WAITALL);
    
    if (bytes_recv <= 0) {
        log_error(logger, "Error al recibir path del paquete");
        free(*path_query);
        *path_query = NULL;
        return;
    }
    
    log_info(logger, "Path recibido: '%s'", *path_query);
    
    // 3. Recibir la prioridad Y CONVERTIR
    int prioridad_network;
    bytes_recv = recv(socket_qc, &prioridad_network, sizeof(int), MSG_WAITALL);
    
    if (bytes_recv <= 0) {
        log_error(logger, "Error al recibir prioridad");
        free(*path_query);
        *path_query = NULL;
        return;
    }
    
    *prioridad = ntohl(prioridad_network);  // ← AGREGAR CONVERSIÓN
    log_info(logger, "Prioridad recibida: %d", *prioridad);
    log_info(logger, "Solicitud de Query Control procesada exitosamente");
}


void* atender_query_control(void* args) {
    t_args_hilo* argumentos = (t_args_hilo*)args;
    int socket_qc = argumentos->socket_cliente;
    free(argumentos);
    
    log_info(logger, "=== INICIANDO ATENCIÓN QUERY CONTROL (Socket %d) ===", socket_qc);
    
    while (sistema_activo) {
        char* path_query = NULL;
        int prioridad = 0;

        log_info(logger, "Esperando solicitud de Query Control (Socket %d)...", socket_qc);
        
        recibir_solicitud_query_control(socket_qc, &path_query, &prioridad);
        
        if (path_query == NULL) {
            log_warning(logger, "Query Control (Socket %d) envió datos inválidos o cerró conexión", socket_qc);
            break;
        }

        log_info(logger, "📨 QUERY RECIBIDA - Path: %s, Prioridad: %d", path_query, prioridad);
        
        // Crear nueva query
        t_query* nueva_query = crear_query(path_query, prioridad, socket_qc);
        free(path_query);
        
        if (nueva_query == NULL) {
            log_error(logger, "❌ Error al crear query");
            continue;
        }
        
        log_info(logger, "Query %d creada exitosamente", nueva_query->query_id);
        
        // AGREGAR DIAGNÓSTICO DETALLADO
        LOCK_QUERIES();
        int queries_antes = list_size(lista_queries_ready);
        UNLOCK_QUERIES();
        
        log_info(logger, "Agregando Query %d a READY (antes: %d queries)", 
                 nueva_query->query_id, queries_antes);
        
        agregar_query_a_ready(nueva_query);
        
        // VERIFICAR QUE SE AGREGÓ
        LOCK_QUERIES();
        int queries_despues = list_size(lista_queries_ready);
        UNLOCK_QUERIES();
        
        if (queries_despues > queries_antes) {
            log_info(logger, "Query %d agregada exitosamente a READY. Total: %d", 
                     nueva_query->query_id, queries_despues);
        } else {
            log_error(logger, "Query %d NO se agregó a READY", nueva_query->query_id);
            // Forzar planificación de todos modos
        }

        debug_mostrar_estado_queries();
        
        // Planificar inmediatamente
        log_info(logger, "Ejecutando planificación para Query %d", nueva_query->query_id);
        planificar_siguiente_query();
    }
    
    log_info(logger, "=== FIN ATENCIÓN QUERY CONTROL (Socket %d) ===", socket_qc);
    close(socket_qc);
    return NULL;
}

t_query* crear_query(char* path_query, int prioridad, int socket_qc) {
    log_info(logger, "CREAR_QUERY - Iniciando creación...");
    log_info(logger, "   - Path: '%s'", path_query);
    log_info(logger, "   - Prioridad: %d", prioridad);
    log_info(logger, "   - Socket QC: %d", socket_qc);
    
    t_query* query = malloc(sizeof(t_query));
    if (query == NULL) {
        log_error(logger, "Error al allocar memoria para query");
        return NULL;
    }
    log_info(logger, "   - Memoria allocada para query struct");
    
    query->query_id = contador_query_id++;
    query->prioridad = prioridad;
    query->prioridad_original = prioridad;
    
    log_info(logger, "   - Query ID asignado: %d", query->query_id);
    
    // Copiar path
    query->path_query = strdup(path_query);
    if (query->path_query == NULL) {
        log_error(logger, "Error al copiar path_query");
        free(query);
        return NULL;
    }
    log_info(logger, "   - Path copiado: '%s'", query->path_query);
    
    query->socket_query_control = socket_qc;
    query->worker_asignado = -1;
    query->estado = QUERY_READY;
    query->pc = 0;
    query->ciclos_en_ready = 0;
    query->cancelada = false;

    log_info(logger, "   - Campos inicializados");

    // AGREGAR A LISTA GLOBAL
    pthread_mutex_lock(&mutex_queries);
    log_info(logger, "   - Mutex queries adquirido");
    
    list_add(lista_todas_queries, query);
    int total_queries_global = list_size(lista_todas_queries);
    
    pthread_mutex_unlock(&mutex_queries);
    log_info(logger, "   - Mutex queries liberado");
    
    log_info(logger, "Query %d creada exitosamente - Path: '%s', Prioridad: %d, Total queries global: %d", 
             query->query_id, query->path_query, query->prioridad, total_queries_global);
    
    logging_conexion_query_control(query);
    return query;
}

void agregar_query_a_ready(t_query* query) {
    log_info(logger, "📥 AGREGAR_QUERY_A_READY - Iniciando para Query %d", query->query_id);
    
    pthread_mutex_lock(&mutex_queries);
    log_info(logger, "   - Mutex queries adquirido en agregar_query_a_ready");
    
    // Verificar estado actual de la lista
    int queries_antes = list_size(lista_queries_ready);
    log_info(logger, "   - Queries en READY antes: %d", queries_antes);
    
    // Verificar que la query no esté ya en la lista
    bool ya_existe = false;
    for (int i = 0; i < list_size(lista_queries_ready); i++) {
        t_query* q = list_get(lista_queries_ready, i);
        if (q->query_id == query->query_id) {
            ya_existe = true;
            break;
        }
    }
    
    if (ya_existe) {
        log_warning(logger, "   - Query %d ya está en lista_queries_ready", query->query_id);
    } else {
        query->ciclos_en_ready = 0;
        list_add(lista_queries_ready, query);
        log_info(logger, "   - Query agregada a lista_queries_ready");
    }
    
    // Verificar que se agregó
    int queries_despues = list_size(lista_queries_ready);
    log_info(logger, "   - Queries en READY después: %d", queries_despues);
    
    if (queries_despues > queries_antes) {
        log_info(logger, "Query %d agregada exitosamente a READY", query->query_id);
    } else {
        log_error(logger, "Query %d NO se agregó a READY", query->query_id);
    }
    
    pthread_mutex_unlock(&mutex_queries);
    log_info(logger, "   - Mutex queries liberado en agregar_query_a_ready");
    
    // Asegurar que se llama a planificación
    log_info(logger, "Llamando a planificar_siguiente_query...");
    planificar_siguiente_query();
}

void mover_query_a_exec(t_query* query, int worker_id) {
    query->estado = QUERY_EXEC;
    query->worker_asignado = worker_id;
    query->ciclos_en_ready = 0;  // Reiniciar al pasar a EXEC
    
    log_info(logger, "Query %d: READY → EXEC (Worker %d asignado)", query->query_id, worker_id);
}

void enviar_query_a_worker_sin_lock(t_query* query, t_worker* worker) {
    log_info(logger, "ENVIANDO QUERY %d AL WORKER %d (sin lock)", query->query_id, worker->worker_id);
    
    // Verificar que el worker esté conectado
    if (!worker->conectado) {
        log_error(logger, "Worker %d no está conectado", worker->worker_id);
        return;
    }
    
    // Verificar socket
    if (worker->socket_worker <= 0) {
        log_error(logger, "Socket inválido del Worker %d: %d", worker->worker_id, worker->socket_worker);
        return;
    }
    
    // ✅ MARCAR WORKER COMO OCUPADO (ya tenemos el mutex)
    worker->ocupado = true;
    worker->query_actual = query->query_id;
    
    // Enviar query al worker
    enviar_query_a_ejecutar(worker, query);
    
    // Mover query a estado EXEC
    query->estado = QUERY_EXEC;
    query->worker_asignado = worker->worker_id;
    query->ciclos_en_ready = 0;
    
    logging_envio_query(query, worker->worker_id);
    
    log_info(logger, "Query %d enviada exitosamente al Worker %d", query->query_id, worker->worker_id);
}

void planificar_siguiente_query(void) {
    log_info(logger, "INICIANDO PLANIFICACIÓN - Buscando query para ejecutar");

    // ORDEN CORRECTO: queries -> workers -> planificacion
    LOCK_QUERIES();
    LOCK_WORKERS();
    LOCK_PLANIFICACION();
    
    int queries_ready = list_size(lista_queries_ready);
    int workers_total = list_size(lista_workers);
    int workers_libres = 0;
    
    log_info(logger, "ESTADO ACTUAL PARA PLANIFICACIÓN:");
    log_info(logger, "   - Queries en READY: %d", queries_ready);
    log_info(logger, "   - Workers totales: %d", workers_total);

    // Contar workers libres y mostrar estado detallado
    for (int i = 0; i < workers_total; i++) {
        t_worker* w = list_get(lista_workers, i);
        log_info(logger, "   - Worker %d: ocupado=%s, conectado=%s, query_actual=%d", 
                 w->worker_id, 
                 w->ocupado ? "SI" : "NO",
                 w->conectado ? "SI" : "NO", 
                 w->query_actual);
        // VERIFICAR QUE ESTÉ CONECTADO Y LIBRE
        if (w->conectado && !w->ocupado) {
            workers_libres++;
        }
    }

    // Después de contar workers_libres, agregar log de diagnóstico
    log_info(logger, "Workers válidos y libres: %d", workers_libres);

    if (queries_ready == 0) {
        log_info(logger, "❌ No hay queries en READY");
        goto cleanup;
    }

    if (workers_libres == 0) {
        log_info(logger, "❌ No hay workers libres");
        goto cleanup;
    }

    log_info(logger, "✅ Condiciones OK: %d queries, %d workers libres", 
             queries_ready, workers_libres);
    
    int queries_enviadas = 0;

    // ✅ ENVIAR QUERIES A TODOS LOS WORKERS LIBRES
    while (workers_libres > 0 && queries_ready > 0) {
        t_query* query_a_ejecutar = NULL;
        t_worker* worker_asignado = NULL;
        
        // Obtener query según algoritmo
        if (strcmp(config_global->algoritmo_planificacion, "FIFO") == 0) {
            if (!list_is_empty(lista_queries_ready)) {
                query_a_ejecutar = list_get(lista_queries_ready, 0);
                list_remove(lista_queries_ready, 0);
                log_info(logger, "📝 FIFO - Query seleccionada: %d", query_a_ejecutar->query_id);
            }
            
            // Obtener primer worker CONECTADO y libre
            for (int i = 0; i < workers_total; i++) {
                t_worker* w = list_get(lista_workers, i);
                if (w->conectado && !w->ocupado) {
                    worker_asignado = w;
                    break;
                }
            }
        } 
        else if (strcmp(config_global->algoritmo_planificacion, "PRIORIDADES") == 0) {
            query_a_ejecutar = obtener_query_mayor_prioridad();
            if (query_a_ejecutar != NULL) {
                log_info(logger, "📝 PRIORIDADES - Query: %d (prioridad: %d)", 
                        query_a_ejecutar->query_id, query_a_ejecutar->prioridad);
                
                worker_asignado = seleccionar_worker_prioridades(query_a_ejecutar);
                
                if (worker_asignado == NULL) {
                    log_info(logger, "No hay worker disponible para Query %d", 
                            query_a_ejecutar->query_id);
                    list_add(lista_queries_ready, query_a_ejecutar);
                    break;
                }
            }
        }
        
        if (query_a_ejecutar == NULL || worker_asignado == NULL) {
            log_error(logger, "❌ No se pudo asignar query a worker");
            if (query_a_ejecutar != NULL) {
                list_add(lista_queries_ready, query_a_ejecutar);
            }
            break;
        }
        
        // ✅ VERIFICAR UNA VEZ MÁS QUE EL WORKER SIGA CONECTADO
        if (!worker_asignado->conectado) {
            log_warning(logger, "⚠️  Worker %d se desconectó antes de asignar query",
                       worker_asignado->worker_id);
            list_add(lista_queries_ready, query_a_ejecutar);
            workers_libres--;
            continue;
        }
        
        log_info(logger, "🔗 ASIGNANDO Query %d → Worker %d", 
                query_a_ejecutar->query_id, worker_asignado->worker_id);
        
        // ENVIAR QUERY AL WORKER
        enviar_query_a_worker_sin_lock(query_a_ejecutar, worker_asignado);
        queries_enviadas++;

        workers_libres--;
        queries_ready--;
        
        log_info(logger, "✅ Query %d enviada al Worker %d",
                query_a_ejecutar->query_id, worker_asignado->worker_id);
    }
        
    if (queries_enviadas > 0) {
        log_info(logger, "✅ PLANIFICACIÓN EXITOSA - %d queries enviadas", queries_enviadas);
    } else {
        log_info(logger, "❌ No se enviaron queries");
    }
    
cleanup:
    UNLOCK_PLANIFICACION();
    UNLOCK_WORKERS();
    UNLOCK_QUERIES();
    
    log_info(logger, "🏁 FIN PLANIFICACIÓN");
}

void finalizar_query(t_query* query, const char* motivo) {
    query->estado = QUERY_EXIT;
    
    // Verificar que el socket del Query Control sigue conectado
    if (query->socket_query_control != -1) {
        // Verificar si el socket sigue válido
        struct stat socket_stat;
        if (fstat(query->socket_query_control, &socket_stat) == 0) {
            // Socket válido, enviar finalización
            enviar_finalizacion_a_query_control(query->socket_query_control, motivo);
            log_info(logger, "Finalización enviada a Query Control para query %d: %s", 
                     query->query_id, motivo);
        } else {
            log_info(logger, "Query Control desconectado, no se envía finalización para query %d", 
                     query->query_id);
        }
    } else {
        log_warning(logger, "Query %d no tiene socket de Query Control asociado", query->query_id);
    }
    
    if (query->worker_asignado != -1) {
        logging_finalizacion_query(query->query_id, query->worker_asignado);
    }
    
    // NO se elimina la query de lista_todas_queries inmediatamente (podría necesitarse para logs o estadísticas)
}

void cancelar_query(t_query* query) {
    query->cancelada = true;
    
    if (query->estado == QUERY_READY) {
        finalizar_query(query, "Query Control desconectado");
    } else if (query->estado == QUERY_EXEC) {
        t_worker* worker = buscar_worker_por_id(query->worker_asignado);
        if (worker != NULL) {
            desalojar_query_de_worker(worker, "DESCONEXION");
        }
        finalizar_query(query, "Query Control desconectado");
    }
}

// Versión CON LOCK - usar cuando NO se tiene el mutex
t_query* buscar_query_por_id(int query_id) {
    pthread_mutex_lock(&mutex_queries);
    
    t_query* query = buscar_query_por_id_unsafe(query_id);
    
    pthread_mutex_unlock(&mutex_queries);
    return query;
}

t_query* buscar_query_por_socket(int socket_qc) {
    for (int i = 0; i < list_size(lista_queries_ready); i++) {
        t_query* query = list_get(lista_queries_ready, i);
        if (query->socket_query_control == socket_qc) {
            return query;
        }
    }
    return NULL;
}

void eliminar_query(t_query* query) {
    if (query != NULL) {
        // Remover de todas las listas
        pthread_mutex_lock(&mutex_queries);
        list_remove_element(lista_queries_ready, query);
        list_remove_element(lista_todas_queries, query);
        pthread_mutex_unlock(&mutex_queries);
        
        if (query->path_query != NULL) {
            free(query->path_query);
        }
        free(query);
    }
}


// WORKER
t_worker* crear_worker(int socket_worker) {
    t_worker* worker = malloc(sizeof(t_worker));
    
    worker->worker_id = contador_worker_id++;  // Valor por defecto
    worker->worker_id_str = NULL;              // Inicializar como NULL
    worker->socket_worker = socket_worker;
    worker->ocupado = false;
    worker->conectado = true;
    worker->query_actual = -1;
    
    return worker;
}

void agregar_worker(t_worker* worker) {
    LOCK_WORKERS();
    list_add(lista_workers, worker);
    
    // DEBUG: Mostrar estado actual
    log_info(logger, "## Worker agregado - ID: %d | Total Workers: %d", 
             worker->worker_id, list_size(lista_workers));
    
    // Mostrar todos los workers
    for (int i = 0; i < list_size(lista_workers); i++) {
        t_worker* w = list_get(lista_workers, i);
        log_info(logger, "   - Worker %d: socket=%d, conectado=%d, ocupado=%d", 
                 w->worker_id, w->socket_worker, w->conectado, w->ocupado);
    }
    
    UNLOCK_WORKERS();
}

void* atender_worker(void* args) {
    t_args_hilo* argumentos = (t_args_hilo*)args;
    int socket_worker = argumentos->socket_cliente;
    free(argumentos);
    
    // Recibir el ID del worker después del handshake
    char* worker_id = NULL;
    
    // Recibir tamaño del ID
    uint32_t tam_id_network;
    ssize_t bytes_recv = recv(socket_worker, &tam_id_network, sizeof(uint32_t), MSG_WAITALL);
    
    if (bytes_recv > 0) {
        uint32_t tam_id = ntohl(tam_id_network);
        
        if (tam_id > 0 && tam_id < 1024) { // Límite razonable
            worker_id = malloc(tam_id);
            bytes_recv = recv(socket_worker, worker_id, tam_id, MSG_WAITALL);
            
            if (bytes_recv > 0) {
                // Asegurar terminación NULL
                if (worker_id[tam_id - 1] != '\0') {
                    char* temp = realloc(worker_id, tam_id + 1);
                    if (temp) {
                        worker_id = temp;
                        worker_id[tam_id] = '\0';
                    }
                }
                log_info(logger, "Worker identificado con ID: %s", worker_id);
            } else {
                free(worker_id);
                worker_id = NULL;
            }
        }
    }
    
    // Si no se recibió ID, usar uno por defecto basado en contador
    if (worker_id == NULL) {
        worker_id = malloc(16);
        snprintf(worker_id, 16, "%d", contador_worker_id);
        log_info(logger, "No se recibió ID, usando ID por defecto: %s", worker_id);
    }

    // Enviar confirmación al worker
    op_code cod_op_confirmacion = CONFIRMATION;
    if (send(socket_worker, &cod_op_confirmacion, sizeof(op_code), MSG_NOSIGNAL) <= 0) {
        log_warning(logger, "Error al enviar confirmación al Worker");
        close(socket_worker);
        free(worker_id);
        return NULL;
    }
    
    log_info(logger, "Confirmación enviada al Worker (CONFIRMATION=103)");

    // Crear worker CON EL ID RECIBIDO
    //pthread_mutex_lock(&mutex_workers);
    t_worker* worker = crear_worker(socket_worker);
    
    // Asignar el ID recibido
    if (worker_id != NULL) {
        int id_numerico = atoi(worker_id);
        if (id_numerico > 0) {
            worker->worker_id = id_numerico;
            if (id_numerico >= contador_worker_id) {
                contador_worker_id = id_numerico + 1;
            }
        }
        log_info(logger, "Worker %d asociado con ID recibido: %s", worker->worker_id, worker_id);
        free(worker_id);
    }

    agregar_worker(worker);
    //pthread_mutex_unlock(&mutex_workers);
    
    // Log de conexión
    logging_conexion_worker(worker);
    
    // FORZAR PLANIFICACIÓN INMEDIATA AL CONECTAR WORKER
    log_info(logger, "🔄 Worker conectado - Ejecutando planificación inmediata");
    planificar_siguiente_query();
    
    // PROCESAR MENSAJES (PUEDE DURAR MUCHO TIEMPO)
    procesar_mensajes_worker(worker);

    // AL SALIR DEL LOOP, YA SE MANEJÓ LA DESCONEXIÓN
    log_info(logger, "Hilo de Worker %d terminando", worker->worker_id);

    return NULL;
}

t_worker* obtener_worker_libre(void) {
    log_info(logger, "🔍 Buscando worker libre...");
    
    for (int i = 0; i < list_size(lista_workers); i++) {
        t_worker* worker = list_get(lista_workers, i);
        
        if (!worker->ocupado && worker->conectado) {
            log_info(logger, "Worker %d encontrado (libre y conectado)", worker->worker_id);
            
            // MARCAR COMO OCUPADO INMEDIATAMENTE para evitar que otro hilo lo tome
            worker->ocupado = true;
            worker->query_actual = -2; // Estado temporal
            
            return worker;
        } else {
            log_debug(logger, "Worker %d no disponible (ocupado=%d, conectado=%d)", 
                     worker->worker_id, worker->ocupado, worker->conectado);
        }
    }
    
    log_info(logger, "❌ No se encontraron workers libres");
    return NULL;
}

t_worker* seleccionar_worker_prioridades(t_query* query_nueva) {
    t_worker* worker_libre = obtener_worker_libre();
    
    if (worker_libre != NULL) {
        return worker_libre;
    }
    
    // No hay workers libres, buscar uno con menor prioridad
    t_worker* worker_menor_prioridad = obtener_worker_con_menor_prioridad();
    
    if (worker_menor_prioridad != NULL) {
        t_query* query_actual = buscar_query_por_id(worker_menor_prioridad->query_actual);
        
        if (query_actual != NULL && query_nueva->prioridad < query_actual->prioridad) {
            // Desalojar query actual
            desalojar_query_de_worker(worker_menor_prioridad, "PRIORIDAD");
            return worker_menor_prioridad;
        }
    }
    
    return NULL;
}

t_worker* obtener_worker_con_menor_prioridad(void) {
    t_worker* worker_menor = NULL;
    int menor_prioridad = -1;  // Buscar el MAYOR número (menor prioridad)
    
    for (int i = 0; i < list_size(lista_workers); i++) {
        t_worker* worker = list_get(lista_workers, i);
        
        if (worker->ocupado && worker->conectado) {
            t_query* query = buscar_query_por_id(worker->query_actual);
            
            if (query != NULL) {
                // Buscar la query con MAYOR número (menor prioridad)
                if (menor_prioridad == -1 || query->prioridad > menor_prioridad) {
                    menor_prioridad = query->prioridad;
                    worker_menor = worker;
                }
            }
        }
    }
    
    if (worker_menor != NULL) {
        log_debug(logger, "Worker con menor prioridad: Worker %d (prioridad: %d)", 
                 worker_menor->worker_id, menor_prioridad);
    }
    
    return worker_menor;
}

t_worker* buscar_worker_por_id(int worker_id) {
    for (int i = 0; i < list_size(lista_workers); i++) {
        t_worker* worker = list_get(lista_workers, i);
        if (worker->worker_id == worker_id) {
            return worker;
        }
    }
    return NULL;
}

// Función para manejar desconexión de Query Control
void manejar_desconexion_query_control(int socket_qc) {
    pthread_mutex_lock(&mutex_queries);
    
    // Buscar todas las queries de este Query Control
    for (int i = list_size(lista_queries_ready) - 1; i >= 0; i--) {
        t_query* query = list_get(lista_queries_ready, i);
        if (query->socket_query_control == socket_qc) {
            log_info(logger, "Cancelando query %d por desconexión de Query Control", 
                     query->query_id);
            logging_desconexion_query_control(query);
            cancelar_query(query);
        }
    }
    
    pthread_mutex_unlock(&mutex_queries);
}

void mostrar_resultado_read_consola(uint32_t query_id, int worker_id, const char* file_tag, uint32_t size_datos, char* datos) {
    printf("\n");
    printf("────────────────────────────────────────────────────────────────────────────────\n");
    printf("                     RESULTADO READ - MASTER\n");
    printf("────────────────────────────────────────────────────────────────────────────────\n");
    printf("Query ID:  %u\n", query_id);
    printf("Worker ID: %d\n", worker_id);
    printf("File:      %s\n", file_tag);
    printf("Tamaño:    %u bytes\n", size_datos);
    printf("────────────────────────────────────────────────────────────────────────────────\n");
    printf("Datos leídos:\n");
    printf("\n");
    
    // Procesar el buffer para concatenar bloques y manejar \0 como separadores
    char* buffer_procesado = malloc(size_datos + 1);
    uint32_t pos_procesado = 0;
    uint32_t pos_original = 0;
    
    while (pos_original < size_datos) {
        // Buscar el próximo carácter no nulo
        uint32_t inicio_linea = pos_original;
        while (inicio_linea < size_datos && datos[inicio_linea] == '\0') {
            inicio_linea++;
        }
        
        if (inicio_linea >= size_datos) break;
        
        // Encontrar el final de esta "línea" (próximo \0 o fin del buffer)
        uint32_t fin_linea = inicio_linea;
        while (fin_linea < size_datos && datos[fin_linea] != '\0') {
            fin_linea++;
        }
        
        // Copiar esta línea al buffer procesado
        uint32_t longitud_linea = fin_linea - inicio_linea;
        if (longitud_linea > 0) {
            // Si no es la primera línea, agregar salto de línea
            if (pos_procesado > 0) {
                buffer_procesado[pos_procesado++] = '\n';
            }
            
            memcpy(buffer_procesado + pos_procesado, datos + inicio_linea, longitud_linea);
            pos_procesado += longitud_linea;
        }
        
        pos_original = fin_linea + 1; // Saltar el \0
    }
    
    // Terminar el string
    buffer_procesado[pos_procesado] = '\0';
    
    // Mostrar el resultado procesado
    if (pos_procesado > 0) {
        printf("%s\n", buffer_procesado);
    } else {
        printf("(Sin datos visibles)\n");
    }
    
    free(buffer_procesado);
    printf("────────────────────────────────────────────────────────────────────────────────\n");
    printf("\n");
}

void procesar_mensaje_lectura(t_worker* worker) {
    // Consumir el paquete que contiene query_id
    t_list* elementos = recibir_paquete(logger, worker->socket_worker);
    
    if (elementos == NULL || list_size(elementos) == 0) {
        log_error(logger, "Error al recibir paquete MENSAJE_LECTURA del Worker %d", worker->worker_id);
        if (elementos) list_destroy_and_destroy_elements(elementos, free);
        return;
    }
    
    // Extraer query_id del primer elemento
    uint32_t query_id_notif;
    void* elem = list_get(elementos, 0);
    memcpy(&query_id_notif, elem, sizeof(uint32_t));
    
    log_info(logger, "Worker %d notifica inicio de operación READ - Query %u", 
             worker->worker_id, query_id_notif);
    
    list_destroy_and_destroy_elements(elementos, free);
}

void procesar_resultado_read(t_worker* worker) {
    log_info(logger, "Procesando resultado de READ del Worker %d", worker->worker_id);
    
    // Recibir lista de elementos
    t_list* elementos = recibir_paquete(logger, worker->socket_worker);
    
    if (elementos == NULL || list_size(elementos) < 4) {  // Son 4 elementos
        log_error(logger, "Error al recibir elementos de RESULTADO_READ del Worker %d", worker->worker_id);
        if (elementos) list_destroy_and_destroy_elements(elementos, free);
        return;
    }
    
    // Parsear elementos: [query_id, file_tag, size, datos]
    uint32_t query_id_recibido;
    void* elem0 = list_get(elementos, 0);
    memcpy(&query_id_recibido, elem0, sizeof(uint32_t));

    logging_envio_lectura(query_id_recibido, worker->worker_id);
    
    // COPIAR FILE_TAG ANTES DE DESTRUIR LA LISTA
    char* file_tag_orig = (char*)list_get(elementos, 1);
    char* file_tag = strdup(file_tag_orig);
    
    uint32_t size_datos;
    void* elem2 = list_get(elementos, 2);
    memcpy(&size_datos, elem2, sizeof(uint32_t));
    
    char* datos_leidos_orig = (char*)list_get(elementos, 3);
    
    // Validar datos recibidos
    if (size_datos == 0 || datos_leidos_orig == NULL) {
        log_error(logger, "Datos READ inválidos del Worker %d", worker->worker_id);
        list_destroy_and_destroy_elements(elementos, free);
        return;
    }
    
    // COPIAR DATOS ANTES DE DESTRUIR LA LISTA
    char* datos_copia = malloc(size_datos + 1);
    if (datos_copia == NULL) {
        log_error(logger, "Error al allocar memoria para datos READ");
        free(file_tag);
        list_destroy_and_destroy_elements(elementos, free);
        return;
    }
    
    memcpy(datos_copia, datos_leidos_orig, size_datos);
    datos_copia[size_datos] = '\0';
    
    // AHORA SÍ PODEMOS DESTRUIR LA LISTA ORIGINAL
    list_destroy_and_destroy_elements(elementos, free);
    

    // MOSTRAR RESULTADO CON FILE_TAG
    mostrar_resultado_read_consola(query_id_recibido, worker->worker_id, file_tag, size_datos, datos_copia);
    
    log_info(logger, "✓ READ completado - Query %u | Worker %d | File: %s | Tamaño: %u bytes", 
             query_id_recibido, worker->worker_id, file_tag, size_datos);
    
    // LIBERAR COPIAS
    free(datos_copia);
    free(file_tag);
}

void procesar_end_worker(t_worker* worker) {
    log_info(logger, "═══════════════════════════════════════════════");
    log_info(logger, "Worker %d envió END para query %d", worker->worker_id, worker->query_actual);
    log_info(logger, "═══════════════════════════════════════════════");
    
    int query_id_finalizada = worker->query_actual;

    // SI NO HAY QUERY ASIGNADA, SOLO INFORMAR Y SALIR
    if (query_id_finalizada == -1) {
        log_info(logger, "Worker %d envió END sin query activa - Ignorando", worker->worker_id);
        return;  // ← SALIR SIN PROCESAR NI MOSTRAR ERROR
    }
    
    // 1. LIBERAR WORKER INMEDIATAMENTE (ANTES de procesar query)
    worker->ocupado = false;
    worker->query_actual = -1;
    log_info(logger, "Worker %d marcado como LIBRE", worker->worker_id);
    
    // 2. PROCESAR FINALIZACIÓN DE QUERY
    pthread_mutex_lock(&mutex_queries);
    t_query* query = buscar_query_por_id_unsafe(query_id_finalizada);
    
    if (query != NULL) {
        log_info(logger, "Query %d encontrada, finalizando por END", query->query_id);
        
        logging_finalizacion_query(query->query_id, worker->worker_id);

        // Finalizar la query exitosamente
        finalizar_query(query, "Query finalizada con END");
        
        // Remover de lista_queries_ready si está ahí
        list_remove_element(lista_queries_ready, query);
        
        log_info(logger, "Query %d finalizada exitosamente", query->query_id);
    } else {
        log_warning(logger, "Query %d no encontrada al procesar END (ya finalizada anteriormente)", 
                   query_id_finalizada);
    }
    
    int queries_ready = list_size(lista_queries_ready);
    pthread_mutex_unlock(&mutex_queries);
    
    // 3. CONTAR WORKERS LIBRES
    pthread_mutex_lock(&mutex_workers);
    int workers_libres = 0;
    int workers_total = list_size(lista_workers);
    
    log_info(logger, "Estado de Workers:");
    for (int i = 0; i < workers_total; i++) {
        t_worker* w = list_get(lista_workers, i);
        log_info(logger, "   - Worker %d: conectado=%s, ocupado=%s, query=%d",
                 w->worker_id,
                 w->conectado ? "SI" : "NO",
                 w->ocupado ? "SI" : "NO",
                 w->query_actual);
        
        if (!w->ocupado && w->conectado) {
            workers_libres++;
        }
    }
    pthread_mutex_unlock(&mutex_workers);
    
    log_info(logger, "Estado actual:");
    log_info(logger, "   - Queries en READY: %d", queries_ready);
    log_info(logger, "   - Workers libres: %d de %d", workers_libres, workers_total);
    
    // 4. FORZAR REPLANIFICACIÓN MÚLTIPLE SI ES NECESARIO
    if (queries_ready > 0 && workers_libres > 0) {
        log_info(logger, "═══════════════════════════════════════════════");
        log_info(logger, "INICIANDO REPLANIFICACIÓN POST-END");
        log_info(logger, "═══════════════════════════════════════════════");
        
        // Planificar tantas veces como sea necesario
        int planificaciones = (queries_ready < workers_libres) ? queries_ready : workers_libres;
        
        for (int i = 0; i < planificaciones; i++) {
            log_info(logger, "Planificación %d de %d", i + 1, planificaciones);
            planificar_siguiente_query();
            
            // Pequeño delay para evitar race conditions
            if (i < planificaciones - 1) {
                usleep(10000); // 10ms
            }
        }
        
        log_info(logger, "Completadas %d planificaciones", planificaciones);
    } else if (queries_ready > 0) {
        log_warning(logger, "Hay %d queries pendientes pero NO hay workers libres", queries_ready);
    } else {
        log_info(logger, "No hay queries pendientes - sistema en espera");
    }
    
    log_info(logger, "═══════════════════════════════════════════════");
    log_info(logger, "END procesado completamente para Worker %d", worker->worker_id);
    log_info(logger, "═══════════════════════════════════════════════");
}

void manejar_desconexion_worker_inmediata(t_worker* worker) {
    log_warning(logger, "════════════════════════════════════════════════════════");
    log_warning(logger, "DESCONEXIÓN DETECTADA - Worker %d", worker->worker_id);
    log_warning(logger, "════════════════════════════════════════════════════════");
    
    int query_actual = worker->query_actual;
    bool tenia_query = worker->ocupado && query_actual != -1;
    
    // Marcar worker como desconectado INMEDIATAMENTE
    worker->conectado = false;
    worker->ocupado = false;
    
    if (tenia_query) {
        log_warning(logger, "Worker %d estaba ejecutando Query %d", worker->worker_id, query_actual);
        
        // Reasignar la query a READY
        pthread_mutex_lock(&mutex_queries);
        t_query* query = buscar_query_por_id_unsafe(query_actual);
        
        if (query != NULL && query->estado == QUERY_EXEC) {
            log_warning(logger, " Devolviendo Query %d a READY por desconexión de Worker %d",
                       query->query_id, worker->worker_id);
            
            // Resetear query para reasignación
            query->estado = QUERY_READY;
            query->worker_asignado = -1;
            query->pc = 0; // Reiniciar desde el inicio
            query->ciclos_en_ready = 0;
            
            // Verificar que no esté duplicada en READY
            bool ya_existe = false;
            for (int i = 0; i < list_size(lista_queries_ready); i++) {
                t_query* q = list_get(lista_queries_ready, i);
                if (q->query_id == query->query_id) {
                    ya_existe = true;
                    break;
                }
            }
            
            if (!ya_existe) {
                list_add(lista_queries_ready, query);
                log_info(logger, "Query %d reingresada a READY (total: %d queries)", 
                         query->query_id, list_size(lista_queries_ready));
            }
        }
        pthread_mutex_unlock(&mutex_queries);
    }
    
    // Liberar worker_actual
    worker->query_actual = -1;

    logging_desconexion_worker(worker, query_actual);
    
    // Contar workers activos restantes
    pthread_mutex_lock(&mutex_workers);
    int workers_activos = 0;
    for (int i = 0; i < list_size(lista_workers); i++) {
        t_worker* w = list_get(lista_workers, i);
        if (w->conectado) workers_activos++;
    }
    pthread_mutex_unlock(&mutex_workers);
    
    log_warning(logger, "Workers activos restantes: %d", workers_activos);
    
    // FORZAR REPLANIFICACIÓN INMEDIATA SI HAY QUERIES PENDIENTES
    pthread_mutex_lock(&mutex_queries);
    int queries_pendientes = list_size(lista_queries_ready);
    pthread_mutex_unlock(&mutex_queries);
    
    if (queries_pendientes > 0 && workers_activos > 0) {
        log_warning(logger, "════════════════════════════════════════════════════════");
        log_warning(logger, "REPLANIFICANDO INMEDIATAMENTE");
        log_warning(logger, "   - Queries pendientes: %d", queries_pendientes);
        log_warning(logger, "   - Workers disponibles: %d", workers_activos);
        log_warning(logger, "════════════════════════════════════════════════════════");
        
        // Planificar inmediatamente
        planificar_siguiente_query();
    } else if (queries_pendientes > 0) {
        log_error(logger, " HAY %d QUERIES PENDIENTES PERO NO HAY WORKERS ACTIVOS", 
                  queries_pendientes);
    }
    
    log_warning(logger, "════════════════════════════════════════════════════════");
    log_warning(logger, "Manejo de desconexión completado para Worker %d", worker->worker_id);
    log_warning(logger, "════════════════════════════════════════════════════════");
}

void procesar_mensajes_worker(t_worker* worker) {
    bool seguir_procesando = true;
    
    log_info(logger, "Iniciando procesamiento de mensajes del Worker %d", worker->worker_id);
    
    while (seguir_procesando && worker->conectado) {
        op_code cod_op;
        ssize_t bytes = recv(worker->socket_worker, &cod_op, sizeof(op_code), MSG_WAITALL);
        
        // DETECTAR DESCONEXIÓN INMEDIATAMENTE
        if (bytes <= 0) {
            if (bytes == 0) {
                log_warning(logger, "Worker %d cerró la conexión limpiamente", worker->worker_id);
            } else {
                log_error(logger, "Worker %d: error en recv(): %s", worker->worker_id, strerror(errno));
            }
            
            // MANEJAR DESCONEXIÓN INMEDIATA
            manejar_desconexion_worker_inmediata(worker);
            break;
        }

        if (cod_op == 0 || cod_op > 500) {
            log_error(logger, "Código de operación inválido del Worker %d: %d", 
                     worker->worker_id, cod_op);
            continue;
        }
        
        log_info(logger, "Recibido cod_op: %d del Worker %d", cod_op, worker->worker_id);
        
        switch (cod_op) {
            case MENSAJE_LECTURA:
                log_info(logger, "Procesando MENSAJE_LECTURA del Worker %d", worker->worker_id);
                procesar_mensaje_lectura(worker);
                break;
                
            case RESULTADO_READ:
                log_info(logger, "Procesando RESULTADO_READ del Worker %d", worker->worker_id);
                procesar_resultado_read(worker);
                break;
                
            case QUERY_FINALIZADA:
                log_info(logger, "Procesando QUERY_FINALIZADA del Worker %d", worker->worker_id);
                procesar_finalizacion_worker(worker);
                break;
                
            case ERROR_EJECUCION:
                log_info(logger, "Procesando ERROR_EJECUCION del Worker %d", worker->worker_id);
                procesar_error_worker(worker);
                break;

            case END:
                log_info(logger, "Procesando END del Worker %d", worker->worker_id);
                procesar_end_worker(worker);
                break;
                
            default:
                log_warning(logger, "Código de operación no manejado del Worker %d: %d", 
                           worker->worker_id, cod_op);
                break;
        }
    }
    
    log_info(logger, "Finalizando procesamiento de mensajes del Worker %d", worker->worker_id);
}

void procesar_lectura_worker(t_log* logger, t_worker* worker, void* buffer) {
    log_info(logger, "Worker %d notifica inicio de operación READ - Query %d", worker->worker_id, worker->query_actual);

    // Liberar buffer
    if (buffer != NULL) {
        free(buffer);
    }
}

void procesar_finalizacion_worker(t_worker* worker) {
    log_info(logger, "Procesando finalización de query del Worker %d", worker->worker_id);
    
    pthread_mutex_lock(&mutex_queries);
    t_query* query = buscar_query_por_id(worker->query_actual);
    
    // Si el worker ya está libre, ignorar este mensaje
    if (!worker->ocupado) {
        log_info(logger, "Worker %d ya está libre, ignorando QUERY_FINALIZADA", worker->worker_id);
        return;
    }
    

    if (query != NULL && !query->cancelada) {
        // Enviar confirmación al Worker
        enviar_ok(worker->socket_worker);
        
        // Finalizar la query
        finalizar_query(query, "Query ejecutada correctamente");
        
        log_info(logger, "Query %d finalizada exitosamente", query->query_id);
    } else {
        log_warning(logger, "No se pudo finalizar query - no encontrada o cancelada");
        // Enviar error al Worker
        int error = htonl(OP_ERROR);
        send(worker->socket_worker, &error, sizeof(int), MSG_NOSIGNAL);
    }
    
    pthread_mutex_unlock(&mutex_queries);
    
    worker->ocupado = false;
    worker->query_actual = -1;
    
    // Planificar siguiente query
    planificar_siguiente_query();
}

void eliminar_worker(t_worker* worker) {
    if (worker != NULL) {
        worker->conectado = false;
        list_remove_element(lista_workers, worker);
        close(worker->socket_worker);
        free(worker);
    }
}

void procesar_error_worker(t_worker* worker) {
    log_info(logger, "Procesando error del Worker %d", worker->worker_id);
    
    // Recibir el paquete completo usando la función correcta
    t_list* elementos = recibir_paquete(logger, worker->socket_worker);
    
    if (elementos == NULL || list_size(elementos) == 0) {
        log_error(logger, "Error al recibir paquete de error del Worker %d", worker->worker_id);
        if (elementos) list_destroy_and_destroy_elements(elementos, free);
        return;
    }
    
    // Extraer mensaje de error
    char* mensaje_error = NULL;
    if (list_size(elementos) > 0) {
        void* elem = list_get(elementos, 0);
        mensaje_error = strdup((char*)elem);
    } else {
        mensaje_error = strdup("Error desconocido");
    }
    
    log_error(logger, "Error del Worker %d: %s", worker->worker_id, mensaje_error);
    
    pthread_mutex_lock(&mutex_queries);
    t_query* query = buscar_query_por_id(worker->query_actual);
    
    if (query != NULL) {
        log_error(logger, "Error en ejecución de Query %d: %s", query->query_id, mensaje_error);
        
        // Finalizar la query con estado de error
        query->estado = QUERY_EXIT;
        
        // Enviar notificación al Query Control
        if (query->socket_query_control != -1) {
            enviar_finalizacion_a_query_control(query->socket_query_control, mensaje_error);
        }
        
        // Remover de listas activas
        list_remove_element(lista_queries_ready, query);
        
        logging_finalizacion_query(query->query_id, worker->worker_id);
    } else {
        log_error(logger, "No se encontró query para error del Worker %d", worker->worker_id);
    }
    
    pthread_mutex_unlock(&mutex_queries);
    
    // Liberar worker
    worker->ocupado = false;
    worker->query_actual = -1;
    
    // Liberar memoria
    free(mensaje_error);
    list_destroy_and_destroy_elements(elementos, free);
    
    // Planificar siguiente query
    planificar_siguiente_query();
}


// COMUNICACIÓN QUERY CONTROL
void enviar_query_a_ejecutar(t_worker* worker, t_query* query) {
    log_info(logger, "📤 ENVIANDO QUERY %d AL WORKER %d", query->query_id, worker->worker_id);

    // Crear paquete con EJECUTAR_QUERY
    t_paquete* paquete = crear_paquete(EJECUTAR_QUERY, logger);
    
    if (paquete == NULL) {
        log_error(logger, "Error al crear paquete para enviar query al worker");
        return;
    }

    log_info(logger, "   - Código OP: EJECUTAR_QUERY (%d)", EJECUTAR_QUERY);

    // Agregar query_id
    agregar_a_paquete(paquete, &query->query_id, sizeof(int));
    log_info(logger, "   - Query ID agregado: %d", query->query_id);
    
    // Agregar PC
    agregar_a_paquete(paquete, &query->pc, sizeof(int));
    log_info(logger, "   - PC agregado: %d", query->pc);
    
    // Agregar path de la query (con tamaño)
    uint32_t tam_path = strlen(query->path_query) + 1; // +1 para el \0
    agregar_a_paquete(paquete, &tam_path, sizeof(uint32_t));
    log_info(logger, "   - Tamaño path agregado: %u", tam_path);
    
    agregar_a_paquete(paquete, query->path_query, tam_path);
    log_info(logger, "   - Path agregado: '%s'", query->path_query);
    
    // Mostrar estructura completa del paquete
    log_info(logger, "   - Estructura paquete: [query_id=%d, pc=%d, tam_path=%u, path='%s']",
             query->query_id, query->pc, tam_path, query->path_query);
    
    // Enviar paquete completo
    log_info(logger, "   - Enviando paquete de %d bytes...", paquete->buffer->size);
    enviar_paquete(paquete, worker->socket_worker);
    
    log_info(logger, "Query %d enviada exitosamente al Worker %d - Total bytes: %d", 
             query->query_id, worker->worker_id, paquete->buffer->size);

    // Liberar paquete
    eliminar_paquete(paquete);
}

void solicitar_desalojo_worker(t_worker* worker) {
    op_code cod_op = DESALOJAR_QUERY;
    send(worker->socket_worker, &cod_op, sizeof(op_code), MSG_NOSIGNAL);
}

int recibir_contexto_desalojo(t_worker* worker) {
    int pc;
    ssize_t bytes = recv(worker->socket_worker, &pc, sizeof(int), MSG_WAITALL);
    
    if (bytes <= 0) {
        log_error(logger, "Error al recibir contexto de desalojo del Worker %d", worker->worker_id);
        return 0;
    }
    
    return pc;
}

void enviar_finalizacion_a_query_control(int socket_qc, const char* motivo) {
    op_code cod_op = QUERY_FINALIZADA;
    send(socket_qc, &cod_op, sizeof(op_code), MSG_NOSIGNAL);
    
    uint32_t tam_motivo = strlen(motivo);
    send(socket_qc, &tam_motivo, sizeof(uint32_t), MSG_NOSIGNAL);
    send(socket_qc, motivo, tam_motivo, MSG_NOSIGNAL);
}


// LOGGING
void logging_conexion_query_control(t_query* query) {
    pthread_mutex_lock(&mutex_workers);
    int cantidad_workers = list_size(lista_workers);
    pthread_mutex_unlock(&mutex_workers);
    
    log_info(logger, "## Se conecta un Query Control para ejecutar la Query %s con prioridad %d - Id asignado: %d. Nivel multiprocesamiento %d",
             query->path_query, query->prioridad, query->query_id, cantidad_workers);
}

void logging_conexion_worker(t_worker* worker) {
    pthread_mutex_lock(&mutex_workers);
    int cantidad_workers = list_size(lista_workers);
    pthread_mutex_unlock(&mutex_workers);
    
    log_info(logger, "## Se conecta el Worker %d - Cantidad total de Workers: %d",
             worker->worker_id, cantidad_workers);
}

void logging_desconexion_query_control(t_query* query) {
    pthread_mutex_lock(&mutex_workers);
    int cantidad_workers = list_size(lista_workers);
    pthread_mutex_unlock(&mutex_workers);
    
    log_info(logger, "## Se desconecta un Query Control. Se finaliza la Query %d con prioridad %d. Nivel multiprocesamiento %d",
             query->query_id, query->prioridad, cantidad_workers);
}

void logging_desconexion_worker(t_worker* worker, int query_id) {
    pthread_mutex_lock(&mutex_workers);
    int cantidad_workers = list_size(lista_workers);
    pthread_mutex_unlock(&mutex_workers);
    
    if (query_id != -1) {
        log_info(logger, "## Se desconecta el Worker %d - Se finaliza la Query %d - Cantidad total de Workers: %d",
                 worker->worker_id, query_id, cantidad_workers);
    } else {
        log_info(logger, "## Se desconecta el Worker %d - Cantidad total de Workers: %d",
                 worker->worker_id, cantidad_workers);
    }
}

void logging_envio_query(t_query* query, int worker_id) {
    printf("\n");
    printf("════════════════════════════════════════════════\n");
    printf("            QUERY ENVIADA A WORKER              \n");
    printf("════════════════════════════════════════════════\n");
    printf(" Query ID:      %-31d \n", query->query_id);
    printf(" Worker ID:     %-31d \n", worker_id);
    printf(" Prioridad:     %-31d \n", query->prioridad);
    printf(" Estado:        %-31s \n", "READY → EXEC");
    printf("════════════════════════════════════════════════\n");
    printf("\n");
    
    log_info(logger, "## Se envía la Query %d (%d) al Worker %d",
             query->query_id, query->prioridad, worker_id);
}

void logging_desalojo_query(t_query* query, int worker_id, const char* motivo) {
    printf("\n");
    printf("════════════════════════════════════════════════\n");
    printf("              QUERY DESALOJADA                  \n");
    printf("════════════════════════════════════════════════\n");
    printf(" Query ID:      %-31d \n", query->query_id);
    printf(" Worker ID:     %-31d \n", worker_id);
    printf(" Prioridad:     %-31d \n", query->prioridad);
    printf(" PC guardado:   %-31d \n", query->pc);
    printf(" Motivo:        %-31s \n", motivo);
    printf(" Estado:        %-31s \n", "EXEC → READY");
    printf("════════════════════════════════════════════════\n");
    printf("\n");
    
    log_info(logger, "## Se desaloja la Query %d (%d) del Worker %d - Motivo: %s",
             query->query_id, query->prioridad, worker_id, motivo);
}

void logging_finalizacion_query(int query_id, int worker_id) {
    log_info(logger, "## Se terminó la Query %d en el Worker %d",
             query_id, worker_id);
}

void logging_cambio_prioridad(int query_id, int prioridad_anterior, int prioridad_nueva) {
    log_info(logger, "## %d Cambio de prioridad: %d - %d",
             query_id, prioridad_anterior, prioridad_nueva);
}

void logging_envio_lectura(int query_id, int worker_id) {
    log_info(logger, "## Se envía un mensaje de lectura de la Query %d en el Worker %d al Query Control",
             query_id, worker_id);
}


// MAIN FUNCTION
int main(int argc, char* argv[]) {
    saludar("master");
    
    // DEBUG: Mostrar directorio actual
    char cwd[1024];
    if (getcwd(cwd, sizeof(cwd)) != NULL) {
        printf("Directorio actual: %s\n", cwd);
        fprintf(stderr, "Directorio actual: %s\n", cwd);
    }

    // Validar argumentos
    if (argc != 2) {
        fprintf(stderr, "Uso: %s [archivo_config]\n", argv[0]);
        fprintf(stderr, "Ejemplo: ./bin/master master.config\n");
        fprintf(stderr, "Ejemplo: ./bin/master ../master.config\n");
        return EXIT_FAILURE;
    }
    
    // Cargar configuración
    config_global = cargar_configuracion_master(argv[1]);
    if (config_global == NULL) {
        fprintf(stderr, "Error al cargar la configuración\n");
        return EXIT_FAILURE;
    }
    
    // Inicializar logger
    logger = log_create("master.log", "Master", true, 
                        log_level_from_string(config_global->log_level));
    
    if (logger == NULL) {
        fprintf(stderr, "Error al crear el logger\n");
        destruir_config_master(config_global);
        return EXIT_FAILURE;
    }
    
    log_info(logger, "=== Iniciando Master ===");
    log_info(logger, "Puerto de escucha: %d", config_global->puerto_escucha);
    log_info(logger, "Algoritmo de planificación: %s", config_global->algoritmo_planificacion);
    log_info(logger, "Tiempo de aging: %d ms", config_global->tiempo_aging);
    log_info(logger, "Nivel de log: %s", config_global->log_level);
    
    // Inicializar estructuras
    inicializar_estructuras_master();
    
    // Iniciar hilo de aging si está configurado
    if (config_global->tiempo_aging > 0 && 
        strcmp(config_global->algoritmo_planificacion, "PRIORIDADES") == 0) {
        pthread_create(&hilo_aging, NULL, proceso_aging, NULL);
        log_info(logger, "Hilo de aging iniciado");
    }
    
    // Iniciar servidor
    iniciar_servidor_master();
    
    // Esperar finalización del sistema
    log_info(logger, "Sistema Master finalizando...");
    
    // Limpiar recursos
    sistema_activo = false;
    if (config_global->tiempo_aging > 0 && 
        strcmp(config_global->algoritmo_planificacion, "PRIORIDADES") == 0) {
        pthread_join(hilo_aging, NULL);
    }
    
    destruir_estructuras_master();
    destruir_config_master(config_global);
    log_destroy(logger);
    
    return EXIT_SUCCESS;
}
