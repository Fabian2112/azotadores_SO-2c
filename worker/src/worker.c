
#include "queryHelper.h"
#include "memoryHelper.h"

// VARIABLES GLOBALES
t_log* logger = NULL;
t_config* config = NULL;
int socket_storage = -1;
int socket_master  = -1;
uint32_t WORKER_BLOCK_SIZE = 0;
char* WORKER_ID = NULL;
static bool IS_MOCK = false;
extern uint32_t current_pc;

// INICIALIZACION
void iniciar_worker(char* config_path, char* log_path, char* worker_id) {
    // Primero inicializar el logger
    iniciar_logger_worker();
    
    // Luego cargar configuración
    config = iniciar_config(config_path);
    if (config == NULL) {
        log_error(logger, "Error fatal: No se pudo cargar la configuración");
        exit(1);
    }
    
    // Verificar si existe MODE en la configuración
    if (config_has_property(config, "MODE")) {
        const char* mode = config_get_string_value(config, "MODE");
        IS_MOCK = (mode && strcmp(mode, "MOCK") == 0);
        if (IS_MOCK) {
            log_info(logger, "Modo MOCK activado - Conexiones reales deshabilitadas");
        }
    } else {
        IS_MOCK = false;
        log_info(logger, "Modo REAL activado - Conectando a módulos reales");
    }

    WORKER_ID = strdup(worker_id);
    log_info(logger, "Worker iniciado. ID: %s", WORKER_ID);
    
    // Mostrar propiedades cargadas
    log_info(logger, "=== DEBUG: Propiedades Worker cargadas ===");
    
    char* propiedades[] = {
        "IP_MASTER", "PUERTO_MASTER", "IP_STORAGE", "PUERTO_STORAGE",
        "TAM_MEMORIA", "RETARDO_MEMORIA", "ALGORITMO_REEMPLAZO", 
        "PATH_QUERIES", "LOG_LEVEL", "MODE"
    };
    
    for (int i = 0; i < 10; i++) {
        if (config_has_property(config, propiedades[i])) {
            if (strcmp(propiedades[i], "TAM_MEMORIA") == 0 || 
                strcmp(propiedades[i], "RETARDO_MEMORIA") == 0 ||
                strcmp(propiedades[i], "PUERTO_MASTER") == 0 ||
                strcmp(propiedades[i], "PUERTO_STORAGE") == 0) {
                int valor = config_get_int_value(config, propiedades[i]);
                log_info(logger, "  %s = %d", propiedades[i], valor);
            } else {
                char* valor = config_get_string_value(config, propiedades[i]);
                log_info(logger, "  %s = %s", propiedades[i], valor ? valor : "NULL");
            }
        } else {
            log_warning(logger, "  %s = NO ENCONTRADO", propiedades[i]);
        }
    }
    log_info(logger, "=== FIN DEBUG ===");
}

void iniciar_logger_worker(void)
{
    // Usar path relativo en lugar de absoluto
    logger = log_create("worker.log", "Worker", true, LOG_LEVEL_INFO);
    if (logger == NULL) {
        printf("No se pudo crear el logger\n");
        exit(1);
    }
    log_info(logger, "Logger del Worker inicializado");
}

t_config* iniciar_config(char* config_path)
{	
    printf("Archivo de configuración solicitado: %s\n", config_path);
    
    // Obtener directorio actual
    char cwd[1024];
    if (getcwd(cwd, sizeof(cwd)) != NULL) {
        printf("Directorio actual: %s\n", cwd);
    }

    // Intentar abrir el archivo directamente primero
    FILE* archivo_encontrado = fopen(config_path, "r");
    if (archivo_encontrado != NULL) {
        printf("✓ Archivo encontrado en: %s\n", config_path);
        fclose(archivo_encontrado);
        
        t_config* nuevo_config = config_create(config_path);
        if(nuevo_config != NULL){
            printf("Configuración cargada exitosamente desde: %s\n", config_path);
            return nuevo_config;
        }
    }

    // Si no se encuentra, buscar en ubicaciones comunes
    printf("Buscando en ubicaciones alternativas...\n");
    
    char* ubicaciones[] = {
        config_path,
        "./worker1.config",
        "../worker1.config", 
        "../../worker1.config",
        "../../../worker1.config",
        "../../worker/worker1.config",
        "../../../worker/worker1.config",
        "../config/worker1.config",
        "../../config/worker1.config", 
        NULL
    };
    
    for (int i = 0; ubicaciones[i] != NULL; i++) {
        printf("  Probando: %s\n", ubicaciones[i]);
        archivo_encontrado = fopen(ubicaciones[i], "r");
        if (archivo_encontrado != NULL) {
            printf("✓ Archivo encontrado en: %s\n", ubicaciones[i]);
            fclose(archivo_encontrado);
            
            t_config* nuevo_config = config_create(ubicaciones[i]);
            if(nuevo_config != NULL){
                printf("Configuración cargada exitosamente\n");
                return nuevo_config;
            }
        }
    }
    
    fprintf(stderr, "Error: No se pudo encontrar '%s' en ninguna ubicación\n", config_path);
    return NULL;
}

void iniciar_memoria(){
    uint32_t tam_memoria = config_get_int_value(config, "TAM_MEMORIA");
    uint32_t retardo = config_get_int_value(config, "RETARDO_MEMORIA");
    const char* algoritmo = config_get_string_value(config, "ALGORITMO_REEMPLAZO");
    uint32_t tam_pagina = WORKER_BLOCK_SIZE;

    log_info(logger, "Inicializando memoria interna: %u bytes, página=%u, retardo=%u ms, algoritmo=%s", tam_memoria, tam_pagina, retardo, algoritmo);

    memory_init(tam_memoria, tam_pagina, retardo, algoritmo);
}

// Conectar y pedir block size al Storage
int conectar_storage(void) {
    //codigo para testear worker sin otros modulos
    if (IS_MOCK) { 
        log_info(logger, "[MOCK] Saltando conexión a Storage");
        return 0;
    }
    //

    char* ip_storage = config_get_string_value(config, "IP_STORAGE");
    char* puerto_storage_str = config_get_string_value(config, "PUERTO_STORAGE");
    int puerto_storage = atoi(puerto_storage_str);

    log_info(logger, "Intentando conectar al Storage %s:%d", ip_storage, puerto_storage);
    socket_storage = crear_conexion(logger, ip_storage, puerto_storage);
    if (socket_storage == -1) {
        log_error(logger, "No se pudo conectar al Storage");
        return -1;
    }
    log_info(logger, "Conexión con Storage establecida (%d)", socket_storage);
    return 0;
}

int handshake_storage_pedir_blocksize(void) {
    if (IS_MOCK) {
        WORKER_BLOCK_SIZE = (uint32_t) config_get_int_value(config, "BLOCK_SIZE_MOCK");
        log_info(logger, "[MOCK] BLOCK_SIZE=%u", WORKER_BLOCK_SIZE);
        return 0;
    }

    log_info(logger, "Enviando GET_BLOCK_SIZE (100) al Storage con ID: %s", WORKER_ID);

    int cod_op = GET_BLOCK_SIZE;
    int cod_op_network = htonl(cod_op);
    
    ssize_t sent_bytes = send(socket_storage, &cod_op_network, sizeof(int), MSG_NOSIGNAL);
    log_info(logger, "Bytes enviados: %zd", sent_bytes);

    uint32_t tam_id = strlen(WORKER_ID) + 1;
    uint32_t tam_id_network = htonl(tam_id);
    
    sent_bytes = send(socket_storage, &tam_id_network, sizeof(uint32_t), MSG_NOSIGNAL);
    if (sent_bytes <= 0) {
        log_error(logger, "Error al enviar tamaño de ID al Storage");
        return -1;
    }
    
    sent_bytes = send(socket_storage, WORKER_ID, tam_id, MSG_NOSIGNAL);
    if (sent_bytes <= 0) {
        log_error(logger, "Error al enviar ID al Storage");
        return -1;
    }

    log_info(logger, "ID enviado al Storage: %s (%u bytes)", WORKER_ID, tam_id);

    log_info(logger, "Esperando respuesta del Storage...");
    
    int cod_respuesta;
    ssize_t bytes = recv(socket_storage, &cod_respuesta, sizeof(int), MSG_WAITALL);
    
    if (bytes <= 0) {
        log_error(logger, "Error al recibir respuesta del Storage");
        return -1;
    }
    
    cod_respuesta = ntohl(cod_respuesta);
    log_info(logger, "Código de operación recibido del Storage: %d", cod_respuesta);
    
    if (cod_respuesta != BLOCK_SIZE) {
        log_error(logger, "Handshake con Storage falló: esperaba %d (BLOCK_SIZE), recibió %d", 
                 BLOCK_SIZE, cod_respuesta);
        return -1;
    }

    uint32_t block_size_network;
    bytes = recv(socket_storage, &block_size_network, sizeof(uint32_t), MSG_WAITALL);
    
    if (bytes <= 0) {
        log_error(logger, "Error al recibir BLOCK_SIZE del Storage");
        return -1;
    }
    
    uint32_t block_size_recibido = ntohl(block_size_network);
    
    // VERIFICACIÓN CRÍTICA: Storage responde con el tamaño que REALMENTE usa
    log_info(logger, "BLOCK_SIZE recibido del Storage: %u bytes", block_size_recibido);
    
    // USAR EL TAMAÑO REAL - NO EL CONFIGURADO
    WORKER_BLOCK_SIZE = block_size_recibido;
    
    log_info(logger, "Handshake Storage OK. BLOCK_SIZE = %u bytes (valor REAL del Storage)", 
             WORKER_BLOCK_SIZE);

    return 0;
}

// Conectar al Master y enviar ID
int conectar_master(void) {
    //codigo para testear worker sin otros modulos
    if (IS_MOCK) {
        log_info(logger, "[MOCK] Saltando conexión a Master");
        return 0;
    }
    //

    char* ip_master = config_get_string_value(config, "IP_MASTER");
    char* puerto_master_str = config_get_string_value(config, "PUERTO_MASTER");
    int puerto_master = atoi(puerto_master_str);

    log_info(logger, "Intentando conectar al Master %s:%d", ip_master, puerto_master);
    socket_master = crear_conexion(logger, ip_master, puerto_master);
    if (socket_master == -1) {
        log_error(logger, "No se pudo conectar al Master");
        return -1;
    }
    log_info(logger, "Conexión con Master establecida (%d)", socket_master);
    return 0;
}

int handshake_master_enviar_id(void) {
    //codigo para testear worker sin otros modulos
    if (IS_MOCK) {
        log_info(logger, "[MOCK] Handshake Master omitido");
        return 0;
    }

    // Enviar HANDSHAKE_WORKER (102) al Master CON EL ID
    op_code cod_op = HANDSHAKE_WORKER;
    log_info(logger, "Enviando handshake al Master: código %d con ID: %s", cod_op, WORKER_ID);
    
    // 1. Enviar código de operación
    ssize_t bytes_sent = send(socket_master, &cod_op, sizeof(op_code), MSG_NOSIGNAL);
    
    if (bytes_sent <= 0) {
        log_error(logger, "Error al enviar handshake al Master: %s", strerror(errno));
        return -1;
    }
    
    // 2. Enviar el ID del Worker como string
    uint32_t tam_id = strlen(WORKER_ID) + 1; // +1 para el NULL
    uint32_t tam_id_network = htonl(tam_id);
    
    bytes_sent = send(socket_master, &tam_id_network, sizeof(uint32_t), MSG_NOSIGNAL);
    if (bytes_sent <= 0) {
        log_error(logger, "Error al enviar tamaño de ID al Master");
        return -1;
    }
    
    bytes_sent = send(socket_master, WORKER_ID, tam_id, MSG_NOSIGNAL);
    if (bytes_sent <= 0) {
        log_error(logger, "Error al enviar ID al Master");
        return -1;
    }
    
    log_info(logger, "Handshake enviado al Master con ID: %s (%ld bytes)", WORKER_ID, bytes_sent);

    // Esperar confirmación del Master
    op_code respuesta;
    log_info(logger, "Esperando confirmación del Master...");
    
    ssize_t bytes_recv = recv(socket_master, &respuesta, sizeof(op_code), MSG_WAITALL);
    
    if (bytes_recv <= 0) {
        log_error(logger, "Error al recibir confirmación del Master: %s", strerror(errno));
        return -1;
    }
    
    log_info(logger, "Respuesta recibida del Master: código %d", respuesta);
    
    if (respuesta == CONFIRMATION) {
        log_info(logger, "✓ Confirmación recibida correctamente del Master");
        return 0;
    } else {
        log_error(logger, "✗ Respuesta inesperada del Master: esperaba %d (CONFIRMATION), recibió %d", 
                 CONFIRMATION, respuesta);
        return -1;
    }
}

// Placeholder: bucle para recibir mensajes del Master
void bucle_escuchar_master(void) {
    log_info(logger, "Entrando al loop de escucha del Master...");
    
    while (1) {
        log_info(logger, "Esperando operación del Master...");
        op_code cod = recibir_operacion(logger, socket_master);
        if (cod == -1) {
            log_warning(logger, "Se perdió la conexión con Master");
            break;
        }

        log_info(logger, "Operación recibida del Master: %d", cod);

        switch (cod) {
            case EJECUTAR_QUERY: {  // Cambiar de ASSIGN_QUERY a EJECUTAR_QUERY
                log_info(logger, "Recibida orden EJECUTAR_QUERY");

                // Recibir el paquete con los campos serializados (query_id, pc, tam_path, path)
                t_list* elementos = recibir_paquete(logger, socket_master);
                if (elementos == NULL) {
                    log_error(logger, "Error al recibir paquete EJECUTAR_QUERY");
                    break;
                }

                // Esperamos 4 elementos: int id, int pc, int tam_path, char[path]
                if (list_size(elementos) < 3) {
                    log_error(logger, "Paquete EJECUTAR_QUERY incompleto");
                    list_destroy_and_destroy_elements(elementos, free);
                    break;
                }

                t_query query;
                // Elemento 0: query id
                void* el0 = list_get(elementos, 0);
                memcpy(&query.id, el0, sizeof(int));

                // Elemento 1: pc
                void* el1 = list_get(elementos, 1);
                memcpy(&query.pc, el1, sizeof(int));

                // Elemento 2: puede ser tam_path o directamente el path (depende de cómo se serializó)
                // En nuestro Master serializamos tam_path (uint32_t) y luego el path como elemento separado.
                char* path_str = NULL;
                if (list_size(elementos) >= 4) {
                    // elemento 2 = tam_path, elemento 3 = path
                    void* el2 = list_get(elementos, 2);
                    uint32_t tam_path = 0;
                    memcpy(&tam_path, el2, sizeof(uint32_t));
                    void* el3 = list_get(elementos, 3);
                    path_str = strdup((char*)el3);
                } else {
                    // Si no hay elemento de tam_path, tomar directamente el tercer elemento como string
                    void* el2 = list_get(elementos, 2);
                    path_str = strdup((char*)el2);
                }

                query.path = path_str;

                log_info(logger, "Query ID recibido: %u", query.id);
                log_info(logger, "PC recibido: %u", query.pc);
                log_info(logger, "Path recibido: %s", query.path);

                log_info(logger, "## Query %u: Se recibe la Query. El path de operaciones es: %s", 
                         query.id, query.path);

                // Ejecutar la query
                ejecutar_query(&query);

                free(query.path);

                // Liberar elementos
                list_destroy_and_destroy_elements(elementos, free);
                break;
            }

            case DESALOJAR_QUERY: {
                log_info(logger, "Recibida orden DESALOJAR_QUERY");
                // TODO: Implementar desalojo
                // Enviar PC actual al Master
                int pc_actual = 0; // Obtener PC real de la query en ejecución
                send(socket_master, &pc_actual, sizeof(int), MSG_NOSIGNAL);
                break;
            }

            default:
                log_warning(logger, "Cod_op no reconocido desde Master: %d", cod);
                break;
        }
    }
}


// Función para notificar al Master que se está realizando una lectura
void enviar_notificacion_lectura_master(uint32_t query_id) {
    log_info(logger, "Notificando lectura al Master - Query %u", query_id);
    
    t_paquete* paquete = crear_paquete(MENSAJE_LECTURA, logger);
    
    if (paquete == NULL) {
        log_error(logger, "Error al crear paquete de notificación de lectura");
        return;
    }
    
    // Agregar query_id a la notificación
    agregar_a_paquete(paquete, &query_id, sizeof(uint32_t));
    
    enviar_paquete(paquete, socket_master);
    eliminar_paquete(paquete);
    
    log_info(logger, "Notificación de lectura enviada al Master - Query %u", query_id);
}

void enviar_finalizacion_exitosa_master(uint32_t query_id) {
    log_info(logger, "Enviando finalización exitosa al Master - Query %u", query_id);
    
    t_paquete* paquete = crear_paquete(QUERY_FINALIZADA, logger);
    
    if (paquete == NULL) {
        log_error(logger, "Error al crear paquete de finalización");
        return;
    }
    
    // Agregar mensaje de éxito
    char* mensaje = "Query ejecutada correctamente";
    agregar_a_paquete(paquete, mensaje, strlen(mensaje) + 1);
    
    enviar_paquete(paquete, socket_master);
    eliminar_paquete(paquete);
    
    log_info(logger, "Finalización exitosa enviada al Master - Query %u", query_id);
}

// Función para enviar errores al Master
void enviar_error_a_master(uint32_t query_id, const char* mensaje_error) {
    log_error(logger, "Enviando error al Master - Query %u: %s", query_id, mensaje_error);
    
    t_paquete* paquete = crear_paquete(ERROR_EJECUCION, logger);
    
    if (paquete == NULL) {
        log_error(logger, "Error al crear paquete de error");
        return;
    }
    
    // Agregar mensaje de error
    agregar_a_paquete(paquete, (void*)mensaje_error, strlen(mensaje_error) + 1);
    
    enviar_paquete(paquete, socket_master);
    eliminar_paquete(paquete);
    
    log_info(logger, "Error enviado al Master - Query %u", query_id);
}

// Ejecutar Query
void ejecutar_query(t_query* q) {
    char fullpath[PATH_MAX];
    
    // DEBUG: Mostrar información
    log_info(logger, "DEBUG ejecutar_query:");
    log_info(logger, "  - Path recibido del master: '%s'", q->path);
    
    // Siempre usar el path recibido del master tal cual
    // El master ya sabe dónde están los archivos de prueba
    snprintf(fullpath, sizeof(fullpath), "%s", q->path);
    
    log_info(logger, "  - Path intentado: '%s'", fullpath);
    
    FILE* file = fopen(fullpath, "r");
    if (!file) {
        log_error(logger, "No se pudo abrir la query: %s", fullpath);
        
        // Si el path es relativo (comienza con ..), intentar desde directorio actual
        if (strstr(q->path, "../") == q->path) {
            // Ya es relativo, intentar como está
            file = fopen(q->path, "r");
        }
        
        if (!file) {
            enviar_error_a_master(q->id, "No se pudo abrir archivo de query");
            return;
        }
    }

    char* line = NULL;
    size_t len = 0;
    uint32_t pc = q->pc;
    bool query_exitosa = true;

    // Notificar inicio de query al Master
    enviar_notificacion_lectura_master(q->id);

    log_info(logger, "## Query %u: Iniciando ejecución desde PC=%u", q->id, pc);

    while (getline(&line, &len, file) != -1) {
        line[strcspn(line, "\n")] = '\0';
        
        // Ignorar líneas vacías y comentarios
        if (strlen(line) == 0 || line[0] == '#') continue;
        
        log_info(logger, "## Query %u: PC=%u - Instrucción: %s", q->id, pc, line);
        
        if (pc >= q->pc) {
            current_pc = pc;
            
            // Solo detener si ejecutar_instruccion retorna false
            if (!ejecutar_instruccion(q->id, line, pc)) {
                log_error(logger, "## Query %u: Error crítico en instrucción, abortando ejecución", q->id);
                query_exitosa = false;
                break;
            }
            
            log_info(logger, "## Query %u: - Instrucción procesada: %s", q->id, line);
        }
        
        pc++;
    }

    free(line);
    fclose(file);

    if (query_exitosa) {
        log_info(logger, "## Query %u: Finalizada exitosamente.", q->id);
        // Enviar END normal al master
        ejecutar_END(q->id);
    } else {
        log_error(logger, "## Query %u: Finalizada con errores críticos.", q->id);
        // Ya se envió el error durante la ejecución
    }
    
    current_pc = 0;
}

// Cleanup
void finalizar_worker(void) {
    if (socket_master != -1) liberar_conexion(socket_master);
    if (socket_storage != -1) liberar_conexion(socket_storage);

    memory_destroy();

    if (config) config_destroy(config);
    if (logger) log_destroy(logger);
    if (WORKER_ID) free(WORKER_ID);    
}

bool recibir_respuesta_storage_simple(t_log* logger, int socket) {
    int respuesta_network;
    ssize_t bytes_recv = recv(socket, &respuesta_network, sizeof(int), MSG_WAITALL);
    
    if (bytes_recv <= 0) {
        log_error(logger, "Error al recibir respuesta del Storage");
        return false;
    }

    int respuesta = ntohl(respuesta_network);
    
    log_info(logger, "Respuesta recibida del Storage: código %d (network: %d)", respuesta, respuesta_network);
    
    // Aceptar 209 (OP_OK) y 210 (OP_ERROR) según conexion.h
    if (respuesta == OP_OK || respuesta == 209) {  // 209 es el OP_OK del Storage
        log_info(logger, "Operación exitosa en Storage (código: %d)", respuesta);
        return true;
    } else if (respuesta == OP_ERROR || respuesta == 210) {
        log_error(logger, "Operación falló en Storage (código: %d)", respuesta);
        return false;
    } else {
        log_error(logger, "Código de respuesta desconocido: %d (esperaba %d, %d, %d o %d)", 
                 respuesta, 209, OP_OK, 210, OP_ERROR);
        return false;
    }
}

// Función para recibir página del Storage (para READ/WRITE)
t_buffer* recibir_pagina_storage(t_log* logger, int socket) {
    // Primero recibir el código de operación
    int cod_op_network;
    ssize_t bytes_recv = recv(socket, &cod_op_network, sizeof(int), MSG_WAITALL);
    
    if (bytes_recv <= 0) {
        log_error(logger, "Error al recibir código OP del Storage");
        return NULL;
    }
    
    int cod_op = ntohl(cod_op_network);
    log_info(logger, "Storage respondió con código: %d", cod_op);
    
    if (cod_op == OP_ERROR) {
        log_warning(logger, "Storage respondió con error - operación no realizada");
        return NULL;
    }
    
    if (cod_op != OP_OK) {
        log_error(logger, "Código OP inesperado del Storage: %d (esperaba %d)", 
                 cod_op, OP_OK);
        return NULL;
    }
    
    // Recibir tamaño del buffer
    uint32_t size_network;
    bytes_recv = recv(socket, &size_network, sizeof(uint32_t), MSG_WAITALL);
    
    if (bytes_recv <= 0) {
        log_error(logger, "Error al recibir tamaño del buffer");
        return NULL;
    }
    
    uint32_t size = ntohl(size_network);
    log_info(logger, "Tamaño de página recibida: %u bytes", size);
    
    // VERIFICACIÓN CRÍTICA: Si el tamaño es incorrecto, usar el tamaño esperado
    uint32_t expected_size = WORKER_BLOCK_SIZE;
    if (size != expected_size) {
        log_error(logger, "Tamaño incorrecto: recibido=%u, esperado=%u. Usando tamaño esperado.", 
                 size, expected_size);
        size = expected_size; // Forzar el tamaño correcto
    }
    
    if (size == 0) {
        log_warning(logger, "Storage devolvió tamaño 0 - página vacía");
        t_buffer* buffer = malloc(sizeof(t_buffer));
        buffer->size = 0;
        buffer->stream = NULL;
        return buffer;
    }
    
    if (size > 1000000) {
        log_error(logger, "Tamaño de página excesivo: %u bytes", size);
        return NULL;
    }
    
    // Recibir datos
    t_buffer* buffer = malloc(sizeof(t_buffer));
    if (!buffer) {
        log_error(logger, "Error al allocar buffer");
        return NULL;
    }
    
    buffer->size = size;
    buffer->stream = malloc(size);
    
    if (!buffer->stream) {
        log_error(logger, "Error al allocar stream del buffer");
        free(buffer);
        return NULL;
    }
    
    // RECIBIR EXACTAMENTE 'size' BYTES
    bytes_recv = recv(socket, buffer->stream, size, MSG_WAITALL);
    if (bytes_recv != (ssize_t)size) {
        log_error(logger, "Error al recibir datos: %zd bytes de %u", bytes_recv, size);
        free(buffer->stream);
        free(buffer);
        return NULL;
    }
    
    log_info(logger, "Página recibida exitosamente: %u bytes", size);
    return buffer;
}
