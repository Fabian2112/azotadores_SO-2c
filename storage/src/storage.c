#include "storage.h"
#include <cliente.h>
#include <server.h>

// VARIABLES GLOBALES

t_log* logger = NULL;
static storage_t* global_storage = NULL;
t_list* lista_workers_storage = NULL;
pthread_mutex_t mutex_workers_storage;
int contador_worker_id_storage = 0;


// FUNCIONES AUXILIARES

// Función segura para concatenar rutas
static void safe_path_join(char *dest, size_t max, const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);

    int written = vsnprintf(dest, max, fmt, args);

    va_end(args);

    // Si se truncó — no es error, pero evita warnings y posibles usos incorrectos
    if (written < 0 || (size_t)written >= max) {
        dest[max - 1] = '\0';
    }
}

// Calcular total_blocks basado en FS_SIZE y BLOCK_SIZE
static size_t calculate_total_blocks(size_t fs_size, size_t block_size) {
    if (block_size == 0) {
        log_error(logger, "Error: BLOCK_SIZE no puede ser 0");
        return 0;
    }
    
    // Calcular total de bloques con redondeo hacia arriba
    size_t total_blocks = (fs_size + block_size - 1) / block_size;
    
    log_info(logger, "Calculando total de bloques: FS_SIZE=%zu, BLOCK_SIZE=%zu -> TOTAL_BLOCKS=%zu", 
             fs_size, block_size, total_blocks);
    
    return total_blocks;
}

// FUNCIONES AUXILIARES PARA DELAY
void apply_operation_delay(storage_t* storage) {
    int delay_ms = config_get_int_value(storage->storage_config, "RETARDO_OPERACION");
    if (delay_ms > 0) {
        usleep(delay_ms * 1000);
    }
}

void apply_block_access_delay(storage_t* storage, size_t block_count) {
    int delay_per_block = config_get_int_value(storage->storage_config, "RETARDO_ACCESO_BLOQUE");
    if (delay_per_block > 0) {
        usleep(delay_per_block * block_count * 1000);
    }
}

// Función para cargar hashes desde archivo al diccionario
static t_dictionary* cargar_hash_index(const char* path) {
    t_dictionary* dict = dictionary_create();
    
    FILE* file = fopen(path, "r");
    if (!file) {
        log_warning(logger, "No se pudo abrir blocks_hash_index.config, creando nuevo");
        return dict;
    }
    
    char line[512];
    while (fgets(line, sizeof(line), file)) {
        // Remover salto de línea
        line[strcspn(line, "\n")] = '\0';
        
        // Ignorar líneas vacías
        if (strlen(line) == 0) continue;
        
        // Buscar el '='
        char* separator = strchr(line, '=');
        if (separator) {
            *separator = '\0';
            char* hash = line;
            char* block_name = separator + 1;
            
            // Agregar al diccionario (duplicar strings)
            dictionary_put(dict, hash, strdup(block_name));
            log_debug(logger, "Hash cargado: %s -> %s", hash, block_name);
        }
    }
    
    fclose(file);
    return dict;
}

// Función para guardar hashes del diccionario al archivo
static int guardar_hash_index(t_dictionary* dict, const char* path) {
    FILE* file = fopen(path, "w");
    if (!file) {
        log_error(logger, "Error al abrir blocks_hash_index.config para escritura: %s", 
                 strerror(errno));
        return -1;
    }
    
    // Función auxiliar para iterar
    void escribir_entrada(char* key, void* value) {
        fprintf(file, "%s=%s\n", key, (char*)value);
        log_debug(logger, "Hash guardado: %s=%s", key, (char*)value);
    }
    
    dictionary_iterator(dict, escribir_entrada);
    
    fclose(file);
    log_info(logger, "blocks_hash_index.config guardado exitosamente");
    return 0;
}

uint32_t obtener_block_size_desde_superbloque(void) {
    // Buscar en múltiples ubicaciones
    char* ubicaciones[] = {
        "superblock.config",
        "../superblock.config", 
        "../../superblock.config",
        "../../storage/superblock.config",
        "../../../superblock.config",
        NULL
    };
    
    t_config* superblock_config = NULL;
    uint32_t block_size = 16; // Valor por defecto
    
    for (int i = 0; ubicaciones[i] != NULL; i++) {
        FILE* test = fopen(ubicaciones[i], "r");
        if (test != NULL) {
            fclose(test);
            log_info(logger, "✅ superblock.config encontrado en: %s", ubicaciones[i]);
            
            superblock_config = config_create(ubicaciones[i]);
            if (superblock_config != NULL) break;
        }
    }
    
    if (superblock_config == NULL) {
        log_error(logger, "❌ No se pudo encontrar superblock.config en ninguna ubicación");
        return block_size;
    }
    
    if (config_has_property(superblock_config, "BLOCK_SIZE")) {
        block_size = config_get_int_value(superblock_config, "BLOCK_SIZE");
        log_info(logger, "✅ BLOCK_SIZE cargado desde superbloque: %u bytes", block_size);
    } else {
        log_error(logger, "❌ No se encontró BLOCK_SIZE en superblock.config, usando valor por defecto: %u", block_size);
    }
    
    // Validar que el BLOCK_SIZE sea razonable
    if (block_size < 16 || block_size > 65536) {
        log_error(logger, "❌ BLOCK_SIZE inválido: %u, usando valor por defecto: 16", block_size);
        block_size = 16;
    }
    
    config_destroy(superblock_config);
    return block_size;
}

// FUNCIONES DE INICIALIZACION Y DESTRUCCION
storage_t* inicializar_storage(const char* config_path) {

    // Primero buscar el archivo de configuración en ubicaciones relativas
    char* ubicaciones[] = {
        (char*)config_path,           // Ruta original
        "../storage.config",          // Un nivel arriba (desde bin/)
        "../../storage/storage.config", // Desde raíz del proyecto  
        "./storage.config",           // Directorio actual
        "storage.config",             // Solo el nombre
        NULL
    };
    
    FILE* archivo_encontrado = NULL;
    char* ruta_final = NULL;
    
    for (int i = 0; ubicaciones[i] != NULL; i++) {
        archivo_encontrado = fopen(ubicaciones[i], "r");
        if (archivo_encontrado != NULL) {
            ruta_final = ubicaciones[i];
            printf("Archivo de configuración encontrado en: %s\n", ruta_final);
            fclose(archivo_encontrado);
            break;
        }
    }
    
    if (ruta_final == NULL) {
        fprintf(stderr, "Error: No se pudo encontrar storage.config en ninguna ubicación\n");
        return NULL;
    }

    logger = log_create("storage.log", "STORAGE", true, LOG_LEVEL_INFO);
    if (!logger) {
        fprintf(stderr, "Error al crear logger\n");
        return NULL;
    }

    log_info(logger, "Inicializando storage con configuración: %s", ruta_final);

    storage_t* storage = malloc(sizeof(storage_t));
    if (!storage) {
        log_error(logger, "Error al allocar storage");
        log_destroy(logger);
        return NULL;
    }

    // Inicializar estructura con valores por defecto
    memset(storage, 0, sizeof(storage_t));

    storage->storage_config = config_create(ruta_final);
    if (!storage->storage_config) {
        log_error(logger, "Error al cargar configuración desde: %s", ruta_final);
        free(storage);
        log_destroy(logger);
        return NULL;
    }

    // DEBUG: Mostrar todas las propiedades cargadas
    log_info(logger, "=== DEBUG: Propiedades cargadas ===");

    // Obtener configuración con validación
    char* propiedades[] = {
        "PUNTO_MONTAJE", "FRESH_START", "RETARDO_OPERACION", 
        "RETARDO_ACCESO_BLOQUE", "LOG_LEVEL"
    };
    
    for (int i = 0; i < 5; i++) {
        if (config_has_property(storage->storage_config, propiedades[i])) {
            char* valor = config_get_string_value(storage->storage_config, propiedades[i]);
            log_info(logger, "  %s = %s", propiedades[i], valor ? valor : "NULL");
        } else {
            log_error(logger, "  %s = NO ENCONTRADO", propiedades[i]);
        }
    }
    log_info(logger, "=== FIN DEBUG ===");

    // Obtener configuración correcta
    char* root_path = config_get_string_value(storage->storage_config, "PUNTO_MONTAJE");
    if (!root_path) {
        log_error(logger, "PUNTO_MONTAJE no encontrado en configuración");
        config_destroy(storage->storage_config);
        free(storage);
        log_destroy(logger);
        return NULL;
    }
    
    strncpy(storage->root_path, root_path, MAX_PATH_LENGTH);
    storage->root_path[MAX_PATH_LENGTH - 1] = '\0'; // Asegurar terminación
    
    // Obtener FRESH_START de forma confiable
    bool fresh_start = false;

    if (config_has_property(storage->storage_config, "FRESH_START")) {
        char* fresh_start_str = config_get_string_value(storage->storage_config, "FRESH_START");
        log_info(logger, "FRESH_START string: '%s'", fresh_start_str);
        
        // Usar solo la comparación de string, NO config_get_int_value
        if (fresh_start_str && strcasecmp(fresh_start_str, "TRUE") == 0) {
            fresh_start = true;
            log_info(logger, "FRESH_START interpretado como: TRUE");
        } else {
            fresh_start = false;
            log_info(logger, "FRESH_START interpretado como: FALSE");
        }
    } else {
        log_error(logger, "FRESH_START no encontrado en configuración");
        config_destroy(storage->storage_config);
        free(storage);
        log_destroy(logger);
        return NULL;
    }

    log_info(logger, "Configuración cargada - PUNTO_MONTAJE: %s, FRESH_START: %s", 
             storage->root_path, fresh_start ? "TRUE" : "FALSE");



    // Inicializar mutex
    if (pthread_mutex_init(&storage->mutex, NULL) != 0) {
        log_error(logger, "Error al inicializar mutex");
        config_destroy(storage->storage_config);
        free(storage);
        log_destroy(logger);
        return NULL;
    }

    int result;
    if (fresh_start) {
        log_info(logger, "Iniciando storage FRESH_START");
        result = initialize_fresh_storage(storage);
    } else {
        log_info(logger, "Cargando storage existente");
        result = load_existing_storage(storage);
    }

    if (result != 0) {
        log_error(logger, "Error al inicializar/cargar storage: %d", result);
        storage_destroy(storage);
        return NULL;
    }

    log_info(logger, "Storage inicializado exitosamente");
    return storage;
}

void iniciar_servidor_storage() {
    // Obtener puerto de escucha desde configuración
    int puerto_escucha = config_get_int_value(global_storage->storage_config, "PUERTO_ESCUCHA");
    
    int socket_servidor = iniciar_servidor(logger, puerto_escucha);
    
    if (socket_servidor == -1) {
        log_error(logger, "Error al iniciar el servidor Storage");
        return;
    }
    
    log_info(logger, "Servidor Storage iniciado en puerto %d", puerto_escucha);
    log_info(logger, "Esperando conexiones de Workers...");
    
    while (1) {
        int socket_cliente = esperar_cliente(logger, socket_servidor);
        
        if (socket_cliente == -1) {
            log_error(logger, "Error al aceptar cliente");
            continue;
        }
        
        // Recibir handshake para identificar tipo de módulo
        int cod_op_network;
        ssize_t bytes = recv(socket_cliente, &cod_op_network, sizeof(int), MSG_WAITALL);
        
        if (bytes <= 0) {
            log_warning(logger, "Cliente se desconectó antes del handshake");
            close(socket_cliente);
            continue;
        }
        
        // Convertir de network byte order
        int cod_op = ntohl(cod_op_network);
        log_info(logger, "Handshake recibido - Código OP: %d (network: %d)", cod_op, cod_op_network);

        // Solo aceptar Workers (con GET_BLOCK_SIZE o WORKER)
        if (cod_op == WORKER || cod_op == GET_BLOCK_SIZE) {
            // Obtener información del cliente
            struct sockaddr_in cliente_addr;
            socklen_t addr_len = sizeof(cliente_addr);
            char cliente_ip[INET_ADDRSTRLEN];
            
            if (getpeername(socket_cliente, (struct sockaddr*)&cliente_addr, &addr_len) == 0) {
                inet_ntop(AF_INET, &cliente_addr.sin_addr, cliente_ip, INET_ADDRSTRLEN);
            } else {
                strcpy(cliente_ip, "IP desconocida");
            }
            
            log_info(logger, "## Se conecta un Worker desde %s - Socket: %d", cliente_ip, socket_cliente);
            
            // Recibir ID del Worker inmediatamente después del handshake
            char* worker_id_recibido = NULL;
            int worker_id_numerico = contador_worker_id_storage; // Valor por defecto
            
            if (cod_op == GET_BLOCK_SIZE) {
                // Recibir ID del Worker
                uint32_t tam_id_network;
                bytes = recv(socket_cliente, &tam_id_network, sizeof(uint32_t), MSG_WAITALL);
                
                if (bytes > 0) {
                    uint32_t tam_id = ntohl(tam_id_network);
                    
                    if (tam_id > 0 && tam_id < 1024) {
                        worker_id_recibido = malloc(tam_id);
                        bytes = recv(socket_cliente, worker_id_recibido, tam_id, MSG_WAITALL);
                        
                        if (bytes > 0) {
                            // Asegurar terminación NULL
                            if (worker_id_recibido[tam_id - 1] != '\0') {
                                char* temp = realloc(worker_id_recibido, tam_id + 1);
                                if (temp) {
                                    worker_id_recibido = temp;
                                    worker_id_recibido[tam_id] = '\0';
                                }
                            }
                            
                            // Convertir a numérico
                            worker_id_numerico = atoi(worker_id_recibido);
                            if (worker_id_numerico <= 0) {
                                worker_id_numerico = contador_worker_id_storage;
                            }
                            
                            log_info(logger, "Worker identificado con ID: %s (numérico: %d)", 
                                     worker_id_recibido, worker_id_numerico);
                        }
                    }
                }
            }

            // Crear y agregar worker CON EL ID RECIBIDO
            pthread_mutex_lock(&mutex_workers_storage);

            t_worker_storage* worker = malloc(sizeof(t_worker_storage));

            // ASIGNAR DIRECTAMENTE EL ID RECIBIDO
            worker->worker_id = worker_id_numerico;
            worker->socket_worker = socket_cliente;
            worker->conectado = true;

            list_add(lista_workers_storage, worker);

            // ACTUALIZAR CONTADOR GLOBAL
            if (worker_id_numerico >= contador_worker_id_storage) {
                contador_worker_id_storage = worker_id_numerico + 1;
            } else {
                // Si el ID recibido es menor, incrementar contador normalmente
                contador_worker_id_storage++;
            }

            int cantidad_workers = list_size(lista_workers_storage);

            pthread_mutex_unlock(&mutex_workers_storage);

            logging_conexion_worker_storage(worker, cantidad_workers);

            // Si el Worker envió GET_BLOCK_SIZE, responder inmediatamente
            if (cod_op == GET_BLOCK_SIZE) {
                log_info(logger, "Respondiendo GET_BLOCK_SIZE inmediatamente al Worker %d", worker->worker_id);
                
                // Enviar BLOCK_SIZE con endianness correcto
                int codigo_respuesta = BLOCK_SIZE;
                int codigo_network = htonl(codigo_respuesta);
                size_t block_size = 16; // USAR BLOCK_SIZE DEL SUPERBLOQUE
                uint32_t block_size_network = htonl((uint32_t)block_size);
                
                send(socket_cliente, &codigo_network, sizeof(int), MSG_NOSIGNAL);
                send(socket_cliente, &block_size_network, sizeof(uint32_t), MSG_NOSIGNAL);
                
                log_info(logger, "BLOCK_SIZE enviado al Worker %d", worker->worker_id);
            }
            
            // Liberar memoria del ID
            if (worker_id_recibido != NULL) {
                free(worker_id_recibido);
            }

            // Crear hilo para atender al worker
            pthread_t hilo_worker;
            pthread_create(&hilo_worker, NULL, atender_worker_storage, worker);
            pthread_detach(hilo_worker);
        } else {
            log_warning(logger, "Tipo de módulo no reconocido (%d) - Cerrando conexión", cod_op);
            close(socket_cliente);
        }
    }
    
    close(socket_servidor);
}

void* atender_worker_storage(void* args) {
    t_worker_storage* worker = (t_worker_storage*)args;
    
    log_info(logger, "Iniciando atención al Worker %d", worker->worker_id);
    
    // CARGAR CONFIGURACIÓN DEL SUPERBLOQUE
    static uint32_t storage_block_size = 0;
    static bool config_cargada = false;
    
    if (!config_cargada) {
        char* ubicaciones_superblock[] = {
            "superblock.config",
            "../superblock.config",
            "../../superblock.config",
            "../../storage/superblock.config",
            "../../../superblock.config",
            NULL
        };
        
        t_config* superblock_config = NULL;
        char* ruta_encontrada = NULL;
        
        for (int i = 0; ubicaciones_superblock[i] != NULL; i++) {
            FILE* test_file = fopen(ubicaciones_superblock[i], "r");
            if (test_file != NULL) {
                fclose(test_file);
                ruta_encontrada = ubicaciones_superblock[i];
                log_info(logger, "superblock.config encontrado en: %s", ruta_encontrada);
                break;
            }
        }
        
        if (ruta_encontrada != NULL) {
            superblock_config = config_create(ruta_encontrada);
            if (superblock_config != NULL) {
                if (config_has_property(superblock_config, "BLOCK_SIZE")) {
                    storage_block_size = config_get_int_value(superblock_config, "BLOCK_SIZE");
                    log_info(logger, "BLOCK_SIZE cargado: %u bytes", storage_block_size);
                } else {
                    log_error(logger, "No se encontró BLOCK_SIZE");
                    storage_block_size = 16;
                }
                config_destroy(superblock_config);
            } else {
                log_error(logger, "No se pudo cargar superblock.config");
                storage_block_size = 16;
            }
        } else {
            log_error(logger, "No se encontró superblock.config");
            storage_block_size = 16;
        }
        config_cargada = true;
    }
    
    bool conexion_activa = true;
    uint32_t current_query_id = 0; 
    
    while (conexion_activa && worker->conectado) {
        int cod_op_network;
        ssize_t bytes = recv(worker->socket_worker, &cod_op_network, sizeof(int), MSG_WAITALL);
        
        if (bytes <= 0) {
            conexion_activa = false;
            break;
        }

        int cod_op = ntohl(cod_op_network);
        log_info(logger, "Worker %d envió código OP: %d", worker->worker_id, cod_op);

        switch (cod_op) {
            case OP_PROGRAM_COUNTER: {
                uint32_t pc_network;
                recv(worker->socket_worker, &pc_network, sizeof(uint32_t), MSG_WAITALL);
                current_query_id = ntohl(pc_network);  
                log_info(logger, "PC recibido del Worker %d: %u", worker->worker_id, current_query_id);
                break;
            }
            
            case GET_BLOCK_SIZE: {
                log_info(logger, "Worker %d solicitó BLOCK_SIZE", worker->worker_id);
                
                int codigo_respuesta = BLOCK_SIZE;
                int codigo_network = htonl(codigo_respuesta);
                uint32_t block_size_network = htonl(storage_block_size);
                
                log_info(logger, "Enviando BLOCK_SIZE=%u bytes", storage_block_size);
                
                send(worker->socket_worker, &codigo_network, sizeof(uint32_t), MSG_NOSIGNAL);
                send(worker->socket_worker, &block_size_network, sizeof(uint32_t), MSG_NOSIGNAL);
                
                log_info(logger, "BLOCK_SIZE enviado al Worker %d", worker->worker_id);
                break;
            }

            case OP_CREATE: {
                log_info(logger, "Worker %d solicitó OP_CREATE", worker->worker_id);
                manejar_create_file(worker->socket_worker, current_query_id);  
                break;
            }
            
            case OP_WRITE: {
                log_info(logger, "Worker %d solicitó OP_WRITE", worker->worker_id);
                manejar_write_file(worker->socket_worker, current_query_id);  
                break;
            }
             
            case OP_READ: {
                log_info(logger, "Worker %d solicitó OP_READ", worker->worker_id);
                manejar_read_page(worker->socket_worker, current_query_id); 
                break;
            }
            
            case OP_TRUNCATE: {
                log_info(logger, "Worker %d solicitó OP_TRUNCATE", worker->worker_id);
                manejar_truncate_file(worker->socket_worker, current_query_id);
                break;
            }
            
            case OP_DELETE: {
                log_info(logger, "Worker %d solicitó OP_DELETE", worker->worker_id);
                manejar_delete_file(worker->socket_worker, current_query_id); 
                break;
            }
            
            case OP_TAG: {
                log_info(logger, "Worker %d solicitó TAG", worker->worker_id);
                manejar_tag_file(worker->socket_worker, current_query_id);
                break;
            }
            
            case OP_COMMIT: {
                log_info(logger, "Worker %d solicitó COMMIT", worker->worker_id);
                manejar_commit_file(worker->socket_worker, current_query_id); 
                break;
            }

            case OP_FLUSH: {
                log_info(logger, "Worker %d solicitó FLUSH", worker->worker_id);
                manejar_flush_file(worker->socket_worker, current_query_id); 
                break;
            }

            case OP_END: {
                log_info(logger, "Worker %d solicitó OP_END", worker->worker_id);
                
                uint32_t tam_id_network;
                ssize_t bytes_recv = recv(worker->socket_worker, &tam_id_network, sizeof(uint32_t), MSG_WAITALL);
                
                if (bytes_recv > 0) {
                    uint32_t tam_id = ntohl(tam_id_network);
                    char* worker_id = malloc(tam_id);
                    if (worker_id) {
                        recv(worker->socket_worker, worker_id, tam_id, MSG_WAITALL);
                        log_info(logger, "Worker %s ha finalizado su query", worker_id);
                        free(worker_id);
                    }
                }
                
                enviar_ok(worker->socket_worker);
                log_info(logger, "Confirmación OP_END enviada al Worker %d", worker->worker_id);
                break;
            }

            default:
                log_warning(logger, "Código de operación desconocido del Worker %d: %d", 
                           worker->worker_id, cod_op);
                break;
        }
    }
    
    // Worker desconectado
    pthread_mutex_lock(&mutex_workers_storage);
    worker->conectado = false;
    list_remove_element(lista_workers_storage, worker);
    int cantidad_workers = list_size(lista_workers_storage);
    pthread_mutex_unlock(&mutex_workers_storage);
    
    logging_desconexion_worker_storage(worker->worker_id, cantidad_workers);
    
    close(worker->socket_worker);
    free(worker);
    
    return NULL;
}

// Función auxiliar para eliminar directorios recursivamente (fallback)
static int remove_directory_recursive(const char* path) {
    DIR* dir = opendir(path);
    if (!dir) {
        log_error(logger, "No se pudo abrir directorio: %s", path);
        return -1;
    }
    
    struct dirent* entry;
    int result = 0;
    
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        
        char full_path[MAX_PATH_LENGTH * 2];
        int written = snprintf(full_path, sizeof(full_path), "%s/%s", path, entry->d_name);
        
        if (written < 0 || written >= (int)sizeof(full_path)) {
            log_error(logger, "Path demasiado largo: %s/%s", path, entry->d_name);
            result = -1;
            continue;
        }
        
        struct stat statbuf;
        if (stat(full_path, &statbuf) == -1) {
            log_error(logger, "No se pudo obtener stat de: %s", full_path);
            result = -1;
            continue;
        }
        
        if (S_ISDIR(statbuf.st_mode)) {
            // Es un directorio, eliminar recursivamente
            if (remove_directory_recursive(full_path) != 0) {
                result = -1;
            }
        } else {
            // Es un archivo, eliminar directamente
            if (unlink(full_path) == -1) {
                log_error(logger, "No se pudo eliminar archivo: %s - %s", full_path, strerror(errno));
                result = -1;
            }
        }
    }
    
    closedir(dir);
    
    // Eliminar el directorio vacío
    if (rmdir(path) == -1) {
        log_error(logger, "No se pudo eliminar directorio: %s - %s", path, strerror(errno));
        result = -1;
    }
    
    return result;
}

// Usando MD5 según especificación
char* calculate_block_hash_md5(const void* data, size_t size) {
    // Usar la función de commons directamente
    char* hash = crypto_md5((unsigned char*)data, size);
    return hash; // Ya viene en formato hexadecimal (32 caracteres)
}

// Crear el initial_file con tag BASE
int create_initial_file(storage_t* storage) {
    log_info(logger, "Creando initial_file con tag BASE...");
    
    // 1. CREAR BLOQUE FÍSICO 0
    char block0_path[MAX_PATH_LENGTH * 2];
    int written = snprintf(block0_path, sizeof(block0_path), "%s/%s/block0000.dat", 
             storage->root_path, PHYSICAL_BLOCKS_DIR);
    
    if (written < 0 || written >= (int)sizeof(block0_path)) {
        log_error(logger, "Ruta demasiado larga para block0000.dat");
        return -1;
    }
    
    log_info(logger, "Creando bloque físico 0: %s", block0_path);
    
    // Crear y escribir el bloque físico 0 lleno de caracteres '0'
    int fd = open(block0_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd == -1) {
        log_error(logger, "Error al crear bloque físico 0: %s", strerror(errno));
        return -1;
    }

    // Llenar el bloque con caracteres '0'
    char* zero_block = malloc(storage->block_size);
    if (!zero_block) {
        log_error(logger, "Error al allocar memoria para bloque cero");
        close(fd);
        return -1;
    }
    
    memset(zero_block, '0', storage->block_size);
    
    ssize_t bytes_written = write(fd, zero_block, storage->block_size);
    close(fd);
    
    if (bytes_written != (ssize_t)storage->block_size) {
        log_error(logger, "Error al escribir bloque físico 0: %zd bytes de %zu", 
                  bytes_written, storage->block_size);
        free(zero_block);
        return -1;
    }
    
    log_info(logger, "Bloque físico 0 creado exitosamente (%zu bytes llenos de '0')", 
             storage->block_size);

    // 2. RESERVAR BLOQUE 0 EN BITMAP
    bitmap_set(storage->bitmap, 0, true);
    if (!bitmap_save(storage->bitmap)) {
        log_error(logger, "Error al guardar bitmap después de reservar bloque 0");
        free(zero_block);
        return -1;
    }
    
    log_info(logger, "Bloque 0 reservado en bitmap");

    // 3. CALCULAR HASH Y AGREGAR AL ÍNDICE
    // USAR zero_block que ya tiene el contenido en memoria (más eficiente)
    char* hash = crypto_md5((unsigned char*)zero_block, storage->block_size);
    free(zero_block); // Liberar después de calcular hash
    
    if (!hash) {
        log_error(logger, "Error al calcular hash del bloque 0");
        return -1;
    }
    
    log_info(logger, "Hash calculado del bloque 0: %s", hash);
    
    // Agregar al diccionario (usando strdup para duplicar "block0000")
    dictionary_put(storage->blocks_hash_index, hash, strdup("block0000"));
    log_info(logger, "Hash agregado al diccionario: %s -> block0000", hash);

    // Guardar en archivo usando la función auxiliar
    char hash_index_path[MAX_PATH_LENGTH * 2];
    written = snprintf(hash_index_path, sizeof(hash_index_path), 
                      "%s/blocks_hash_index.config", storage->root_path);
    
    if (written < 0 || written >= (int)sizeof(hash_index_path)) {
        log_error(logger, "Ruta demasiado larga para blocks_hash_index.config");
        free(hash);
        return -1;
    }
    
    // Usar la función auxiliar para guardar
    if (guardar_hash_index(storage->blocks_hash_index, hash_index_path) != 0) {
        log_error(logger, "Error al guardar blocks_hash_index.config");
        free(hash);
        return -1;
    }
    
    log_info(logger, "Hash del bloque 0 guardado en archivo: %s", hash);
    free(hash);
    
    // 4. CREAR ESTRUCTURA DE initial_file
    
    // 4.1 Crear directorio initial_file
    char initial_file_path[MAX_PATH_LENGTH * 2];
    written = snprintf(initial_file_path, sizeof(initial_file_path), "%s/%s/initial_file", 
             storage->root_path, FILES_DIR);
    
    if (written < 0 || written >= (int)sizeof(initial_file_path)) {
        log_error(logger, "Ruta demasiado larga para initial_file");
        return -1;
    }
    
    log_info(logger, "Creando directorio initial_file: %s", initial_file_path);
    if (mkdir(initial_file_path, 0755) == -1 && errno != EEXIST) {
        log_error(logger, "Error al crear directorio initial_file: %s", strerror(errno));
        return -1;
    }

    // 4.2 Crear directorio BASE (tag)
    char base_tag_path[MAX_PATH_LENGTH * 2];
    written = snprintf(base_tag_path, sizeof(base_tag_path), "%s/BASE", initial_file_path);
    
    if (written < 0 || written >= (int)sizeof(base_tag_path)) {
        log_error(logger, "Ruta demasiado larga para BASE");
        return -1;
    }
    
    log_info(logger, "Creando directorio BASE: %s", base_tag_path);
    if (mkdir(base_tag_path, 0755) == -1 && errno != EEXIST) {
        log_error(logger, "Error al crear directorio BASE: %s", strerror(errno));
        return -1;
    }

    // 4.3 Crear directorio logical_blocks
    char logical_blocks_path[MAX_PATH_LENGTH * 2];
    written = snprintf(logical_blocks_path, sizeof(logical_blocks_path), "%s/%s", 
                      base_tag_path, LOGICAL_BLOCKS_DIR);
    
    if (written < 0 || written >= (int)sizeof(logical_blocks_path)) {
        log_error(logger, "Ruta demasiado larga para logical_blocks");
        return -1;
    }
    
    log_info(logger, "Creando directorio logical_blocks: %s", logical_blocks_path);
    if (mkdir(logical_blocks_path, 0755) == -1 && errno != EEXIST) {
        log_error(logger, "Error al crear directorio logical_blocks: %s", strerror(errno));
        return -1;
    }

    // 5. CREAR METADATA.CONFIG
    char metadata_path[MAX_PATH_LENGTH * 2];
    written = snprintf(metadata_path, sizeof(metadata_path), "%s/%s", 
                      base_tag_path, METADATA_FILENAME);
    
    if (written < 0 || written >= (int)sizeof(metadata_path)) {
        log_error(logger, "Ruta demasiado larga para metadata.config");
        return -1;
    }
    
    log_info(logger, "Creando metadata.config: %s", metadata_path);
    
    // Crear archivo metadata primero
    FILE* metadata_file = fopen(metadata_path, "w");
    if (!metadata_file) {
        log_error(logger, "Error al crear archivo metadata: %s", strerror(errno));
        return -1;
    }
    fclose(metadata_file);
    
    // Crear configuración de metadata
    t_config* metadata = config_create(metadata_path);
    if (!metadata) {
        log_error(logger, "Error al cargar metadata de initial_file");
        return -1;
    }
    
    // Configurar valores según especificación
    char* size_str = string_itoa(storage->block_size);
    config_set_value(metadata, "TAMAÑO", size_str);
    free(size_str);
    
    config_set_value(metadata, "BLOCKS", "[0]");
    config_set_value(metadata, "ESTADO", "COMMITED");
    
    if (!config_save(metadata)) {
        log_error(logger, "Error al guardar metadata de initial_file");
        config_destroy(metadata);
        return -1;
    }
    
    config_destroy(metadata);
    log_info(logger, "metadata.config creado exitosamente");

    // 6. CREAR HARD LINK DEL BLOQUE LÓGICO
    char logical_block_path[MAX_PATH_LENGTH * 2];
    written = snprintf(logical_block_path, sizeof(logical_block_path), "%s/000000.dat", 
                      logical_blocks_path);
    
    if (written < 0 || written >= (int)sizeof(logical_block_path)) {
        log_error(logger, "Ruta demasiado larga para logical block");
        return -1;
    }
    
    log_info(logger, "Creando hard link: %s -> %s", logical_block_path, block0_path);
    
    // Eliminar archivo existente si existe
    unlink(logical_block_path);
    
    // Crear hard link al bloque físico 0
    if (link(block0_path, logical_block_path) == -1) {
        log_error(logger, "Error al crear hard link: %s", strerror(errno));
        return -1;
    }

    logging_hard_link_agregado(0, "initial_file", "BASE", 0, 0);

    // 7. VERIFICAR ESTRUCTURA COMPLETA
    log_info(logger, "Verificando estructura creada...");
    
    struct stat st;
    bool verification_ok = true;
    
    // Verificar que existe el bloque físico
    if (stat(block0_path, &st) == -1) {
        log_error(logger, "Bloque físico 0 no existe: %s", block0_path);
        verification_ok = false;
    }
    
    // Verificar que existe el metadata
    if (stat(metadata_path, &st) == -1) {
        log_error(logger, "metadata.config no existe: %s", metadata_path);
        verification_ok = false;
    }
    
    // Verificar que existe el hard link
    if (stat(logical_block_path, &st) == -1) {
        log_error(logger, "Bloque lógico no existe: %s", logical_block_path);
        verification_ok = false;
    }
    
    if (!verification_ok) {
        log_error(logger, "Fallo la verificación de la estructura de initial_file");
        return -1;
    }

    log_info(logger, "initial_file:BASE creado exitosamente con la estructura completa");
    log_info(logger, "  - Bloque físico: %s", block0_path);
    log_info(logger, "  - Metadata: %s", metadata_path);
    log_info(logger, "  - Bloque lógico: %s", logical_block_path);
    log_info(logger, "  - Tamaño: %zu bytes", storage->block_size);
    log_info(logger, "  - Estado: COMMITED");
    log_info(logger, "  - Bloques físicos: [0]");
    
    return 0;
}

// Inicializar storage en modo FRESH_START
int initialize_fresh_storage(storage_t* storage) {
    log_info(logger, "Inicializando storage desde cero (FRESH_START)...");
       
    // 0. CARGAR CONFIGURACIÓN DEL SUPERBLOCK EXISTENTE
    char superblock_path[MAX_PATH_LENGTH * 2];
    int written = snprintf(superblock_path, sizeof(superblock_path), 
                          "%s/superblock.config", storage->root_path);
    
    if (written < 0 || written >= (int)sizeof(superblock_path)) {
        log_error(logger, "Ruta demasiado larga para superblock.config");
        return -1;
    }
    
    // Cargar el superblock existente
    storage->superblock = config_create(superblock_path);
    if (!storage->superblock) {
        log_error(logger, "Error al cargar superblock.config existente: %s", superblock_path);
        return -1;
    }
    
    // Obtener valores del superblock existente
    storage->fs_size = config_get_int_value(storage->superblock, "FS_SIZE");
    storage->block_size = config_get_int_value(storage->superblock, "BLOCK_SIZE");
    
    if (storage->fs_size == 0 || storage->block_size == 0) {
        log_error(logger, "Error: FS_SIZE o BLOCK_SIZE inválidos");
        config_destroy(storage->superblock);
        return -1;
    }
    
    log_info(logger, "Configuración cargada: FS_SIZE=%zu, BLOCK_SIZE=%zu", 
             storage->fs_size, storage->block_size);
    
    storage->total_blocks = calculate_total_blocks(storage->fs_size, storage->block_size);
    
    if (storage->total_blocks == 0) {
        log_error(logger, "Error: No se pudieron calcular los bloques totales");
        config_destroy(storage->superblock);
        return -1;
    }
    
    log_info(logger, "Total de bloques calculado: %zu", storage->total_blocks);

    // LIMPIAR VOLUMEN EXISTENTE (FRESH_START)
    log_info(logger, "FRESH_START: Limpiando volumen existente...");

    // 1. Eliminar directorio files
    char files_path[MAX_PATH_LENGTH * 2];
    snprintf(files_path, sizeof(files_path), "%s/%s", storage->root_path, FILES_DIR);
    
    if (access(files_path, F_OK) == 0) {
        log_info(logger, "FRESH_START: Eliminando directorio files: %s", files_path);
        char command[MAX_PATH_LENGTH * 2 + 20]; 
        int cmd_written = snprintf(command, sizeof(command), "rm -rf \"%s\"", files_path);
        
        if (cmd_written < 0 || cmd_written >= (int)sizeof(command)) {
            log_error(logger, "FRESH_START: Comando demasiado largo");
            if (remove_directory_recursive(files_path) != 0) {
                log_warning(logger, "FRESH_START: No se pudo eliminar files completamente");
            }
        } else {
            int result = system(command);
            if (result != 0) {
                log_warning(logger, "FRESH_START: Error eliminando files");
            }
        }
    }
    
    // 2. Eliminar directorio physical_blocks
    char physical_blocks_path[MAX_PATH_LENGTH * 2];
    snprintf(physical_blocks_path, sizeof(physical_blocks_path), 
            "%s/%s", storage->root_path, PHYSICAL_BLOCKS_DIR);
    
    if (access(physical_blocks_path, F_OK) == 0) {
        log_info(logger, "FRESH_START: Eliminando physical_blocks: %s", physical_blocks_path);
        char command[MAX_PATH_LENGTH * 2 + 20];
        int cmd_written = snprintf(command, sizeof(command), "rm -rf \"%s\"", physical_blocks_path);
        
        if (cmd_written < 0 || cmd_written >= (int)sizeof(command)) {
            if (remove_directory_recursive(physical_blocks_path) != 0) {
                log_warning(logger, "FRESH_START: Error eliminando physical_blocks");
            }
        } else {
            system(command);
        }
    }
    
    // 3. Eliminar archivo blocks_hash_index.config
    char hash_index_path[MAX_PATH_LENGTH * 2];
    snprintf(hash_index_path, sizeof(hash_index_path), 
            "%s/blocks_hash_index.config", storage->root_path);
    
    if (access(hash_index_path, F_OK) == 0) {
        log_info(logger, "FRESH_START: Eliminando blocks_hash_index.config");
        unlink(hash_index_path);
    }
    
    // 4. Eliminar archivo bitmap.bin
    char bitmap_path[MAX_PATH_LENGTH * 2];
    snprintf(bitmap_path, sizeof(bitmap_path), "%s/bitmap.bin", storage->root_path);
    
    if (access(bitmap_path, F_OK) == 0) {
        log_info(logger, "FRESH_START: Eliminando bitmap.bin");
        unlink(bitmap_path);
    }
    
    // CREAR ESTRUCTURA NUEVA
    log_info(logger, "FRESH_START: Creando nueva estructura de storage...");

    // Verificar directorio raíz
    struct stat st = {0};
    if (stat(storage->root_path, &st) == -1) {
        if (mkdir(storage->root_path, 0755) == -1) {
            log_error(logger, "Error al crear directorio raíz: %s", strerror(errno));
            return -1;
        }
    }

    // Verificar longitud del root_path
    size_t root_len = strlen(storage->root_path);
    if (root_len >= MAX_PATH_LENGTH - 50) {
        log_error(logger, "Root path demasiado largo: %s", storage->root_path);
        return -1;
    }
    
    char path[MAX_PATH_LENGTH*2];
    
    // 1. Crear bitmap.bin - CORREGIR ESTA PARTE
    snprintf(bitmap_path, sizeof(bitmap_path), "%s/bitmap.bin", storage->root_path);
    
    log_info(logger, "Creando bitmap con %zu bloques en: %s", storage->total_blocks, bitmap_path);
    
    // ✅ VERIFICAR QUE EL DIRECTORIO EXISTE
    if (stat(storage->root_path, &st) == -1) {
        log_error(logger, "Directorio raíz no existe: %s", storage->root_path);
        return -1;
    }
    
    // ✅ CREAR BITMAP CON MÁS VERIFICACIONES
    storage->bitmap = bitmap_create(bitmap_path, storage->total_blocks);
    if (!storage->bitmap) {
        log_error(logger, "Error crítico: No se pudo crear bitmap");
        return -1;
    }
    
    // ✅ INICIALIZAR TODOS LOS BLOQUES COMO LIBRES (0 = libre, 1 = ocupado)
    for (int i = 0; i < storage->total_blocks; i++) {
        bitmap_set(storage->bitmap, i, false);
    }
    
    // ✅ GUARDAR BITMAP INICIAL
    if (!bitmap_save(storage->bitmap)) {
        log_error(logger, "Error al guardar bitmap inicial");
        bitmap_destroy(storage->bitmap);
        storage->bitmap = NULL;
        return -1;
    }
    
    log_info(logger, "Bitmap inicializado exitosamente con %zu bloques", storage->total_blocks);
    
    // 2. Crear blocks_hash_index.config (archivo vacío)
    snprintf(path, sizeof(path), "%s/blocks_hash_index.config", storage->root_path);
    
    FILE* hash_file = fopen(path, "w");
    if (!hash_file) {
        log_error(logger, "Error al crear blocks_hash_index.config: %s", strerror(errno));
        config_destroy(storage->superblock);
        bitmap_destroy(storage->bitmap);
        return -1;
    }
    fclose(hash_file);
    
    // ✅ Inicializar diccionario en memoria
    storage->blocks_hash_index = dictionary_create();
    if (!storage->blocks_hash_index) {
        log_error(logger, "Error al crear blocks_hash_index dictionary");
        config_destroy(storage->superblock);
        bitmap_destroy(storage->bitmap);
        return -1;
    }
    
    // 3. Crear directorio physical_blocks
    snprintf(path, sizeof(path), "%s/%s", storage->root_path, PHYSICAL_BLOCKS_DIR);
    log_info(logger, "Creando directorio physical_blocks: %s", path);
    if (mkdir(path, 0755) == -1 && errno != EEXIST) {
        log_error(logger, "Error al crear physical_blocks: %s", strerror(errno));
        config_destroy(storage->superblock);
        bitmap_destroy(storage->bitmap);
        dictionary_destroy(storage->blocks_hash_index);
        return -1;
    }
    
    // 4. Crear directorio files
    snprintf(path, sizeof(path), "%s/%s", storage->root_path, FILES_DIR);
    log_info(logger, "Creando directorio files: %s", path);
    if (mkdir(path, 0755) == -1 && errno != EEXIST) {
        log_error(logger, "Error al crear files: %s", strerror(errno));
        config_destroy(storage->superblock);
        bitmap_destroy(storage->bitmap);
        dictionary_destroy(storage->blocks_hash_index);
        return -1;
    }
    
    // 5. Crear initial_file con tag BASE
    log_info(logger, "FRESH_START: Creando initial_file:BASE...");
    if (create_initial_file(storage) != 0) {
        log_error(logger, "Error al crear initial_file");
        config_destroy(storage->superblock);
        bitmap_destroy(storage->bitmap);
        dictionary_destroy(storage->blocks_hash_index);
        return -1;
    }
    
    log_info(logger, "FRESH_START: Storage inicializado exitosamente");
    return 0;
}

int load_existing_storage(storage_t* storage) {
    log_info(logger, "Cargando storage existente...");
    
    // Verificar directorio raíz
    struct stat st = {0};
    if (stat(storage->root_path, &st) == -1) {
        log_error(logger, "El directorio raíz no existe: %s", storage->root_path);
        return -1;
    }

    // Verificar longitud del root_path
    size_t root_len = strlen(storage->root_path);
    if (root_len >= MAX_PATH_LENGTH - 50) {
        log_error(logger, "Root path demasiado largo: %s", storage->root_path);
        return -1;
    }
    
    // Cargar configuración del superbloque
    char superblock_path[MAX_PATH_LENGTH];
    safe_path_join(superblock_path, sizeof(superblock_path), 
                   "%s/superblock.config", storage->root_path);
    
    t_config* superblock_config = config_create(superblock_path);
    if (superblock_config) {
        storage->fs_size = config_get_int_value(superblock_config, "FS_SIZE");
        storage->block_size = config_get_int_value(superblock_config, "BLOCK_SIZE");
        storage->total_blocks = calculate_total_blocks(storage->fs_size, storage->block_size);
        config_destroy(superblock_config);
    } else {
        log_error(logger, "No se pudo cargar superblock.config desde: %s", superblock_path);
        return -1;
    }
    
    // ✅ CARGAR BITMAP EXISTENTE
    char bitmap_path[MAX_PATH_LENGTH];
    safe_path_join(bitmap_path, sizeof(bitmap_path), 
                   "%s/bitmap.bin", storage->root_path);
    
    log_info(logger, "Cargando bitmap desde: %s", bitmap_path);
    
    if (access(bitmap_path, F_OK) == 0) {
        storage->bitmap = bitmap_create(bitmap_path, storage->total_blocks);
        if (!storage->bitmap) {
            log_error(logger, "Error al cargar bitmap existente");
            return -1;
        }
        log_info(logger, "Bitmap cargado exitosamente con %zu bloques", storage->total_blocks);
    } else {
        log_error(logger, "Bitmap no encontrado: %s", bitmap_path);
        return -1;
    }

    // ✅ Cargar hash index desde archivo
    char hash_index_path[MAX_PATH_LENGTH];
    safe_path_join(hash_index_path, sizeof(hash_index_path), 
                   "%s/blocks_hash_index.config", storage->root_path);
    
    storage->blocks_hash_index = cargar_hash_index(hash_index_path);
    
    log_info(logger, "Storage cargado exitosamente");
    return 0;
}

void storage_destroy(storage_t* storage) {
    if (!storage) return;
    
    log_info(logger, "Destruyendo storage...");
    
    // Destruir mutex
    pthread_mutex_destroy(&storage->mutex);
    
    // Destruir configuraciones
    if (storage->storage_config) {
        config_destroy(storage->storage_config);
    }
    
    if (storage->superblock) {
        config_destroy(storage->superblock);
    }
    
    // ✅ Destruir diccionario (NO usar config_destroy)
    if (storage->blocks_hash_index) {
        // Liberar todos los valores (son strings duplicados)
        void liberar_valor(char* key, void* value) {
            free(value);
        }
        dictionary_iterator(storage->blocks_hash_index, liberar_valor);
        dictionary_destroy(storage->blocks_hash_index);
    }
    
    // Destruir bitmap
    if (storage->bitmap) {
        bitmap_destroy(storage->bitmap);
    }
    
    // Liberar estructura
    free(storage);
    
    if (logger) {
        log_destroy(logger);
    }
}

// Función auxiliar para recibir strings del Worker
char* recibir_string_del_worker(int socket) {
    // Primero recibir el tamaño
    uint32_t size_network;
    if (recv(socket, &size_network, sizeof(uint32_t), MSG_WAITALL) <= 0) {
        return NULL;
    }
    uint32_t size = ntohl(size_network);
    
    // Recibir el string
    char* str = malloc(size);
    if (recv(socket, str, size, MSG_WAITALL) <= 0) {
        free(str);
        return NULL;
    }
    
    return str;
}

// Función auxiliar para recibir strings de tamaño conocido
char* recibir_string_directo(int socket, uint32_t tam) {
    char* str = malloc(tam);
    if (recv(socket, str, tam, MSG_WAITALL) <= 0) {
        free(str);
        return NULL;
    }
    return str;
}


// IMPLEMENTACIÓN OPERACIONES

// CREATE
// Función auxiliar para crear estructura de directorios de un archivo
int create_file_structure(storage_t* storage, const char* filename, const char* tag) {
    char path[MAX_PATH_LENGTH * 2];
    int written;
    
    log_info(logger, "CREATE_STRUCTURE: Creando estructura para %s:%s", filename, tag);

    // Crear directorio del archivo
    written = snprintf(path, sizeof(path), "%s/%s/%s", storage->root_path, FILES_DIR, filename);
    if (written < 0 || written >= (int)sizeof(path)) {
        log_error(logger, "Path demasiado largo para archivo: %s/%s/%s", storage->root_path, FILES_DIR, filename);
        return -1;
    }
    if (mkdir(path, 0755) == -1 && errno != EEXIST) {
        log_error(logger, "Error al crear directorio del archivo %s: %s", filename, strerror(errno));
        return -1;
    }
    
    // Crear directorio del tag
    written = snprintf(path, sizeof(path), "%s/%s/%s/%s", storage->root_path, FILES_DIR, filename, tag);
    if (written < 0 || written >= (int)sizeof(path)) {
        log_error(logger, "Path demasiado largo para tag: %s/%s/%s/%s", storage->root_path, FILES_DIR, filename, tag);
        return -1;
    }
    if (mkdir(path, 0755) == -1 && errno != EEXIST) {
        log_error(logger, "Error al crear directorio del tag %s: %s", tag, strerror(errno));
        return -1;
    }
    
    // Crear directorio de bloques lógicos
    written = snprintf(path, sizeof(path), "%s/%s/%s/%s/%s", storage->root_path, FILES_DIR, filename, tag, LOGICAL_BLOCKS_DIR);
    if (written < 0 || written >= (int)sizeof(path)) {
        log_error(logger, "Path demasiado largo para bloques lógicos: %s/%s/%s/%s/%s", 
                 storage->root_path, FILES_DIR, filename, tag, LOGICAL_BLOCKS_DIR);
        return -1;
    }
    if (mkdir(path, 0755) == -1 && errno != EEXIST) {
        log_error(logger, "Error al crear directorio de bloques lógicos: %s", strerror(errno));
        return -1;
    }

    // Verificar que todos los directorios se crearon
    struct stat st;
    written = snprintf(path, sizeof(path), "%s/%s/%s/%s", storage->root_path, FILES_DIR, filename, tag);
    if (stat(path, &st) == -1) {
        log_error(logger, "El directorio del tag no se creó correctamente: %s", path);
        return -1;
    }
    
    written = snprintf(path, sizeof(path), "%s/%s/%s/%s/%s", storage->root_path, FILES_DIR, filename, tag, LOGICAL_BLOCKS_DIR);
    if (stat(path, &st) == -1) {
        log_error(logger, "El directorio de bloques lógicos no se creó correctamente: %s", path);
        return -1;
    }
    
    log_info(logger, "Estructura de archivo creada exitosamente para %s:%s", filename, tag);
    
    return 0;
}

int storage_create_file(storage_t* storage, const char* filename, const char* tag) {
    pthread_mutex_lock(&storage->mutex);
    apply_operation_delay(storage);
    
    log_info(logger, "CREATE_FILE: Creando archivo %s con tag %s", filename, tag);
    
    // ✅ VERIFICACIÓN MÁS ROBUSTA
    if (!filename || strlen(filename) == 0) {
        log_error(logger, "STORAGE_CREATE_FILE: filename inválido");
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }

    if (!tag || strlen(tag) == 0) {
        log_warning(logger, "STORAGE_CREATE_FILE: tag vacío, usando 'BASE'");
        tag = "BASE";
    }

    // Verificar si el archivo ya existe - con verificación de longitud
    char file_path[MAX_PATH_LENGTH * 2];
    int written = snprintf(file_path, sizeof(file_path), "%s/%s/%s", 
             storage->root_path, FILES_DIR, filename);
    
    if (written < 0 || written >= (int)sizeof(file_path)) {
        log_error(logger, "CREATE_FILE: Path demasiado largo para %s/%s/%s", 
                 storage->root_path, FILES_DIR, filename);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    if (access(file_path, F_OK) == 0) {
        log_error(logger, "CREATE_FILE: El archivo %s ya existe", filename);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    // Crear estructura del archivo
    log_info(logger, "Creando estructura de directorios...");
    if (create_file_structure(storage, filename, tag) != 0) {
        log_error(logger, "STORAGE_CREATE_FILE: Error al crear estructura para %s:%s", filename, tag);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    log_info(logger, "Estructura de directorios creada exitosamente");
    
    // Crear metadata en estado WORK_IN_PROGRESS sin bloques asignados
    char metadata_path[MAX_PATH_LENGTH * 2];
    written = snprintf(metadata_path, sizeof(metadata_path), "%s/%s/%s/%s/%s", 
             storage->root_path, FILES_DIR, filename, tag, METADATA_FILENAME);
    
    if (written < 0 || written >= (int)sizeof(metadata_path)) {
        log_error(logger, "CREATE_FILE: Path demasiado largo para metadata: %s/%s/%s/%s/%s", 
                 storage->root_path, FILES_DIR, filename, tag, METADATA_FILENAME);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    
    // Verificar que el directorio del tag existe antes de crear metadata
    char tag_dir_path[MAX_PATH_LENGTH * 2];
    written = snprintf(tag_dir_path, sizeof(tag_dir_path), "%s/%s/%s/%s", 
             storage->root_path, FILES_DIR, filename, tag);
    
    if (written < 0 || written >= (int)sizeof(tag_dir_path)) {
        log_error(logger, "CREATE_FILE: Path demasiado largo para tag dir");
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    // Verificar que el directorio del tag se creó correctamente
    struct stat st;
    if (stat(tag_dir_path, &st) == -1) {
        log_error(logger, "CREATE_FILE: El directorio del tag no existe: %s", tag_dir_path);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    log_info(logger, "CREATE_FILE: Creando metadata en: %s", metadata_path);
    
    // Crear el archivo de metadata manualmente primero
    FILE* metadata_file = fopen(metadata_path, "w");
    if (!metadata_file) {
        log_error(logger, "CREATE_FILE: Error al crear archivo metadata: %s", strerror(errno));
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    fclose(metadata_file);
    
    // Usar config_create con el path del archivo
    t_config* metadata = config_create(metadata_path);
    if (!metadata) {
        log_error(logger, "CREATE_FILE: Error al cargar configuración de metadata desde: %s", metadata_path);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }

    config_set_value(metadata, "TAMAÑO", "0");
    config_set_value(metadata, "BLOCKS", "[]");
    config_set_value(metadata, "ESTADO", "WORK_IN_PROGRESS");

    if (!config_save(metadata)) {
        log_error(logger, "CREATE_FILE: Error al guardar metadata para %s:%s", filename, tag);
        config_destroy(metadata);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    config_destroy(metadata);
    log_info(logger, "CREATE_FILE: Archivo %s:%s creado exitosamente", filename, tag);
    
    pthread_mutex_unlock(&storage->mutex);
    return 0;
}

void manejar_create_file(int socket_cliente, uint32_t query_id) {
    log_info(logger, "═══════════════════════════════════════════════");
    log_info(logger, "INICIANDO CREATE_FILE");
    
    // Recibir filename
    char* filename = recibir_string_del_worker(socket_cliente);
    if (!filename) {
        log_error(logger, "ERROR: No se pudo recibir filename en CREATE");
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        return;
    }

    // Recibir tag
    char* tag = recibir_string_del_worker(socket_cliente);
    if (!tag) {
        log_error(logger, "ERROR: No se pudo recibir tag en CREATE");
        free(filename);
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        return;
    }

    log_info(logger, "CREATE_FILE recibido: filename='%s', tag='%s'", filename, tag);
    
    // VERIFICAR PARÁMETROS
    if (strlen(filename) == 0) {
        log_error(logger, "ERROR: filename está vacío");
        free(filename);
        free(tag);
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        return;
    }

    if (strlen(tag) == 0) {
        log_warning(logger, "Tag vacío, usando 'BASE' por defecto");
        free(tag);
        tag = strdup("BASE");
    }

    // Ejecutar operación
    log_info(logger, "Llamando a storage_create_file...");
    int result = storage_create_file(global_storage, filename, tag);
    
    if (result == 0) {
        logging_file_creado(query_id, filename, tag);
        
        log_info(logger, "CREATE_FILE EXITOSO: %s:%s", filename, tag);
        enviar_ok(socket_cliente);
    } else {
        log_error(logger, "CREATE_FILE FALLÓ: %s:%s", filename, tag);
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
    }
    
    free(filename);
    free(tag);
}

int reservar_bloque_libre(storage_t* storage, uint32_t query_id) {
    log_info(logger, "Buscando bloque físico libre...");
    
    // VERIFICAR QUE EL BITMAP ESTÁ INICIALIZADO
    if (!storage->bitmap) {
        log_error(logger, "ERROR CRÍTICO: Bitmap no inicializado");
        return -1;
    }
    
    if (storage->total_blocks == 0) {
        log_error(logger, "ERROR: total_blocks es 0");
        return -1;
    }
    
    // BUSCAR BLOQUE LIBRE (empezar desde 1 para evitar bloque 0 del sistema)
    for (int i = 1; i < storage->total_blocks; i++) {
        // VERIFICAR ESTADO ACTUAL DEL BLOQUE
        bool ocupado = bitmap_get(storage->bitmap, i);
        
        if (!ocupado) {
            log_info(logger, "Bloque libre encontrado: %d", i);
            
            // RESERVAR BLOQUE
            bitmap_set(storage->bitmap, i, true);
            if (!bitmap_save(storage->bitmap)) {
                log_error(logger, "Error al guardar bitmap al reservar bloque %d", i);
                bitmap_set(storage->bitmap, i, false); // Revertir
                return -1;
            }
            
            // CREAR ARCHIVO DEL BLOQUE FÍSICO
            char block_path[MAX_PATH_LENGTH * 2];
            int written = snprintf(block_path, sizeof(block_path), 
                                  "%s/%s/block%04d.dat", 
                                  storage->root_path, PHYSICAL_BLOCKS_DIR, i);
            
            if (written < 0 || written >= (int)sizeof(block_path)) {
                log_error(logger, "Path demasiado largo para bloque físico %d", i);
                bitmap_set(storage->bitmap, i, false);
                bitmap_save(storage->bitmap);
                return -1;
            }
            
            // CREAR Y INICIALIZAR BLOQUE CON CEROS
            int fd = open(block_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
            if (fd == -1) {
                log_error(logger, "Error al crear archivo de bloque físico %d: %s", 
                         i, strerror(errno));
                bitmap_set(storage->bitmap, i, false);
                bitmap_save(storage->bitmap);
                return -1;
            }
            
            // INICIALIZAR CON CEROS
            char* zero_block = malloc(storage->block_size);
            if (zero_block) {
                memset(zero_block, 0, storage->block_size);
                ssize_t bytes_written = write(fd, zero_block, storage->block_size);
                if (bytes_written != (ssize_t)storage->block_size) {
                    log_error(logger, "Error al escribir bloque %d: %zd de %zu bytes", 
                             i, bytes_written, storage->block_size);
                }
                free(zero_block);
            }
            
            close(fd);

            logging_bloque_fisico_reservado(query_id, i);
            
            return i;
        }
    }
    
    log_error(logger, "No hay bloques físicos libres para reservar");
    return -1;
}

// TRUNCATE
// Función para obtener el estado de un archivo
int get_file_status(storage_t* storage, const char* filename, const char* tag) {
    char metadata_path[MAX_PATH_LENGTH * 2];
    int written = snprintf(metadata_path, sizeof(metadata_path), "%s/%s/%s/%s/%s", 
             storage->root_path, FILES_DIR, filename, tag, METADATA_FILENAME);
    
    if (written < 0 || written >= (int)sizeof(metadata_path)) {
        log_error(logger, "Path demasiado largo en get_file_status");
        return -1;
    }
    
    t_config* metadata = config_create(metadata_path);
    if (!metadata) {
        return -1;
    }
    
    char* estado = config_get_string_value(metadata, "ESTADO");
    int status = -1;
    
    if (estado && strcmp(estado, "COMMITED") == 0) {
        status = COMMITED;
    } else if (estado && strcmp(estado, "WORK_IN_PROGRESS") == 0) {
        status = WORK_IN_PROGRESS;
    }
    
    config_destroy(metadata);
    return status;
}

// Función para obtener la lista de bloques de un archivo
t_list* get_file_blocks(storage_t* storage, const char* filename, const char* tag) {
    t_list* blocks = list_create();
    char metadata_path[MAX_PATH_LENGTH * 2];
    int written = snprintf(metadata_path, sizeof(metadata_path), "%s/%s/%s/%s/%s", 
             storage->root_path, FILES_DIR, filename, tag, METADATA_FILENAME);
    
    if (written < 0 || written >= (int)sizeof(metadata_path)) {
        log_error(logger, "Path demasiado largo en get_file_blocks");
        return blocks; // Retornar lista vacía, no NULL
    }
    
    // ✅ VERIFICAR QUE EL ARCHIVO EXISTE
    if (access(metadata_path, F_OK) != 0) {
        log_error(logger, "Metadata no existe: %s", metadata_path);
        return blocks; // Retornar lista vacía, no NULL
    }
    
    t_config* metadata = config_create(metadata_path);
    if (!metadata) {
        log_error(logger, "Error al cargar metadata desde: %s", metadata_path);
        return blocks; // Retornar lista vacía, no NULL
    }
    
    char* blocks_str = config_get_string_value(metadata, "BLOCKS");
    if (blocks_str && strlen(blocks_str) > 2) {
        char* blocks_copy = string_duplicate(blocks_str);
        blocks_copy[strlen(blocks_copy)-1] = '\0'; // Remover ']'
        char** blocks_array = string_split(blocks_copy + 1, ","); // Remover '['
        
        for (int i = 0; blocks_array[i] != NULL; i++) {
            int block_num = atoi(blocks_array[i]);
            list_add(blocks, (void*)(long)block_num);
            free(blocks_array[i]);
        }
        free(blocks_array);
        free(blocks_copy);
    }
    
    config_destroy(metadata);
    return blocks;
}

// Función para liberar un bloque físico
void free_physical_block(storage_t* storage, int block_num, uint32_t query_id) {
    if (block_num >= 0 && block_num < storage->total_blocks) {
        bitmap_set(storage->bitmap, block_num, false);
        bitmap_save(storage->bitmap);
        logging_bloque_fisico_liberado(query_id, block_num);
    }
}

// Función para obtener la ruta de un bloque lógico
char* get_logical_block_path(storage_t* storage, const char* filename, const char* tag, size_t block_num) {
    char* path = malloc(MAX_PATH_LENGTH * 2);
    int written = snprintf(path, MAX_PATH_LENGTH * 2, "%s/%s/%s/%s/%s/%06lu.dat", 
             storage->root_path, FILES_DIR, filename, tag, LOGICAL_BLOCKS_DIR, block_num);
    
    if (written < 0 || written >= MAX_PATH_LENGTH * 2) {
        log_error(logger, "Path demasiado largo en get_logical_block_path");
        free(path);
        return NULL;
    }
    return path;
}

// Función para verificar si un tag específico referencia un bloque físico
bool tag_references_block(storage_t* storage, const char* filename, const char* tag, int physical_block) {
    // Verificar que el archivo/tag existe
    char metadata_path[MAX_PATH_LENGTH];
    safe_path_join(metadata_path, sizeof(metadata_path), 
                   "%s/%s/%s/%s/%s", storage->root_path, FILES_DIR, filename, tag, METADATA_FILENAME);
    
    if (access(metadata_path, F_OK) != 0) {
        return false;
    }
    
    t_list* blocks = get_file_blocks(storage, filename, tag);
    if (!blocks) return false;
    
    bool found = false;
    for (int i = 0; i < list_size(blocks); i++) {
        int block_num = (int)(long)list_get(blocks, i);
        if (block_num == physical_block) {
            found = true;
            break;
        }
    }
    
    list_destroy(blocks);
    return found;
}

// Función auxiliar para verificar si un bloque físico es compartido
bool is_block_shared(storage_t* storage, int physical_block, const char* current_filename, const char* current_tag) {
    // Para simplificar, asumimos que si el bloque no es 0, podría ser compartido
    // En una implementación real, buscaríamos en todos los metadata
    if (physical_block == 0) {
        return true; // El bloque 0 (initial_file) siempre es compartido
    }
    
    // Contador de referencias básico
    int reference_count = 0;
    
    char files_dir_path[MAX_PATH_LENGTH];
    safe_path_join(files_dir_path, sizeof(files_dir_path), 
                   "%s/%s", storage->root_path, FILES_DIR);
    
    DIR* files_dir = opendir(files_dir_path);
    if (!files_dir) return false;
    
    struct dirent* file_entry;
    while ((file_entry = readdir(files_dir)) != NULL) {
        if (strcmp(file_entry->d_name, ".") == 0 || strcmp(file_entry->d_name, "..") == 0) {
            continue;
        }
        
        // Buscar en todos los tags de este archivo
        char file_path[MAX_PATH_LENGTH];
        safe_path_join(file_path, sizeof(file_path), "%s/%s", files_dir_path, file_entry->d_name);
        
        DIR* file_dir = opendir(file_path);
        if (!file_dir) continue;
        
        struct dirent* tag_entry;
        while ((tag_entry = readdir(file_dir)) != NULL) {
            if (strcmp(tag_entry->d_name, ".") == 0 || strcmp(tag_entry->d_name, "..") == 0) {
                continue;
            }
            
            // Verificar metadata
            char metadata_path[MAX_PATH_LENGTH];
            safe_path_join(metadata_path, sizeof(metadata_path), 
                          "%s/%s/%s", file_path, tag_entry->d_name, METADATA_FILENAME);
            
            if (access(metadata_path, F_OK) == 0) {
                t_list* blocks = get_file_blocks(storage, file_entry->d_name, tag_entry->d_name);
                if (blocks) {
                    for (int i = 0; i < list_size(blocks); i++) {
                        int block_num = (int)(long)list_get(blocks, i);
                        if (block_num == physical_block) {
                            reference_count++;
                        }
                    }
                    list_destroy(blocks);
                }
            }
        }
        closedir(file_dir);
        
        // Si tiene más de 1 referencia, está compartido
        if (reference_count > 1) {
            closedir(files_dir);
            return true;
        }
    }
    closedir(files_dir);
    
    return reference_count > 1;
}

// Función para copiar contenido entre bloques físicos
int copy_block_content(storage_t* storage, int src_block, int dest_block) {
    char* src_path = get_physical_block_path(storage, src_block);
    char* dest_path = get_physical_block_path(storage, dest_block);
    
    if (!src_path || !dest_path) {
        if (src_path) free(src_path);
        if (dest_path) free(dest_path);
        return -1;
    }
    
    int src_fd = open(src_path, O_RDONLY);
    int dest_fd = open(dest_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    
    if (src_fd == -1 || dest_fd == -1) {
        if (src_fd != -1) close(src_fd);
        if (dest_fd != -1) close(dest_fd);
        free(src_path);
        free(dest_path);
        return -1;
    }
    
    char* buffer = malloc(storage->block_size);
    if (!buffer) {
        close(src_fd);
        close(dest_fd);
        free(src_path);
        free(dest_path);
        return -1;
    }
    
    ssize_t bytes_read = read(src_fd, buffer, storage->block_size);
    
    if (bytes_read > 0) {
        write(dest_fd, buffer, bytes_read);
    }
    
    free(buffer);
    close(src_fd);
    close(dest_fd);
    free(src_path);
    free(dest_path);
    
    return 0;
}

// Función para actualizar hard link de bloque lógico
void update_logical_block_link(storage_t* storage, const char* filename, const char* tag, 
                               size_t logical_index, int new_physical_block) {
    char* old_logical_path = get_logical_block_path(storage, filename, tag, logical_index);
    char* new_physical_path = get_physical_block_path(storage, new_physical_block);
    
    if (!old_logical_path || !new_physical_path) {
        if (old_logical_path) free(old_logical_path);
        if (new_physical_path) free(new_physical_path);
        return;
    }
    
    // Eliminar el hard link antiguo
    unlink(old_logical_path);
    
    // Crear nuevo hard link al bloque físico
    link(new_physical_path, old_logical_path);
    
    free(old_logical_path);
    free(new_physical_path);
}


int storage_truncate_file(storage_t* storage, const char* filename, const char* tag, 
                          size_t new_size, uint32_t query_id) {
    pthread_mutex_lock(&storage->mutex);
    apply_operation_delay(storage);
    
    log_info(logger, "TRUNCATE_FILE: Truncando %s:%s a tamaño %zu", filename, tag, new_size);
    
    // VERIFICACIONES CRÍTICAS AL INICIO
    if (!storage->bitmap) {
        log_error(logger, "TRUNCATE_FILE: Bitmap no inicializado");
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    if (storage->total_blocks == 0) {
        log_error(logger, "TRUNCATE_FILE: total_blocks es 0");
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }

    // Verificar que el archivo existe
    char metadata_path[PATH_MAX];
    int written = snprintf(metadata_path, sizeof(metadata_path), "%s/%s/%s/%s/%s", 
             storage->root_path, FILES_DIR, filename, tag, METADATA_FILENAME);
    
    if (written < 0 || written >= (int)sizeof(metadata_path)) {
        log_error(logger, "TRUNCATE_FILE: Path demasiado largo");
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    if (access(metadata_path, F_OK) != 0) {
        log_error(logger, "TRUNCATE_FILE: Archivo %s:%s no existe", filename, tag);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    // Verificar estado (no se puede truncar COMMITED)
    if (get_file_status(storage, filename, tag) == COMMITED) {
        log_error(logger, "TRUNCATE_FILE: No se puede truncar archivo COMMITED %s:%s", filename, tag);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    // Cargar metadata actual
    t_config* metadata = config_create(metadata_path);
    if (!metadata) {
        log_error(logger, "TRUNCATE_FILE: Error al cargar metadata de %s:%s", filename, tag);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    size_t current_size = config_get_int_value(metadata, "TAMAÑO");
    t_list* current_blocks = get_file_blocks(storage, filename, tag);
    
    log_info(logger, "TRUNCATE_FILE: Tamaño actual: %zu, nuevo: %zu", current_size, new_size);
    
    size_t current_block_count = list_size(current_blocks);
    size_t new_block_count = (new_size + storage->block_size - 1) / storage->block_size;
    
    if (new_size > current_size) {
        for (size_t i = current_block_count; i < new_block_count; i++) {
        // 1) Reservar bloque físico libre
        int physical_block = reservar_bloque_libre(storage, query_id);
        if (physical_block < 0) {
            log_error(logger, "TRUNCATE_FILE: No hay bloques libres para el bloque lógico %zu", i);
            goto cleanup_error;
        }

        // 2) Guardarlo en la lista de bloques lógicos
        list_add(current_blocks, (void*)(long)physical_block);

        // 3) Path del bloque físico
        char physical_block_path[PATH_MAX];
        written = snprintf(physical_block_path, sizeof(physical_block_path),
                           "%s/%s/block%04d.dat",
                           storage->root_path, PHYSICAL_BLOCKS_DIR, physical_block);
        if (written < 0 || written >= (int)sizeof(physical_block_path)) {
            log_error(logger, "TRUNCATE_FILE: Path bloque físico demasiado largo");
            goto cleanup_error;
        }

        // Crear archivo físico si no existe
        int fd = open(physical_block_path, O_CREAT | O_RDWR, 0644);
        if (fd == -1) {
            log_error(logger, "TRUNCATE_FILE: Error creando bloque físico %d: %s",
                      physical_block, strerror(errno));
            goto cleanup_error;
        }
        close(fd);

        // 4) Crear el hard link para el bloque lógico i
        char logical_block_path[PATH_MAX];
        written = snprintf(logical_block_path, sizeof(logical_block_path),
                           "%s/%s/%s/%s/%s/%06zu.dat",
                           storage->root_path, FILES_DIR, filename, tag,
                           LOGICAL_BLOCKS_DIR, i);
        if (written < 0 || written >= (int)sizeof(logical_block_path)) {
            log_error(logger, "TRUNCATE_FILE: Path bloque lógico demasiado largo");
            goto cleanup_error;
        }

        if (link(physical_block_path, logical_block_path) == -1 && errno != EEXIST) {
            log_error(logger, "TRUNCATE_FILE: Error al crear hard link: %s", strerror(errno));
            goto cleanup_error;
        }

        logging_hard_link_agregado(query_id, filename, tag, i, physical_block);

    } 
} else {
        // Reducir tamaño - liberar bloques sobrantes
        for (size_t i = new_block_count; i < current_block_count; i++) {
            int physical_block = (int)(long)list_get(current_blocks, i);
            
            // Verificar si el bloque físico es referenciado por otros archivos
            bool referenced = false;
            
            /*if (!referenced && physical_block != 0) { // No liberar bloque 0
                free_physical_block(storage, physical_block);
            }*/

            if (!referenced && physical_block >= 0) {
                free_physical_block(storage, physical_block, query_id);
            }
            
            // Eliminar bloque lógico
            char* logical_path = get_logical_block_path(storage, filename, tag, i);
            if (logical_path) {
            
                logging_hard_link_eliminado(query_id, filename, tag, i, physical_block);
            
                unlink(logical_path);
                free(logical_path);
            }
        }
        
        // Truncar la lista de bloques
        while (list_size(current_blocks) > new_block_count) {
            list_remove(current_blocks, list_size(current_blocks) - 1);
        }
    }
    
    // Actualizar metadata
    char new_blocks_str[4096] = "[";
    new_blocks_str[0] = '\0';
    strcat(new_blocks_str, "[");

    for (int i = 0; i < list_size(current_blocks); i++) {
        char num[20];
        snprintf(num, sizeof(num), "%d", (int)(long)list_get(current_blocks, i));
        strcat(new_blocks_str, num);
        if (i < list_size(current_blocks) - 1) strcat(new_blocks_str, ",");
    }
    strcat(new_blocks_str, "]");

    // Actualizar archivo metadata manualmente
    FILE* metadata_file = fopen(metadata_path, "w");
    if (!metadata_file) {
        log_error(logger, "TRUNCATE_FILE: Error al abrir metadata para escritura");
        goto cleanup_error;
    }
    
    fprintf(metadata_file, "TAMAÑO=%zu\n", new_size);
    fprintf(metadata_file, "BLOCKS=%s\n", new_blocks_str);
    
    // Mantener el estado actual
    char* current_status = config_get_string_value(metadata, "ESTADO");
    if (current_status) {
        fprintf(metadata_file, "ESTADO=%s\n", current_status);
    } else {
        fprintf(metadata_file, "ESTADO=WORK_IN_PROGRESS\n");
    }
    
    fclose(metadata_file);
    
    // Limpieza final
    config_destroy(metadata);
    list_destroy(current_blocks);
    pthread_mutex_unlock(&storage->mutex);

    log_info(logger, "TRUNCATE_FILE: %s:%s truncado exitosamente a %zu bytes",
             filename, tag, new_size);

    return 0;

    cleanup_error:
    config_destroy(metadata);
    list_destroy(current_blocks);
    pthread_mutex_unlock(&storage->mutex);
    return -1;
}

void manejar_truncate_file(int socket_cliente, uint32_t query_id) {
    log_info(logger, "Manejando TRUNCATE_FILE");
    
    // Recibir filename
    char* filename = recibir_string_del_worker(socket_cliente);
    if (!filename) {
        log_error(logger, "Error al recibir filename en TRUNCATE");
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        return;
    }

    log_info(logger, "Filename recibido: %s", filename);
    
    // Recibir tag
    char* tag = recibir_string_del_worker(socket_cliente);
    if (!tag) {
        free(filename);
        log_error(logger, "Error al recibir tag en TRUNCATE");
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        return;
    }

    log_info(logger, "Tag recibido: %s", tag);
    log_info(logger, "TRUNCATE_FILE solicitado para: %s:%s", filename, tag);
    
    // Recibir nuevo tamaño
    uint32_t size_network;
    ssize_t bytes_recv = recv(socket_cliente, &size_network, sizeof(uint32_t), MSG_WAITALL);
    if (bytes_recv <= 0) {
        free(filename);
        free(tag);
        log_error(logger, "Error recibiendo tamaño en TRUNCATE: %zd bytes", bytes_recv);
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        return;
    }

    if (bytes_recv != sizeof(uint32_t)) {
        free(filename);
        free(tag);
        log_error(logger, "Tamaño incompleto en TRUNCATE: %zd de %zu bytes", 
                 bytes_recv, sizeof(uint32_t));
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        return;
    }

    uint32_t new_size = ntohl(size_network);
    
    log_info(logger, "TRUNCATE_FILE recibido: %s:%s, nuevo tamaño: %u", filename, tag, new_size);
    
    // Llamar a la función storage_truncate_file
    int result = storage_truncate_file(global_storage, filename, tag, new_size, query_id);
    
    if (result == 0) {
        logging_file_truncado(query_id, filename, tag, new_size);

        enviar_ok(socket_cliente);
        log_info(logger, "TRUNCATE_FILE completado para: %s:%s", filename, tag);
    } else {
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        log_error(logger, "TRUNCATE_FILE falló para: %s:%s", filename, tag);
    }
    
    free(filename);
    free(tag);

    log_info(logger, "TRUNCATE_FILE - Procesamiento completamente finalizado");
}


// WRITE
// Función para asignar un bloque físico libre
int allocate_physical_block(storage_t* storage, uint32_t query_id) {
    
    return reservar_bloque_libre(storage, query_id);
}

int storage_write_file(storage_t* storage,
                       const char* filename,
                       const char* tag,
                       uint32_t offset,
                       const void* data,
                       uint32_t size, uint32_t query_id) {
    pthread_mutex_lock(&storage->mutex);
    apply_operation_delay(storage);

    log_info(logger, "STORAGE_WRITE_FILE: %s:%s offset=%u size=%u (BLOCK_SIZE=%zu)",
             filename, tag, offset, size, storage->block_size);

    char metadata_path[MAX_PATH_LENGTH];
    int written = snprintf(metadata_path, sizeof(metadata_path),
                           "%s/%s/%s/%s/%s",
                           storage->root_path, FILES_DIR, filename, tag, METADATA_FILENAME);
    if (written < 0 || written >= (int)sizeof(metadata_path)) {
        log_error(logger, "STORAGE_WRITE_FILE: Path metadata demasiado largo");
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }

    if (access(metadata_path, F_OK) != 0) {
        log_error(logger, "STORAGE_WRITE_FILE: Archivo %s:%s no existe", filename, tag);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }

    t_config* metadata = config_create(metadata_path);
    if (!metadata) {
        log_error(logger, "STORAGE_WRITE_FILE: Error al abrir metadata");
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }

    // Verificar COMMITTED
    bool is_committed = false;
    if (config_has_property(metadata, "COMMITTED")) {
        const char* committed_value = config_get_string_value(metadata, "COMMITTED");
        is_committed = (committed_value && strcmp(committed_value, "1") == 0);
    }
    if (!is_committed && config_has_property(metadata, "ESTADO")) {
        const char* estado = config_get_string_value(metadata, "ESTADO");
        is_committed = (estado && strcmp(estado, "COMMITED") == 0);
    }

    if (is_committed) {
        log_error(logger, "STORAGE_WRITE_FILE: No se puede escribir en archivo COMMITTED %s:%s",
                  filename, tag);
        config_destroy(metadata);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }

    uint32_t file_size = (uint32_t)config_get_int_value(metadata, "TAMAÑO");

    if (offset >= file_size) {
        log_error(logger,
                  "STORAGE_WRITE_FILE: offset=%u fuera de rango (TAMAÑO=%u)",
                  offset, file_size);
        config_destroy(metadata);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }

    if (offset + size > file_size) {
        log_info(logger,
                 "STORAGE_WRITE_FILE: recortando escritura: offset+size=%u > TAMAÑO=%u",
                 offset + size, file_size);
        size = file_size - offset;
    }

    t_list* blocks = get_file_blocks(storage, filename, tag);
    if (!blocks) {
        log_error(logger, "STORAGE_WRITE_FILE: Error obteniendo bloques para %s:%s",
                  filename, tag);
        config_destroy(metadata);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }

    size_t block_size = storage->block_size;

    // ✅ CALCULAR BLOQUE LÓGICO DE INICIO
    uint32_t bloque_logico = offset / block_size;
    uint32_t offset_in_block = offset % block_size;

    const uint8_t* src = (const uint8_t*)data;
    uint32_t remaining = size;
    
    bool blocks_modified = false;

    log_info(logger, "WRITE: offset=%u, size=%u -> bloque_inicial=%u, offset_en_bloque=%u",
             offset, size, bloque_logico, offset_in_block);

    while (remaining > 0) {
        if (bloque_logico >= (uint32_t)list_size(blocks)) {
            log_error(logger,
                      "STORAGE_WRITE_FILE: bloque_logico=%u fuera de los bloques del archivo",
                      bloque_logico);
            break;
        }

        int current_physical_block = (int)(long)list_get(blocks, bloque_logico);
        int physical_block_to_write = current_physical_block;

        // Verificar si es compartido (Copy-on-Write)
        bool is_shared = is_block_shared(storage, current_physical_block, filename, tag);
        
        if (is_shared) {
            log_info(logger, "STORAGE_WRITE_FILE: Bloque %d compartido, aplicando Copy-on-Write",
                     current_physical_block);
            
            int new_physical_block = reservar_bloque_libre(storage, query_id);
            if (new_physical_block == -1) {
                log_error(logger, "STORAGE_WRITE_FILE: No hay bloques libres para CoW");
                break;
            }
            
            if (copy_block_content(storage, current_physical_block, new_physical_block) != 0) {
                log_error(logger, "STORAGE_WRITE_FILE: Error copiando contenido del bloque");
                free_physical_block(storage, new_physical_block, query_id);
                break;
            }
            
            list_replace(blocks, bloque_logico, (void*)(long)new_physical_block);
            update_logical_block_link(storage, filename, tag, bloque_logico, new_physical_block);
            physical_block_to_write = new_physical_block;
            blocks_modified = true;
            
            log_info(logger, "STORAGE_WRITE_FILE: CoW completado: %d -> %d",
                     current_physical_block, new_physical_block);
        }

        // ✅ ESCRIBIR EN EL BLOQUE FÍSICO (posicionándose en el offset correcto)
        char* block_path = get_physical_block_path(storage, physical_block_to_write);
        int fd = open(block_path, O_RDWR);
        if (fd == -1) {
            log_error(logger, "STORAGE_WRITE_FILE: Error al abrir bloque físico %d: %s",
                      physical_block_to_write, strerror(errno));
            free(block_path);
            break;
        }

        // Calcular cuánto escribir en este bloque
        uint32_t writable = block_size - offset_in_block;
        if (writable > remaining) writable = remaining;

        // ✅ POSICIONARSE EN EL OFFSET DENTRO DEL BLOQUE
        lseek(fd, (off_t)offset_in_block, SEEK_SET);
        ssize_t written_bytes = write(fd, src, writable);
        
        if (written_bytes != (ssize_t)writable) {
            log_error(logger, "STORAGE_WRITE_FILE: Error en write bloque %d",
                      physical_block_to_write);
            close(fd);
            free(block_path);
            break;
        }

        fsync(fd);  // Forzar escritura
        close(fd);
        free(block_path);
        
        log_info(logger, "WRITE: Escritos %zd bytes en bloque físico %d (lógico %u) en offset %u",
                 written_bytes, physical_block_to_write, bloque_logico, offset_in_block);
        
        apply_block_access_delay(storage, 1);

        // Avanzar al siguiente bloque
        remaining -= writable;
        src += writable;
        bloque_logico++;
        offset_in_block = 0;  // En bloques siguientes, empezamos desde 0
    }

    if (blocks_modified) {
        char new_blocks_str[4096] = "[";
        for (int i = 0; i < list_size(blocks); i++) {
            int block_num = (int)(long)list_get(blocks, i);
            char block_str[20];
            snprintf(block_str, sizeof(block_str), "%d", block_num);
            strcat(new_blocks_str, block_str);
            if (i < list_size(blocks) - 1) strcat(new_blocks_str, ",");
        }
        strcat(new_blocks_str, "]");
        
        config_set_value(metadata, "BLOCKS", new_blocks_str);
        
        if (!config_save(metadata)) {
            log_error(logger, "STORAGE_WRITE_FILE: Error al guardar metadatos actualizados");
        } else {
            log_info(logger, "STORAGE_WRITE_FILE: Metadatos actualizados por cambios en bloques");
        }
    }

    list_destroy(blocks);
    config_destroy(metadata);
    pthread_mutex_unlock(&storage->mutex);

    if (remaining > 0) {
        log_error(logger,
                  "STORAGE_WRITE_FILE: quedó sin escribir %u bytes para %s:%s",
                  remaining, filename, tag);
        return -1;
    }

    log_info(logger,
             "STORAGE_WRITE_FILE: Escritura exitosa de %u bytes en %s:%s (offset inicial=%u)",
             size, filename, tag, offset);
    return 0;
}

// Función para obtener la ruta de un bloque físico
char* get_physical_block_path(storage_t* storage, int block_num) {
    char* path = malloc(MAX_PATH_LENGTH * 2);
    if (!path) return NULL;
    
    int written = snprintf(path, MAX_PATH_LENGTH * 2, "%s/%s/block%04d.dat", 
                          storage->root_path, PHYSICAL_BLOCKS_DIR, block_num);
    
    if (written < 0 || written >= MAX_PATH_LENGTH * 2) {
        log_error(logger, "Path demasiado largo para bloque físico %d", block_num);
        free(path);
        return NULL;
    }
    
    return path;
}

int storage_write_block(storage_t* storage, const char* filename, const char* tag, size_t block_num, const void* data, uint32_t query_id) {
    pthread_mutex_lock(&storage->mutex);
    apply_operation_delay(storage);
    
    log_info(logger, "WRITE_BLOCK: Escribiendo bloque %zu de %s:%s", block_num, filename, tag);
    
    // Verificar que el archivo existe
    char metadata_path[MAX_PATH_LENGTH];
    safe_path_join(
        metadata_path, sizeof(metadata_path),
        "%s/%s/%s/%s/%s",
        storage->root_path, FILES_DIR, filename, tag, METADATA_FILENAME
    );

    
    if (access(metadata_path, F_OK) != 0) {
        log_error(logger, "WRITE_BLOCK: Archivo %s:%s no existe", filename, tag);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    // Verificar estado (no se puede escribir en COMMITED)
    if (get_file_status(storage, filename, tag) == COMMITED) {
        log_error(logger, "WRITE_BLOCK: No se puede escribir en archivo COMMITED %s:%s", filename, tag);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    // Verificar que el bloque existe
    t_list* blocks = get_file_blocks(storage, filename, tag);
    if (block_num >= list_size(blocks)) {
        log_error(logger, "WRITE_BLOCK: Bloque %zu fuera de límites", block_num);
        list_destroy(blocks);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    int current_block = (int)(long)list_get(blocks, block_num);
    
    // Verificar si el bloque es referenciado por múltiples archivos
    bool shared = false;
    
    if (shared) {
        // Asignar nuevo bloque físico
        int new_block = allocate_physical_block(storage, query_id);
        if (new_block == -1) {
            log_error(logger, "WRITE_BLOCK: No hay bloques libres");
            list_destroy(blocks);
            pthread_mutex_unlock(&storage->mutex);
            return -1;
        }
        
        // Escribir datos en el nuevo bloque
        char* new_block_path = get_physical_block_path(storage, new_block);
        int fd = open(new_block_path, O_WRONLY | O_CREAT, 0644);
        if (fd == -1) {
            log_error(logger, "WRITE_BLOCK: Error al abrir bloque %d: %s", new_block, strerror(errno));
            free(new_block_path);
            list_destroy(blocks);
            pthread_mutex_unlock(&storage->mutex);
            return -1;
        }
        
        ssize_t written = write(fd, data, storage->block_size);

        // ✅ Verificar que se escribió todo el contenido
        if (written != storage->block_size) {
            log_error(logger, "Escritura incompleta: %zd de %zu bytes", written, storage->block_size);
            close(fd);
            return -1;
        }
        close(fd);
        
        // Actualizar bloque lógico
        list_replace(blocks, block_num, (void*)(long)new_block);
        
        // Actualizar hard link
        char* logical_path = get_logical_block_path(storage, filename, tag, block_num);
        unlink(logical_path);
        link(new_block_path, logical_path);
        
        free(new_block_path);
        free(logical_path);
        
    } else {
        // Escribir directamente en el bloque físico existente
        char* block_path = get_physical_block_path(storage, current_block);
        int fd = open(block_path, O_WRONLY);
        if (fd == -1) {
            log_error(logger, "WRITE_BLOCK: Error al abrir bloque %d: %s", current_block, strerror(errno));
            free(block_path);
            list_destroy(blocks);
            pthread_mutex_unlock(&storage->mutex);
            return -1;
        }
        
        write(fd, data, storage->block_size);
        close(fd);
        free(block_path);
    }
    
    // Aplicar delay por acceso a bloque
    apply_block_access_delay(storage, 1);
    
    list_destroy(blocks);
    log_info(logger, "WRITE_BLOCK: Escritura completada en bloque %zu de %s:%s", block_num, filename, tag);
    
    pthread_mutex_unlock(&storage->mutex);
    return 0;
}

void manejar_write_file(int socket_cliente, uint32_t query_id) {
    log_info(logger, "Manejando WRITE_FILE");

    // 1) Recibir filename
    char* filename = recibir_string_del_worker(socket_cliente);
    if (!filename) {
        log_error(logger, "Error al recibir filename en WRITE_FILE");
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        return;
    }
    log_info(logger, "WRITE_FILE - Filename recibido: %s", filename);

    // 2) Recibir tag
    char* tag = recibir_string_del_worker(socket_cliente);
    if (!tag) {
        log_error(logger, "Error al recibir tag en WRITE_FILE (tag)");
        free(filename);
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        return;
    }
    log_info(logger, "WRITE_FILE - Tag recibido: %s", tag);

    // 3) Recibir offset (uint32_t)
    uint32_t offset_net;
    ssize_t bytes_recv = recv(socket_cliente, &offset_net, sizeof(uint32_t), MSG_WAITALL);
    if (bytes_recv != sizeof(uint32_t)) {
        log_error(logger, "Error recibiendo offset en WRITE_FILE: %zd bytes", bytes_recv);
        free(filename);
        free(tag);
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        return;
    }
    uint32_t offset = ntohl(offset_net);
    log_info(logger, "WRITE_FILE - Offset recibido: %u", offset);

    // 4) Recibir tamaño de los datos (size)
    uint32_t size_net;
    bytes_recv = recv(socket_cliente, &size_net, sizeof(uint32_t), MSG_WAITALL);
    if (bytes_recv != sizeof(uint32_t)) {
        log_error(logger, "Error recibiendo size en WRITE_FILE: %zd bytes", bytes_recv);
        free(filename);
        free(tag);
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        return;
    }
    uint32_t size = ntohl(size_net);
    log_info(logger, "WRITE_FILE - Size recibido: %u", size);

    // 5) Recibir datos
    void* data = malloc(size);
    if (!data) {
        log_error(logger, "Error al allocar buffer para WRITE_FILE (size=%u)", size);
        free(filename);
        free(tag);
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        return;
    }

    bytes_recv = recv(socket_cliente, data, size, MSG_WAITALL);
    if (bytes_recv != (ssize_t)size) {
        log_error(logger, "Datos incompletos en WRITE_FILE: %zd de %u bytes", bytes_recv, size);
        free(filename);
        free(tag);
        free(data);
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        return;
    }

    log_info(logger, "WRITE_FILE recibido: %s:%s, offset=%u, size=%u", 
             filename, tag, offset, size);

    // 6) Llamar a la función de alto nivel que escribe con offset
    int result = storage_write_file(global_storage, filename, tag, offset, data, size, query_id);

    if (result == 0) {
        uint32_t bloque_logico = offset / global_storage->block_size;
        logging_bloque_logico_escrito(query_id, filename, tag, bloque_logico);
        
        enviar_ok(socket_cliente);

    } else {
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        log_error(logger, "WRITE_FILE falló para: %s:%s (offset=%u, size=%u)",
                  filename, tag, offset, size);
    }

    free(filename);
    free(tag);
    free(data);

    log_info(logger, "WRITE_FILE - Procesamiento completamente finalizado");
}


// FLUSH
// FLUSH - VERSIÓN CORREGIDA
int storage_flush_file(storage_t* storage, const char* filename, const char* tag) {
    pthread_mutex_lock(&storage->mutex);
    apply_operation_delay(storage);
    
    log_info(logger, "FLUSH: Procesando %s:%s", filename, tag);
    
    // 1. Verificar que el archivo existe
    char metadata_path[MAX_PATH_LENGTH];
    safe_path_join(
        metadata_path, sizeof(metadata_path),
        "%s/%s/%s/%s/%s",
        storage->root_path, FILES_DIR, filename, tag, METADATA_FILENAME
    );
    
    if (access(metadata_path, F_OK) != 0) {
        log_error(logger, "FLUSH: Archivo %s:%s no existe", filename, tag);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    // ✅ NUEVO: Cargar metadatos para verificar estado COMMITTED
    t_config* metadata = config_create(metadata_path);
    if (!metadata) {
        log_error(logger, "FLUSH: Error al cargar metadata de %s:%s", filename, tag);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    // ✅ NUEVO: Verificar si el archivo está COMMITTED
    if (config_has_property(metadata, "COMMITTED")) {
        const char* committed_value = config_get_string_value(metadata, "COMMITTED");
        if (committed_value && strcmp(committed_value, "1") == 0) {
            log_info(logger, "FLUSH: %s:%s está COMMITTED - Operación nula (según especificación)", filename, tag);
            config_destroy(metadata);
            pthread_mutex_unlock(&storage->mutex);
            return 0; // ✅ ÉXITO pero operación nula
        }
    }
    
    // 2. Sincronizar metadatos actualizados (solo si NO está COMMITTED)
    log_info(logger, "FLUSH: Sincronizando %s:%s (no COMMITTED)", filename, tag);
    
    // Verificar si hay cambios pendientes en metadatos (como tamaño)
    size_t current_size = config_get_int_value(metadata, "TAMAÑO");
    t_list* blocks = get_file_blocks(storage, filename, tag);
    size_t calculated_size = list_size(blocks) * storage->block_size;
    
    // Si el tamaño calculado difiere del guardado, actualizar
    if (current_size != calculated_size) {
        log_info(logger, "FLUSH: Actualizando tamaño de %zu a %zu", current_size, calculated_size);
        config_set_value(metadata, "TAMAÑO", string_itoa(calculated_size));
        
        // Actualizar lista de bloques si es necesario
        char blocks_str[4096] = "[";
        for (int i = 0; i < list_size(blocks); i++) {
            char num[20];
            snprintf(num, sizeof(num), "%d", (int)(long)list_get(blocks, i));
            strcat(blocks_str, num);
            if (i < list_size(blocks) - 1) strcat(blocks_str, ",");
        }
        strcat(blocks_str, "]");
        
        config_set_value(metadata, "BLOCKS", blocks_str);
    }
    
    // Guardar metadatos actualizados
    if (!config_save(metadata)) {
        log_error(logger, "FLUSH: Error al guardar metadatos de %s:%s", filename, tag);
        config_destroy(metadata);
        list_destroy(blocks);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    config_destroy(metadata);
    
    // 3. Forzar persistencia de todos los bloques (fsync) - solo si NO está COMMITTED
    for (int i = 0; i < list_size(blocks); i++) {
        int physical_block = (int)(long)list_get(blocks, i);
        char* block_path = get_physical_block_path(storage, physical_block);
        
        if (block_path) {
            // Abrir y hacer fsync para forzar escritura a disco
            int fd = open(block_path, O_RDONLY);
            if (fd != -1) {
                fsync(fd);  // Forzar escritura a disco
                close(fd);
            }
            free(block_path);
        }
        
        // Aplicar delay por acceso a bloque
        apply_block_access_delay(storage, 1);
    }
    
    list_destroy(blocks);
    
    // 4. Forzar persistencia de metadatos también
    int metadata_fd = open(metadata_path, O_RDONLY);
    if (metadata_fd != -1) {
        fsync(metadata_fd);
        close(metadata_fd);
    }
    
    log_info(logger, "FLUSH: Sincronización completada para %s:%s", filename, tag);
    
    pthread_mutex_unlock(&storage->mutex);
    return 0;
}

void manejar_flush_file(int socket_cliente, uint32_t query_id) {
    log_info(logger, "Manejando FLUSH_FILE");
    
    // Recibir filename
    char* filename = recibir_string_del_worker(socket_cliente);
    if (!filename) {
        log_error(logger, "Error al recibir filename en FLUSH_FILE");
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        return;
    }

    log_info(logger, "FLUSH_FILE - Filename recibido: %s", filename);
    
    // Recibir tag
    char* tag = recibir_string_del_worker(socket_cliente);
    if (!tag) {
        free(filename);
        log_error(logger, "Error al recibir tag en FLUSH_FILE");
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        return;
    }

    log_info(logger, "FLUSH_FILE - Tag recibido: %s", tag);
    log_info(logger, "FLUSH_FILE recibido para: %s:%s", filename, tag);
    
    // Llamar a la función storage_flush_file
    int result = storage_flush_file(global_storage, filename, tag);
    
    // ✅ MODIFICADO: Siempre enviar OK si el archivo existe, incluso si está COMMITTED
    if (result == 0) {
        enviar_ok(socket_cliente);
        log_info(logger, "FLUSH_FILE completado para: %s:%s", filename, tag);
    } else {
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        log_error(logger, "FLUSH_FILE falló para: %s:%s", filename, tag);
    }
    
    free(filename);
    free(tag);

    log_info(logger, "FLUSH_FILE - Procesamiento completamente finalizado");
}

// COMMIT
// Función para calcular hash de un bloque (para deduplicación)
unsigned char* calculate_block_hash(const void* data, size_t size, unsigned char* hash_out) {
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) return NULL;

    if (EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) != 1) goto error;
    if (EVP_DigestUpdate(ctx, data, size) != 1) goto error;

    unsigned int len;
    if (EVP_DigestFinal_ex(ctx, hash_out, &len) != 1) goto error;

    EVP_MD_CTX_free(ctx);
    return hash_out;

error:
    EVP_MD_CTX_free(ctx);
    return NULL;
}

// Función para buscar bloque por hash (deduplicación)
int find_block_by_hash(storage_t* storage, const char* hash) {
    // Buscar en el diccionario
    if (dictionary_has_key(storage->blocks_hash_index, (char*)hash)) {
        char* block_name = dictionary_get(storage->blocks_hash_index, (char*)hash);
        
        // Extraer número del nombre "block0003" -> 3
        int block_num = -1;
        if (sscanf(block_name, "block%d", &block_num) == 1) {
            log_debug(logger, "Hash encontrado en índice: %s -> %s (bloque %d)", 
                     hash, block_name, block_num);
            return block_num;
        }
    }
    
    return -1; // No encontrado
}

// Verificar si un file:tag está COMMITTED
bool is_file_committed(storage_t* storage, const char* filename, const char* tag) {
    char metadata_path[MAX_PATH_LENGTH];
    safe_path_join(
        metadata_path, sizeof(metadata_path),
        "%s/%s/%s/%s/%s",
        storage->root_path, FILES_DIR, filename, tag, METADATA_FILENAME
    );
    
    if (access(metadata_path, F_OK) != 0) {
        return false; // No existe
    }
    
    t_config* metadata = config_create(metadata_path);
    if (!metadata) {
        return false;
    }
    
    // Verificar por campo "COMMITTED" explícito
    if (config_has_property(metadata, "COMMITTED")) {
        const char* committed_value = config_get_string_value(metadata, "COMMITTED");
        if (committed_value && strcmp(committed_value, "1") == 0) {
            config_destroy(metadata);
            return true;
        }
    }
    
    // Verificar por campo "ESTADO" para compatibilidad
    if (config_has_property(metadata, "ESTADO")) {
        const char* estado = config_get_string_value(metadata, "ESTADO");
        if (estado && strcmp(estado, "COMMITED") == 0) {
            config_destroy(metadata);
            return true;
        }
    }
    
    config_destroy(metadata);
    return false;
}

int storage_commit_tag(storage_t* storage, const char* filename, const char* tag, uint32_t query_id) {
    pthread_mutex_lock(&storage->mutex);
    apply_operation_delay(storage);
    
    log_info(logger, "COMMIT_TAG: Confirmando %s:%s", filename, tag);
    
    // Verificar que el archivo existe
    char metadata_path[MAX_PATH_LENGTH];
    safe_path_join(
        metadata_path, sizeof(metadata_path),
        "%s/%s/%s/%s/%s",
        storage->root_path, FILES_DIR, filename, tag, METADATA_FILENAME
    );
    
    if (access(metadata_path, F_OK) != 0) {
        log_error(logger, "COMMIT_TAG: Archivo %s:%s no existe", filename, tag);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    // ✅ NUEVO: Verificar si ya está COMMITED para evitar trabajo innecesario
    if (get_file_status(storage, filename, tag) == COMMITED) {
        log_info(logger, "COMMIT_TAG: %s:%s ya está COMMITED - Operación nula", filename, tag);
        pthread_mutex_unlock(&storage->mutex);
        return 0;
    }
    
    // ✅ NUEVO: HACER FLUSH IMPLÍCITO ANTES DE COMMIT (según especificación)
    log_info(logger, "COMMIT_TAG: Realizando FLUSH implícito para %s:%s", filename, tag);
    
    t_list* blocks = get_file_blocks(storage, filename, tag);
    
    // Forzar persistencia de todos los bloques (fsync)
    for (int i = 0; i < list_size(blocks); i++) {
        int physical_block = (int)(long)list_get(blocks, i);
        char* block_path = get_physical_block_path(storage, physical_block);
        
        if (block_path) {
            int fd = open(block_path, O_RDONLY);
            if (fd != -1) {
                fsync(fd);  // Forzar escritura a disco
                close(fd);
            }
            free(block_path);
        }
        apply_block_access_delay(storage, 1);
    }
    
    // ✅ NUEVO: Forzar persistencia de metadatos también
    int metadata_fd = open(metadata_path, O_RDONLY);
    if (metadata_fd != -1) {
        fsync(metadata_fd);
        close(metadata_fd);
    }
    
    log_info(logger, "COMMIT_TAG: FLUSH implícito completado para %s:%s", filename, tag);
    
    // CONTINUAR CON EL CÓDIGO ORIGINAL DE COMMIT (deduplicación, etc.)
    t_config* metadata = config_create(metadata_path);
    bool hashes_modificados = false;
    
    // Por cada bloque, verificar si existe otro con el mismo contenido
    for (int i = 0; i < list_size(blocks); i++) {
        int current_block = (int)(long)list_get(blocks, i);
        
        // Leer bloque físico
        char* block_path = get_physical_block_path(storage, current_block);
        FILE* f = fopen(block_path, "rb");
        if (!f) {
            free(block_path);
            continue;
        }
        
        void* data = malloc(storage->block_size);
        size_t bytes_read = fread(data, 1, storage->block_size, f);
        fclose(f);
        free(block_path);
        
        if (bytes_read != storage->block_size) {
            log_warning(logger, "COMMIT: Bloque %d incompleto (%zu de %zu bytes)", 
                       current_block, bytes_read, storage->block_size);
            free(data);
            continue;
        }
        
        // Calcular hash MD5
        char* current_hash = crypto_md5((unsigned char*)data, storage->block_size);
        free(data);
        
        if (!current_hash) {
            log_error(logger, "COMMIT: Error calculando hash del bloque %d", current_block);
            continue;
        }
        
        log_debug(logger, "COMMIT: Bloque %d -> Hash MD5: %s", current_block, current_hash);
        
        // Buscar bloque duplicado
        int existing_block = find_block_by_hash(storage, current_hash);
        
        if (existing_block != -1 && existing_block != current_block) {
            logging_deduplicacion_bloque(query_id, filename, tag, i, current_block, existing_block);
    
            
            log_info(logger, "COMMIT: Deduplicación - Bloque %d -> %d (hash: %s)", 
                     current_block, existing_block, current_hash);
        
            // Verificar si el bloque actual es referenciado por otros archivos
            bool referenced = false;
            
            // Liberar bloque actual si no es referenciado por otros
            if (!referenced) {
                free_physical_block(storage, current_block, query_id);
            }
            
            // Actualizar bloque lógico para que apunte al bloque existente
            list_replace(blocks, i, (void*)(long)existing_block);
            
            // Actualizar hard link
            char* physical_path = get_physical_block_path(storage, existing_block);
            char* logical_path = get_logical_block_path(storage, filename, tag, i);
            
            if (physical_path && logical_path) {
                logging_hard_link_eliminado(query_id, filename, tag, i, current_block);

                unlink(logical_path); // Eliminar link anterior
                link(physical_path, logical_path); // Crear nuevo link

                logging_hard_link_agregado(query_id, filename, tag, i, existing_block);
            }
            
            free(physical_path);
            free(logical_path);
        } else {
            // GUARDAR HASH NUEVO
            char block_name[20];
            snprintf(block_name, sizeof(block_name), "block%04d", current_block);
            
            // Verificar si ya existe
            if (!dictionary_has_key(storage->blocks_hash_index, current_hash)) {
                log_info(logger, "COMMIT: Registrando hash %s -> %s", 
                         current_hash, block_name);
                dictionary_put(storage->blocks_hash_index, current_hash, strdup(block_name));
                hashes_modificados = true;
            }
        }
        
        free(current_hash);
    }

    // PERSISTIR HASHES AL ARCHIVO SI HAY CAMBIOS
    if (hashes_modificados) {
        char hash_index_path[MAX_PATH_LENGTH * 2];
        int written = snprintf(hash_index_path, sizeof(hash_index_path), 
                              "%s/blocks_hash_index.config", storage->root_path);
        
        if (written > 0 && written < (int)sizeof(hash_index_path)) {
            if (guardar_hash_index(storage->blocks_hash_index, hash_index_path) == 0) {
                log_info(logger, "COMMIT: blocks_hash_index.config actualizado");
            } else {
                log_error(logger, "COMMIT: Error al guardar blocks_hash_index.config");
            }
        }
    }
    
    // Actualizar metadata con nueva lista de bloques y estado COMMITED
    char new_blocks_str[4096] = "[";
    for (int i = 0; i < list_size(blocks); i++) {
        int block_num = (int)(long)list_get(blocks, i);
        char block_str[20];
        snprintf(block_str, sizeof(block_str), "%d", block_num);
        strcat(new_blocks_str, block_str);
        if (i < list_size(blocks) - 1) strcat(new_blocks_str, ",");
    }
    strcat(new_blocks_str, "]");
    
    config_set_value(metadata, "BLOCKS", new_blocks_str);
    config_set_value(metadata, "ESTADO", "COMMITED");
    
    // ✅ NUEVO: Agregar campo COMMITTED explícito para FLUSH
    config_set_value(metadata, "COMMITTED", "1");
    
    if (!config_save(metadata)) {
        log_error(logger, "COMMIT_TAG: Error al guardar metadata de %s:%s", filename, tag);
        config_destroy(metadata);
        list_destroy(blocks);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    // ✅ NUEVO: Forzar persistencia final de metadatos actualizados
    metadata_fd = open(metadata_path, O_RDONLY);
    if (metadata_fd != -1) {
        fsync(metadata_fd);
        close(metadata_fd);
    }
    
    list_destroy(blocks);
    config_destroy(metadata);
    
    log_info(logger, "COMMIT_TAG: %s:%s confirmado exitosamente", filename, tag);
    
    pthread_mutex_unlock(&storage->mutex);
    return 0;
}

void manejar_commit_file(int socket_cliente, uint32_t query_id) {
    log_info(logger, "Manejando COMMIT_FILE");
    
    // Recibir filename
    char* filename = recibir_string_del_worker(socket_cliente);
    if (!filename) {
        log_error(logger, "Error al recibir filename en COMMIT_FILE");
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        return;
    }

    log_info(logger, "COMMIT_FILE - Filename recibido: %s", filename);
    
    // Recibir tag
    char* tag = recibir_string_del_worker(socket_cliente);
    if (!tag) {
        free(filename);
        log_error(logger, "Error al recibir tag en COMMIT_FILE");
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        return;
    }

    log_info(logger, "COMMIT_FILE - Tag recibido: %s", tag);
    log_info(logger, "COMMIT_FILE recibido para: %s:%s", filename, tag);
    
    // Llamar a la función storage_commit_tag
    int result = storage_commit_tag(global_storage, filename, tag, query_id);
    
    if (result == 0) {
        logging_commit_tag(query_id, filename, tag);
        enviar_ok(socket_cliente);
        
    } else {
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        log_error(logger, "COMMIT_FILE falló para: %s:%s", filename, tag);
    }
    
    free(filename);
    free(tag);

    log_info(logger, "COMMIT_FILE - Procesamiento completamente finalizado");
}

// READ
int storage_read_block(storage_t* storage, const char* filename, const char* tag, size_t block_num, void* buffer, size_t buffer_size) {
    pthread_mutex_lock(&storage->mutex);
    apply_operation_delay(storage);
    
    log_info(logger, "READ_BLOCK: Leyendo bloque %zu de %s:%s", block_num, filename, tag);
    
    // Verificar que el archivo existe
    char metadata_path[MAX_PATH_LENGTH * 2];
    int written = snprintf(metadata_path, sizeof(metadata_path), "%s/%s/%s/%s/%s", 
                          storage->root_path, FILES_DIR, filename, tag, METADATA_FILENAME);
    
    if (written < 0 || written >= (int)sizeof(metadata_path)) {
        log_error(logger, "READ_BLOCK: Path demasiado largo");
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    if (access(metadata_path, F_OK) != 0) {
        log_error(logger, "READ_BLOCK: Archivo %s:%s no existe", filename, tag);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    // Verificar que el bloque existe
    t_list* blocks = get_file_blocks(storage, filename, tag);
    if (!blocks) {
        log_error(logger, "READ_BLOCK: Error obteniendo bloques para %s:%s", filename, tag);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    // ✅ VERIFICAR LÍMITES ANTES DE CONTINUAR
    if (block_num >= list_size(blocks)) {
        log_warning(logger, "READ_BLOCK: Bloque %zu fuera de límites (max: %d) para %s:%s", 
                   block_num, list_size(blocks), filename, tag);
        list_destroy(blocks);
        pthread_mutex_unlock(&storage->mutex);
        return -2; // Código especial para "fuera de límites"
    }
    
    int physical_block = (int)(long)list_get(blocks, block_num);
    log_info(logger, "READ_BLOCK: Bloque lógico %zu -> bloque físico %d", 
             block_num, physical_block);
    
    char* block_path = get_physical_block_path(storage, physical_block);
    if (!block_path) {
        log_error(logger, "READ_BLOCK: Error al construir path del bloque");
        list_destroy(blocks);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    log_info(logger, "READ_BLOCK: Abriendo bloque físico: %s", block_path);
    int fd = open(block_path, O_RDONLY);
    if (fd == -1) {
        log_error(logger, "READ_BLOCK: Error al abrir bloque %d: %s", 
                 physical_block, strerror(errno));
        free(block_path);
        list_destroy(blocks);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    // ✅ LEER EXACTAMENTE storage->block_size BYTES
    ssize_t bytes_read = read(fd, buffer, storage->block_size);
    close(fd);
    
    log_info(logger, "READ_BLOCK: Leídos %zd bytes de %zu solicitados (BLOCK_SIZE=%zu)", 
             bytes_read, storage->block_size, storage->block_size);
    
    // ✅ Si el archivo tiene menos de 16 bytes, llenar con ceros
    if (bytes_read < (ssize_t)storage->block_size) {
        log_warning(logger, "READ_BLOCK: Bloque incompleto (%zd de %zu bytes), llenando con ceros", 
                   bytes_read, storage->block_size);
        memset((char*)buffer + bytes_read, 0, storage->block_size - bytes_read);
    }
    
    free(block_path);
    list_destroy(blocks);
    
    apply_block_access_delay(storage, 1);
    
    if (bytes_read < 0) {
        log_error(logger, "READ_BLOCK: Error al leer bloque: %s", strerror(errno));
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    log_info(logger, "READ_BLOCK: Lectura completada del bloque %zu de %s:%s", 
             block_num, filename, tag);
    
    pthread_mutex_unlock(&storage->mutex);
    return 0; // Éxito
}

void manejar_read_page(int socket_cliente, uint32_t query_id) {
    log_info(logger, "Manejando READ_PAGE");
    
    // Recibir file_tag completo (filename:tag)
    char* file_tag = recibir_string_del_worker(socket_cliente);
    if (!file_tag) {
        log_error(logger, "Error al recibir file_tag en READ");
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        return;
    }

    log_info(logger, "READ_PAGE - file_tag recibido: %s", file_tag);
    
    // Recibir número de página
    uint32_t pagina_network;
    ssize_t bytes_recv = recv(socket_cliente, &pagina_network, sizeof(uint32_t), MSG_WAITALL);
    if (bytes_recv <= 0) {
        log_error(logger, "Error al recibir número de página");
        free(file_tag);
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        return;
    }
    uint32_t pagina = ntohl(pagina_network);
    
    log_info(logger, "READ PAGE solicitado: %s, página %u", file_tag, pagina);
    
    // Parsear file_tag
    char* filename = NULL;
    char* tag = NULL;
    char* separador = strchr(file_tag, ':');
    
    if (separador) {
        *separador = '\0';
        filename = strdup(file_tag);
        tag = strdup(separador + 1);
        
        if (strlen(tag) == 0) {
            free(tag);
            tag = strdup("BASE");
        }
    } else {
        filename = strdup(file_tag);
        tag = strdup("BASE");
    }
    
    log_info(logger, "READ_PAGE parseado: filename='%s', tag='%s'", filename, tag);
    
    // Allocar buffer con el tamaño REAL del bloque del Storage
    void* buffer = malloc(global_storage->block_size);
    if (!buffer) {
        log_error(logger, "Error al allocar buffer para lectura");
        free(filename);
        free(tag);
        free(file_tag);
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        return;
    }
    
    // Inicializar con ceros
    memset(buffer, 0, global_storage->block_size);
    
    // Leer el bloque usando storage_read_block
    int result = storage_read_block(global_storage, filename, tag, pagina, 
                                    buffer, global_storage->block_size);
    
    // MANEJAR DIFERENTES RESULTADOS
    if (result == -2) {
        // CASO ESPECIAL: Bloque fuera de límites - ENVIAR BLOQUE VACÍO
        logging_bloque_logico_leido(query_id, filename, tag, pagina);
        
        // Enviar respuesta exitosa con bloque vacío
        enviar_ok(socket_cliente);
        
        // Enviar el tamaño CORRECTO del bloque (16 bytes)
        uint32_t size = global_storage->block_size; // Esto debería ser 16
        uint32_t size_network = htonl(size);
        
        log_info(logger, "Enviando tamaño de bloque: %u bytes", size);
        ssize_t sent_bytes = send(socket_cliente, &size_network, sizeof(uint32_t), MSG_NOSIGNAL);
        
        if (sent_bytes <= 0) {
            log_error(logger, "Error al enviar tamaño del bloque");
        } else {
            log_info(logger, "Tamaño enviado: %zd bytes", sent_bytes);
        }
        
        // Enviar exactamente 'size' bytes de datos
        sent_bytes = send(socket_cliente, buffer, size, MSG_NOSIGNAL);
        
        if (sent_bytes != (ssize_t)size) {
            log_error(logger, "Error al enviar datos del bloque: %zd de %u bytes enviados", 
                     sent_bytes, size);
        } else {
            log_info(logger, "Bloque vacío enviado exitosamente: %u bytes", size);
        }
        
    } else if (result != 0) {
        // ... manejo de error ...
    } else {
        // ÉXITO - ENVIAR BLOQUE NORMAL (también corregir aquí)
        log_info(logger, "Bloque %u de %s:%s leído exitosamente", pagina, filename, tag);
        
        // Enviar respuesta exitosa
        enviar_ok(socket_cliente);
        
        // Enviar tamaño CORRECTO
        uint32_t size = global_storage->block_size; // 16 bytes
        uint32_t size_network = htonl(size);
        
        log_info(logger, "Enviando tamaño de bloque: %u bytes", size);
        ssize_t bytes_sent = send(socket_cliente, &size_network, sizeof(uint32_t), MSG_NOSIGNAL);
        
        if (bytes_sent <= 0) {
            log_error(logger, "Error al enviar tamaño del buffer");
        } else {
            log_info(logger, "Tamaño del buffer enviado: %u bytes", size);
        }
        
        // Enviar exactamente 'size' bytes
        bytes_sent = send(socket_cliente, buffer, size, MSG_NOSIGNAL);
        
        if (bytes_sent != (ssize_t)size) {
            log_error(logger, "Error al enviar datos del bloque: %zd de %u bytes", 
                     bytes_sent, size);
        } else {
            log_info(logger, "Datos del bloque enviados exitosamente: %zd bytes", bytes_sent);
        }
    }
    
    free(buffer);
    free(filename);
    free(tag);
    free(file_tag);

    log_info(logger, "READ_PAGE - Procesamiento completamente finalizado para página: %u", pagina);
}

// TAG
int storage_tag_file(storage_t* storage, const char* filename, const char* source_tag, const char* dest_tag, uint32_t query_id) {
    pthread_mutex_lock(&storage->mutex);
    apply_operation_delay(storage);
    
    log_info(logger, "TAG_FILE: Copiando %s:%s a %s:%s con bloques independientes", filename, source_tag, filename, dest_tag);
    
    // 1. Verificar que el archivo origen existe
    char source_metadata_path[MAX_PATH_LENGTH];
    safe_path_join(
        source_metadata_path, sizeof(source_metadata_path),
        "%s/%s/%s/%s/%s",
        storage->root_path, FILES_DIR, filename, source_tag, METADATA_FILENAME
    );
    
    if (access(source_metadata_path, F_OK) != 0) {
        log_error(logger, "TAG_FILE: Archivo origen %s:%s no existe", filename, source_tag);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    // 2. Verificar que el tag destino no existe
    char dest_dir_path[MAX_PATH_LENGTH];
    safe_path_join(
        dest_dir_path, sizeof(dest_dir_path),
        "%s/%s/%s/%s",
        storage->root_path, FILES_DIR, filename, dest_tag
    );
    
    if (access(dest_dir_path, F_OK) == 0) {
        log_error(logger, "TAG_FILE: Tag destino %s ya existe", dest_tag);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    // 3. Crear estructura del tag destino
    log_info(logger, "TAG_FILE: Creando estructura destino...");
    if (create_file_structure(storage, filename, dest_tag) != 0) {
        log_error(logger, "TAG_FILE: Error al crear estructura destino");
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    // 4. Verificar que la estructura se creó
    char dest_metadata_path[MAX_PATH_LENGTH];
    safe_path_join(dest_metadata_path, sizeof(dest_metadata_path),
        "%s/%s/%s/%s/%s", storage->root_path, FILES_DIR, filename, dest_tag, METADATA_FILENAME);
    
    usleep(100000); // 100ms para permitir que el sistema de archivos se actualice
    
    struct stat st;
    if (stat(dest_dir_path, &st) == -1) {
        log_error(logger, "TAG_FILE: Directorio destino no se creó: %s", dest_dir_path);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    // 5. Crear metadata destino
    log_info(logger, "TAG_FILE: Creando metadata destino en: %s", dest_metadata_path);
    
    FILE* metadata_file = fopen(dest_metadata_path, "w");
    if (!metadata_file) {
        log_error(logger, "TAG_FILE: Error al crear archivo metadata: %s", strerror(errno));
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    fclose(metadata_file);
    
    // Cargar metadata origen primero para obtener información
    t_config* source_metadata = config_create(source_metadata_path);
    if (!source_metadata) {
        log_error(logger, "TAG_FILE: No se pudo abrir metadata origen %s:%s", filename, source_tag);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }

    // Obtener bloques del archivo origen
    t_list* source_blocks = get_file_blocks(storage, filename, source_tag);
    if (!source_blocks) {
        log_error(logger, "TAG_FILE: Error al obtener bloques del archivo origen");
        config_destroy(source_metadata);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }

    // PATH: blocks destino
    char dest_blocks_path[MAX_PATH_LENGTH];
    safe_path_join(
        dest_blocks_path, sizeof(dest_blocks_path),
        "%s/%s/%s/%s/%s",
        storage->root_path, FILES_DIR, filename, dest_tag, LOGICAL_BLOCKS_DIR
    );
    
    // Crear lista para los NUEVOS bloques del destino
    t_list* dest_blocks = list_create();
    
    // COPIAR BLOQUES FÍSICOS - CREAR NUEVOS BLOQUES INDEPENDIENTES
    for (int i = 0; i < list_size(source_blocks); i++) {
        int source_physical_block = (int)(long)list_get(source_blocks, i);
        
        // 1. Leer bloque físico origen
        char* source_physical_path = get_physical_block_path(storage, source_physical_block);
        if (!source_physical_path) {
            log_error(logger, "TAG_FILE: Error obteniendo path del bloque físico %d", source_physical_block);
            continue;
        }
        
        int src_fd = open(source_physical_path, O_RDONLY);
        if (src_fd == -1) {
            log_error(logger, "TAG_FILE: Error al abrir bloque origen %d: %s", source_physical_block, strerror(errno));
            free(source_physical_path);
            continue;
        }
        
        // 2. Reservar NUEVO bloque físico para destino
        int dest_physical_block = reservar_bloque_libre(storage, query_id);
        if (dest_physical_block == -1) {
            log_error(logger, "TAG_FILE: No hay bloques libres para el bloque lógico %d", i);
            close(src_fd);
            free(source_physical_path);
            continue;
        }
        
        char* dest_physical_path = get_physical_block_path(storage, dest_physical_block);
        
        // 3. Crear y escribir en el nuevo bloque físico
        int dest_fd = open(dest_physical_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (dest_fd == -1) {
            log_error(logger, "TAG_FILE: Error al crear bloque destino %d: %s", dest_physical_block, strerror(errno));
            close(src_fd);
            free(source_physical_path);
            free(dest_physical_path);
            continue;
        }
        
        // 4. Copiar contenido COMPLETO del bloque origen al destino
        char* buffer = malloc(storage->block_size);
        if (!buffer) {
            log_error(logger, "TAG_FILE: Error al allocar buffer para copia");
            close(src_fd);
            close(dest_fd);
            free(source_physical_path);
            free(dest_physical_path);
            continue;
        }
        
        // Leer todo el bloque origen
        ssize_t bytes_read = read(src_fd, buffer, storage->block_size);
        if (bytes_read > 0) {
            // Escribir todo el contenido en el bloque destino
            ssize_t bytes_written = write(dest_fd, buffer, bytes_read);
            if (bytes_written != bytes_read) {
                log_error(logger, "TAG_FILE: Error copiando datos al bloque destino %d", dest_physical_block);
                free_physical_block(storage, dest_physical_block, query_id);
            } else {
                // ÉXITO: Agregar el nuevo bloque a la lista destino
                list_add(dest_blocks, (void*)(long)dest_physical_block);
                log_info(logger, "TAG_FILE: Bloque copiado %d -> %d (%zd bytes)", 
                         source_physical_block, dest_physical_block, bytes_written);
                
                // 5. Crear hard link para el bloque lógico destino
                char dest_logical_path[MAX_PATH_LENGTH];
                safe_path_join(
                    dest_logical_path, sizeof(dest_logical_path),
                    "%s/%06d.dat",
                    dest_blocks_path, i
                );
                
                // Crear hard link al NUEVO bloque físico
                if (link(dest_physical_path, dest_logical_path) == -1) {
                    log_error(logger, "TAG_FILE: Error al crear hard link para bloque %d: %s", 
                             dest_physical_block, strerror(errno));
                } else {
                    log_debug(logger, "TAG_FILE: Creado hard link %s -> %s", dest_logical_path, dest_physical_path);

                    logging_hard_link_agregado(query_id, filename, dest_tag, i, dest_physical_block);
                }
            }
        } else {
            log_error(logger, "TAG_FILE: Error leyendo bloque origen %d", source_physical_block);
            free_physical_block(storage, dest_physical_block, query_id);
        }
        
        free(buffer);
        close(src_fd);
        close(dest_fd);
        free(source_physical_path);
        free(dest_physical_path);
        
        // Aplicar delay por acceso a bloque
        apply_block_access_delay(storage, 1);
    }
    
    // 6. Ahora crear el metadata destino con la NUEVA lista de bloques
    t_config* dest_metadata = config_create(dest_metadata_path);
    if (!dest_metadata) {
        log_error(logger, "TAG_FILE: Error al cargar configuración de metadata destino");
        config_destroy(source_metadata);
        list_destroy(source_blocks);
        list_destroy(dest_blocks);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    // Copiar valores del metadata origen al destino
    char* tamaño = config_get_string_value(source_metadata, "TAMAÑO");
    if (tamaño) {
        config_set_value(dest_metadata, "TAMAÑO", tamaño);
    } else {
        // Calcular tamaño basado en bloques
        size_t calculated_size = list_size(dest_blocks) * storage->block_size;
        config_set_value(dest_metadata, "TAMAÑO", string_itoa(calculated_size));
    }
    
    // El nuevo tag siempre empieza en estado WORK_IN_PROGRESS
    config_set_value(dest_metadata, "ESTADO", "WORK_IN_PROGRESS");
    
    // 7. Actualizar metadata destino con la NUEVA lista de bloques
    char new_blocks_str[4096] = "[";
    for (int i = 0; i < list_size(dest_blocks); i++) {
        int block_num = (int)(long)list_get(dest_blocks, i);
        char block_str[20];
        snprintf(block_str, sizeof(block_str), "%d", block_num);
        strcat(new_blocks_str, block_str);
        if (i < list_size(dest_blocks) - 1) strcat(new_blocks_str, ",");
    }
    strcat(new_blocks_str, "]");
    
    config_set_value(dest_metadata, "BLOCKS", new_blocks_str);
    
    log_info(logger, "TAG_FILE: Nueva lista de bloques para %s:%s: %s", filename, dest_tag, new_blocks_str);
    
    if (!config_save(dest_metadata)) {
        log_error(logger, "TAG_FILE: Error al guardar metadata destino");
        config_destroy(source_metadata);
        config_destroy(dest_metadata);
        list_destroy(source_blocks);
        list_destroy(dest_blocks);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    // 8. Verificar que los metadatos se guardaron correctamente
    t_config* verify_metadata = config_create(dest_metadata_path);
    if (verify_metadata) {
        char* saved_blocks = config_get_string_value(verify_metadata, "BLOCKS");
        log_info(logger, "TAG_FILE: Bloques guardados en metadata: %s", saved_blocks ? saved_blocks : "NULL");
        config_destroy(verify_metadata);
    }
    
    // Limpiar recursos
    config_destroy(source_metadata);
    config_destroy(dest_metadata);
    list_destroy(source_blocks);
    list_destroy(dest_blocks);
    
    log_info(logger, "TAG_FILE: Copia INDEPENDIENTE completada %s:%s -> %s:%s", 
             filename, source_tag, filename, dest_tag);
    
    pthread_mutex_unlock(&storage->mutex);
    return 0;
}

void manejar_tag_file(int socket_cliente, uint32_t query_id) {
    log_info(logger, "Manejando TAG_FILE");
    
    // Recibir origen (formato: filename:tag)
    char* origen = recibir_string_del_worker(socket_cliente);
    if (!origen) {
        log_error(logger, "Error al recibir origen en TAG_FILE");
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        return;
    }

    log_info(logger, "TAG_FILE - Origen recibido: %s", origen);
    
    // Recibir destino (formato: filename:tag o solo tag)
    char* destino = recibir_string_del_worker(socket_cliente);
    if (!destino) {
        free(origen);
        log_error(logger, "Error al recibir destino en TAG_FILE");
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        return;
    }

    log_info(logger, "TAG_FILE - Destino recibido: %s", destino);
    
    // Parsear origen (filename:tag)
    char* filename_origen = NULL;
    char* source_tag = NULL;
    char* separador = strchr(origen, ':');
    
    if (separador) {
        *separador = '\0';
        filename_origen = strdup(origen);
        source_tag = strdup(separador + 1);
    } else {
        // Si no hay ':', usar el string completo como filename y tag por defecto
        filename_origen = strdup(origen);
        source_tag = strdup("BASE");
    }
    
    // Parsear destino (puede ser filename:tag o solo tag)
    char* filename_destino = NULL;
    char* dest_tag = NULL;
    separador = strchr(destino, ':');
    
    if (separador) {
        *separador = '\0';
        filename_destino = strdup(destino);
        dest_tag = strdup(separador + 1);
    } else {
        // Si no hay ':', usar el mismo filename del origen y el destino como tag
        filename_destino = strdup(filename_origen);
        dest_tag = strdup(destino);
    }
    
    log_info(logger, "TAG_FILE parseado: %s:%s -> %s:%s", 
             filename_origen, source_tag, filename_destino, dest_tag);
    
    // Verificar que los filenames coincidan (solo permitir copia entre mismo archivo)
    if (strcmp(filename_origen, filename_destino) != 0) {
        log_error(logger, "TAG_FILE: No se permite copia entre archivos diferentes (%s -> %s)", 
                 filename_origen, filename_destino);
        
        free(filename_origen);
        free(source_tag);
        free(filename_destino);
        free(dest_tag);
        free(origen);
        free(destino);
        
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        return;
    }
    
    // Llamar a la función storage_tag_file
    int result = storage_tag_file(global_storage, filename_origen, source_tag, dest_tag, query_id);
    
    // Liberar memoria
    free(filename_origen);
    free(source_tag);
    free(filename_destino);
    free(dest_tag);
    free(origen);
    free(destino);
    
    // Enviar respuesta
    if (result == 0) {
        logging_tag_creado(query_id, filename_origen, dest_tag);
        
        enviar_ok(socket_cliente);
    } else {
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
    }

    log_info(logger, "TAG_FILE - Procesamiento completamente finalizado");
}

// DELETE
int storage_delete_tag(storage_t* storage, const char* filename, const char* tag, uint32_t query_id) {
    pthread_mutex_lock(&storage->mutex);
    apply_operation_delay(storage);
    
    log_info(logger, "DELETE_TAG: Eliminando %s:%s", filename, tag);
    
    // 1. Verificar que el archivo existe
    char metadata_path[MAX_PATH_LENGTH];
    safe_path_join(
        metadata_path, sizeof(metadata_path),
        "%s/%s/%s/%s/%s",
        storage->root_path, FILES_DIR, filename, tag, METADATA_FILENAME
    );
    
    if (access(metadata_path, F_OK) != 0) {
        log_error(logger, "DELETE_TAG: Archivo %s:%s no existe", filename, tag);
        pthread_mutex_unlock(&storage->mutex);
        return -1;  // ← RETURN AGREGADO AQUÍ
    }
    
    // 2. No permitir eliminar initial_file/BASE (protección del sistema)
    if (strcmp(filename, "initial_file") == 0 && strcmp(tag, "BASE") == 0) {
        log_error(logger, "DELETE_TAG: No se puede eliminar initial_file/BASE");
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    // 3. Obtener bloques del archivo
    t_list* blocks = get_file_blocks(storage, filename, tag);
    if (!blocks) {
        log_error(logger, "DELETE_TAG: Error al obtener bloques del archivo %s:%s", filename, tag);
        pthread_mutex_unlock(&storage->mutex);
        return -1;
    }
    
    // 4. Liberar bloques físicos que no sean referenciados por otros archivos
    for (int i = 0; i < list_size(blocks); i++) {
        int physical_block = (int)(long)list_get(blocks, i);
        
        // Verificar si el bloque es referenciado por otros archivos
        bool referenced = false;
        
        if (!referenced && physical_block != 0) { // No liberar bloque 0 (sistema)
            free_physical_block(storage, physical_block, query_id);
            log_debug(logger, "DELETE_TAG: Bloque físico %d liberado", physical_block);
        }
        
        // Eliminar bloque lógico
        char* logical_path = get_logical_block_path(storage, filename, tag, i);
        if (logical_path) {

            logging_hard_link_eliminado(query_id, filename, tag, i, physical_block);

            if (unlink(logical_path) == -1) {
                log_warning(logger, "DELETE_TAG: Error al eliminar bloque lógico %s: %s", 
                           logical_path, strerror(errno));
            } else {
                log_debug(logger, "DELETE_TAG: Bloque lógico %s eliminado", logical_path);
            }
            free(logical_path);
        }
        
        // Aplicar delay por acceso a bloque
        apply_block_access_delay(storage, 1);
    }
    
    // 5. Limpiar lista de bloques
    list_destroy(blocks);
    
    // 6. Eliminar directorio del tag de forma segura
    char tag_dir_path[MAX_PATH_LENGTH];
    safe_path_join(
        tag_dir_path, sizeof(tag_dir_path),
        "%s/%s/%s/%s",
        storage->root_path, FILES_DIR, filename, tag
    );
    
    // Verificar que el directorio existe antes de eliminarlo
    if (access(tag_dir_path, F_OK) == 0) {
        // Eliminar recursivamente el directorio usando comando del sistema
        char command[MAX_PATH_LENGTH + 50];
        int written = snprintf(command, sizeof(command), "rm -rf \"%s\"", tag_dir_path);
        
        if (written < 0 || written >= (int)sizeof(command)) {
            log_error(logger, "DELETE_TAG: Comando demasiado largo para eliminar directorio");
            pthread_mutex_unlock(&storage->mutex);
            return -1;
        }
        
        log_debug(logger, "DELETE_TAG: Ejecutando comando: %s", command);
        int result = system(command);
        
        if (result != 0) {
            log_error(logger, "DELETE_TAG: Error al eliminar directorio %s (código: %d)", 
                     tag_dir_path, result);
            pthread_mutex_unlock(&storage->mutex);
            return -1;
        }
        
        log_debug(logger, "DELETE_TAG: Directorio %s eliminado exitosamente", tag_dir_path);
    } else {
        log_warning(logger, "DELETE_TAG: El directorio %s no existe", tag_dir_path);
    }
    
    // 7. Verificar que se eliminó correctamente
    if (access(metadata_path, F_OK) == 0) {
        log_warning(logger, "DELETE_TAG: El archivo metadata aún existe después de la eliminación: %s", 
                   metadata_path);
        // Intentar eliminar el metadata directamente
        if (unlink(metadata_path) == -1) {
            log_error(logger, "DELETE_TAG: No se pudo eliminar metadata: %s", strerror(errno));
        }
    }
    
    log_info(logger, "DELETE_TAG: %s:%s eliminado exitosamente", filename, tag);
    
    pthread_mutex_unlock(&storage->mutex);
    return 0;
}

void manejar_delete_file(int socket_cliente, uint32_t query_id) {
    log_info(logger, "Manejando DELETE_FILE");
    
    // Recibir file_tag completo (formato: filename:tag)
    char* file_tag = recibir_string_del_worker(socket_cliente);
    if (!file_tag) {
        log_error(logger, "Error al recibir file_tag en DELETE_FILE");
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
        return;
    }

    log_info(logger, "DELETE_FILE - file_tag recibido: %s", file_tag);
    
    // Parsear file_tag en filename y tag
    char* filename = NULL;
    char* tag = NULL;
    char* separador = strchr(file_tag, ':');
    
    if (separador) {
        *separador = '\0';
        filename = strdup(file_tag);
        tag = strdup(separador + 1);
        
        // Validar que el tag no esté vacío
        if (strlen(tag) == 0) {
            free(tag);
            tag = strdup("BASE");
        }
    } else {
        // Si no hay ':', usar el string completo como filename y tag por defecto
        filename = strdup(file_tag);
        tag = strdup("BASE");
    }
    
    log_info(logger, "DELETE_FILE parseado: filename='%s', tag='%s'", filename, tag);
    
    // Llamar a la función storage_delete_tag
    int result = storage_delete_tag(global_storage, filename, tag, query_id);
    
    // Liberar memoria
    free(filename);
    free(tag);
    free(file_tag);
    
    // Enviar respuesta
    if (result == 0) {
        // ✅ AGREGAR LOG OBLIGATORIO
        logging_tag_eliminado(query_id, filename, tag);
        
        enviar_ok(socket_cliente);
    } else {
        int error = htonl(OP_ERROR);
        send(socket_cliente, &error, sizeof(int), MSG_NOSIGNAL);
    }

    log_info(logger, "DELETE_FILE - Procesamiento completamente finalizado");
}


// MAIN FUNCTION

int main(int argc, char* argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <config_path>\n", argv[0]);
        fprintf(stderr, "Ejemplo: ./storage storage.config\n");
        fprintf(stderr, "Ejemplo: ./storage ../storage.config\n");
        return 1;
    }

    printf("Iniciando Storage...\n");
    printf("Archivo de configuración solicitado: %s\n", argv[1]);
    
    // Mostrar directorio actual
    char cwd[1024];
    if (getcwd(cwd, sizeof(cwd)) != NULL) {
        printf("Directorio actual: %s\n", cwd);
    }

    // Inicializar lista de workers para storage
    lista_workers_storage = list_create();
    pthread_mutex_init(&mutex_workers_storage, NULL);

    // Inicializar storage
    global_storage = inicializar_storage(argv[1]);
    if (!global_storage) {
        fprintf(stderr, "Fallo para inicializar storage\n");
        list_destroy(lista_workers_storage);
        pthread_mutex_destroy(&mutex_workers_storage);
        return 1;
    }

    printf("Storage INICIALIZADO\n");
    printf("Root path: %s\n", global_storage->root_path);
    
    // Iniciar servidor con mensaje específico
    log_info(logger, "=== Servidor Storage iniciado ===");
    log_info(logger, "Esperando conexiones de Workers...");
    printf("Server iniciado, esperando conexiones de Workers...\n");

    // Iniciar servidor
    iniciar_servidor_storage();

    // Mantener el proceso principal activo
    while (1) {
        sleep(1);
    }

    // Limpiar recursos
    list_destroy_and_destroy_elements(lista_workers_storage, free);
    pthread_mutex_destroy(&mutex_workers_storage);
    storage_destroy(global_storage);
    return 0;
}


//LOGS
//1. LOG CONEXIÓN WORKER
void logging_conexion_worker_storage(t_worker_storage* worker, int cantidad) {
    log_info(logger, "##Se conecta el Worker %d - Cantidad de Workers: %d",
             worker->worker_id, cantidad);
}

// 2. LOG DESCONEXIÓN WORKER
void logging_desconexion_worker_storage(int worker_id, int cantidad) {
    log_info(logger, "##Se desconecta el Worker %d - Cantidad de Workers: %d",
             worker_id, cantidad);
}

// 3. LOG FILE CREADO 
void logging_file_creado(uint32_t query_id, const char* filename, const char* tag) {
    log_info(logger, "##%u - File Creado %s:%s", query_id, filename, tag);
}

// 4. LOG FILE TRUNCADO
void logging_file_truncado(uint32_t query_id, const char* filename, const char* tag, size_t tamanio) {
    log_info(logger, "##%u - File Truncado %s:%s - Tamaño: %zu", 
             query_id, filename, tag, tamanio);
}

// 5. LOG TAG CREADO
void logging_tag_creado(uint32_t query_id, const char* filename, const char* tag) {
    log_info(logger, "##%u - Tag creado %s:%s", query_id, filename, tag);
}

// 6. LOG COMMIT TAG
void logging_commit_tag(uint32_t query_id, const char* filename, const char* tag) {
    log_info(logger, "##%u - Commit de File:Tag %s:%s", query_id, filename, tag);
}

// 7. LOG TAG ELIMINADO
void logging_tag_eliminado(uint32_t query_id, const char* filename, const char* tag) {
    log_info(logger, "##%u - Tag Eliminado %s:%s", query_id, filename, tag);
}

// 8. LOG BLOQUE LÓGICO LEÍDO
void logging_bloque_logico_leido(uint32_t query_id, const char* filename, const char* tag, size_t bloque) {
    log_info(logger, "##%u - Bloque Lógico Leído %s:%s - Número de Bloque: %zu",
             query_id, filename, tag, bloque);
}

// 9. LOG BLOQUE LÓGICO ESCRITO
void logging_bloque_logico_escrito(uint32_t query_id, const char* filename, const char* tag, size_t bloque) {
    log_info(logger, "##%u - Bloque Lógico Escrito %s:%s - Número de Bloque: %zu",
             query_id, filename, tag, bloque);
}

// 10. LOG BLOQUE FÍSICO RESERVADO
void logging_bloque_fisico_reservado(uint32_t query_id, int bloque) {
    log_info(logger, "##%u - Bloque Físico Reservado - Número de Bloque: %d",
             query_id, bloque);
}

// 11. LOG BLOQUE FÍSICO LIBERADO
void logging_bloque_fisico_liberado(uint32_t query_id, int bloque) {
    log_info(logger, "##%u - Bloque Físico Liberado - Número de Bloque: %d",
             query_id, bloque);
}

// 12. LOG HARD LINK AGREGADO
void logging_hard_link_agregado(uint32_t query_id, const char* filename, const char* tag, 
                                 size_t bloque_logico, int bloque_fisico) {
    log_info(logger, "##%u - %s:%s Se agregó el hard link del bloque lógico %zu al bloque físico %d",
             query_id, filename, tag, bloque_logico, bloque_fisico);
}

// 13. LOG HARD LINK ELIMINADO
void logging_hard_link_eliminado(uint32_t query_id, const char* filename, const char* tag,
                                  size_t bloque_logico, int bloque_fisico) {
    log_info(logger, "##%u - %s:%s Se eliminó el hard link del bloque lógico %zu al bloque físico %d",
             query_id, filename, tag, bloque_logico, bloque_fisico);
}

// 14. LOG DEDUPLICACIÓN
void logging_deduplicacion_bloque(uint32_t query_id, const char* filename, const char* tag,
                                   size_t bloque_logico, int bloque_actual, int bloque_nuevo) {
    log_info(logger, "##%u - %s:%s Bloque Lógico %zu se reasigna de %d a %d",
             query_id, filename, tag, bloque_logico, bloque_actual, bloque_nuevo);
}