#include "queryHelper.h"
#include "memoryHelper.h"

// CONSTANTES
#define FILES_DIR "files"
#define METADATA_FILENAME "metadata.config"

static bool ejecutar_CREATE(uint32_t id, char* file_tag);
static bool ejecutar_TRUNCATE(uint32_t id, char* file_tag, char* size);
static bool ejecutar_WRITE(uint32_t id, char* file_tag, char* direccion, char* contenido);
static bool ejecutar_READ(uint32_t id, char* file_tag, char* direccion, char* size);
static bool ejecutar_TAG(uint32_t id, char* origen, char* destino);
static bool ejecutar_COMMIT(uint32_t id, char* file_tag);
static bool ejecutar_FLUSH(uint32_t id, char* file_tag);
static bool ejecutar_DELETE(uint32_t id, char* file_tag);

static t_query_instruccion instruccion_from_string(const char* str);
extern bool recibir_respuesta_storage_simple(t_log* logger, int socket);
extern t_buffer* recibir_pagina_storage(t_log* logger, int socket);

uint32_t current_pc = 0;

// FUNCIONES AUXILIARES

static t_query_instruccion instruccion_from_string(const char* str) {
    if (strcmp(str, "CREATE") == 0) return QUERY_INST_CREATE;
    if (strcmp(str, "TRUNCATE") == 0) return QUERY_INST_TRUNCATE;
    if (strcmp(str, "WRITE") == 0) return QUERY_INST_WRITE;
    if (strcmp(str, "READ") == 0) return QUERY_INST_READ;
    if (strcmp(str, "TAG") == 0) return QUERY_INST_TAG;
    if (strcmp(str, "COMMIT") == 0) return QUERY_INST_COMMIT;
    if (strcmp(str, "FLUSH") == 0) return QUERY_INST_FLUSH;
    if (strcmp(str, "DELETE") == 0) return QUERY_INST_DELETE;
    if (strcmp(str, "END") == 0) return QUERY_INST_END;
    return QUERY_INST_INVALID;
}

// Función auxiliar para parsear file:tag y separar en filename y tag
static void parse_file_tag(const char* file_tag_str, char** filename, char** tag) {
    if (!file_tag_str) {
        *filename = strdup("unknown");
        *tag = strdup("BASE");
        return;
    }
    
    char* copia = strdup(file_tag_str);
    char* separador = strchr(copia, ':');
    
    if (separador) {
        *separador = '\0';
        *filename = strdup(copia);
        *tag = strdup(separador + 1);
        
        // Validar que el tag no esté vacío
        if (strlen(*tag) == 0) {
            free(*tag);
            *tag = strdup("BASE");
        }
    } else {
        // Si no hay ':', usar el string completo como filename y tag por defecto
        *filename = strdup(copia);
        *tag = strdup("BASE");
    }
    
    free(copia);
    
    log_debug(logger, "Parseado file:tag: '%s' -> filename='%s', tag='%s'", 
              file_tag_str, *filename, *tag);
}

// Función auxiliar para enviar string al storage
static void enviar_string_storage(const char* str, int socket) {
    uint32_t tam_str = htonl((uint32_t)(strlen(str) + 1));
    send(socket, &tam_str, sizeof(uint32_t), MSG_NOSIGNAL);
    send(socket, str, strlen(str) + 1, MSG_NOSIGNAL);
}

// Función auxiliar para enviar PC al Storage
static void enviar_pc_a_storage(uint32_t pc, int socket) {
    if (socket < 0) {
        log_warning(logger, "Socket de storage inválido, saltando envío de PC");
        return;
    }

    log_info(logger, "📤 Enviando PC al Storage: %u", pc);
    
    // Enviar código especial para PC
    int cod_op_pc = htonl(OP_PROGRAM_COUNTER);
    ssize_t bytes_sent = send(socket, &cod_op_pc, sizeof(int), MSG_NOSIGNAL);
    
    if (bytes_sent <= 0) {
        log_error(logger, "Error al enviar código OP_PROGRAM_COUNTER al Storage");
        return;
    }
    
    // Enviar el valor del PC
    uint32_t pc_network = htonl(pc);
    bytes_sent = send(socket, &pc_network, sizeof(uint32_t), MSG_NOSIGNAL);
    
    if (bytes_sent <= 0) {
        log_error(logger, "Error al enviar valor de PC al Storage");
        return;
    }
    
    // ESPERAR BREVE CONFIRMACIÓN
    usleep(10000); // 10ms para que el Storage procese
    
    log_info(logger, "PC enviado exitosamente al Storage: %u", pc);
}

// FUNCIONES DE EJECUCION
bool ejecutar_instruccion(uint32_t id, const char* line, uint32_t pc) {
    log_info(logger, "## Query %u: Ejecutando instrucción: %s", id, line);
    
    if (!line || strlen(line) == 0) return true;

    char* copia = strdup(line);
    char* token = strtok(copia, " ");
    if (!token) {
        free(copia);
        return true;
    }

    // LOG DETALLADO DE PARSING
    log_info(logger, "## Query %u: Token principal: '%s'", id, token);
    t_query_instruccion inst = instruccion_from_string(token);
    
    char* arg1 = strtok(NULL, " ");
    char* arg2 = strtok(NULL, " ");
    char* arg3 = strtok(NULL, " ");

    log_info(logger, "## Query %u: Args: '%s' '%s' '%s'", id, 
             arg1 ? arg1 : "NULL", 
             arg2 ? arg2 : "NULL", 
             arg3 ? arg3 : "NULL");
    
    bool resultado = true;
    bool es_operacion_critica = false;
    
    switch (inst) {
        case QUERY_INST_CREATE:
            if (arg1) {
                resultado = ejecutar_CREATE(id, arg1);
                es_operacion_critica = true; // CREATE es crítico
            } else {
                log_error(logger, "## Query %u: CREATE requiere parámetro file:tag", id);
                resultado = false;
                es_operacion_critica = true;
            }
            break;
            
        case QUERY_INST_TRUNCATE:
            if (arg1 && arg2) {
                resultado = ejecutar_TRUNCATE(id, arg1, arg2);
            } else {
                log_error(logger, "## Query %u: TRUNCATE requiere parámetros file:tag y size", id);
                resultado = false;
            }
            break;
            
        case QUERY_INST_WRITE:
            if (arg1 && arg2 && arg3) {
                resultado = ejecutar_WRITE(id, arg1, arg2, arg3);
            } else {
                log_error(logger, "## Query %u: WRITE requiere parámetros file:tag, dirección y contenido", id);
                resultado = false;
            }
            break;
            
        case QUERY_INST_READ:
            if (arg1 && arg2 && arg3) {
                log_info(logger, "## Query %u: EJECUTANDO READ %s %s %s", id, arg1, arg2, arg3);
                resultado = ejecutar_READ(id, arg1, arg2, arg3);
                // READ NO ES CRÍTICO - puede fallar sin detener query
                es_operacion_critica = false;
            } else {
                log_error(logger, "## Query %u: READ requiere 3 parámetros", id);
                resultado = false;
                es_operacion_critica = false; // No crítico
            }
            break;
            
        case QUERY_INST_TAG:
            if (arg1 && arg2) {
                resultado = ejecutar_TAG(id, arg1, arg2);
            } else {
                log_error(logger, "## Query %u: TAG requiere parámetros origen y destino", id);
                resultado = false;
            }
            break;
            
        case QUERY_INST_COMMIT:
            if (arg1) {
                resultado = ejecutar_COMMIT(id, arg1);
            } else {
                log_error(logger, "## Query %u: COMMIT requiere parámetro file:tag", id);
                resultado = false;
            }
            break;
            
        case QUERY_INST_FLUSH:
            if (arg1) {
                resultado = ejecutar_FLUSH(id, arg1);
            } else {
                log_error(logger, "## Query %u: FLUSH requiere parámetro file:tag", id);
                resultado = false;
            }
            break;
            
        case QUERY_INST_DELETE:
            if (arg1) {
                resultado = ejecutar_DELETE(id, arg1);
            } else {
                log_error(logger, "## Query %u: DELETE requiere parámetro file:tag", id);
                resultado = false;
            }
            break;
            
        case QUERY_INST_END:
            ejecutar_END(id);
            // END siempre es exitoso, no afecta el resultado de la query
            resultado = true;
            break;
            
        default:
            log_error(logger, "## Query %u: Instrucción desconocida: %s", id, line);
            resultado = false;
            break;
    }

    free(copia);
    
    // SOLO DETENER QUERY SI ES UNA OPERACIÓN CRÍTICA QUE FALLÓ
    if (!resultado && es_operacion_critica) {
        log_error(logger, "## Query %u: Error crítico en instrucción, abortando ejecución", id);
        return false;
    }
    
    // SI ES ERROR NO CRÍTICO, CONTINUAR
    if (!resultado) {
        log_warning(logger, "## Query %u: Operación no ejecutada, continuando con siguiente instrucción", id);
    }

    return true;
}

void enviar_mensaje_storage(op_code codigo, void* buffer, size_t size, int socket) {
    t_paquete* paquete = crear_paquete(codigo, logger);
    agregar_a_paquete(paquete, buffer, size);
    enviar_paquete(paquete, socket);
    eliminar_paquete(paquete);
}

// Función auxiliar para enviar resultado READ al Master
void enviar_resultado_read_a_master(uint32_t query_id, const char* file_tag, const char* datos, uint32_t size) {
    log_info(logger, "Enviando resultado READ al Master - Query %u | File: %s | Tamaño: %u bytes", 
             query_id, file_tag, size);

    if (socket_master == -1) {
        log_error(logger, "No hay conexión con el Master");
        return;
    }

    // PERMITIR DATOS NULL O SIZE 0 (para errores no críticos)
    if (datos == NULL || size == 0) {
        log_warning(logger, "Enviando resultado READ vacío para Query %u (error no crítico)", query_id);
        // Enviar resultado vacío en lugar de no enviar nada
        datos = "";
        size = 0;
    }
    
    // Crear paquete con RESULTADO_READ
    t_paquete* paquete = crear_paquete(RESULTADO_READ, logger);
    
    if (paquete == NULL) {
        log_error(logger, "Error al crear paquete para resultado READ");
        return;
    }
    
    // Agregar query_id (4 bytes)
    agregar_a_paquete(paquete, &query_id, sizeof(uint32_t));
    
    // AGREGAR FILE_TAG (asegurarse que no sea NULL)
    const char* safe_file_tag = file_tag ? file_tag : "unknown:BASE";
    agregar_a_paquete(paquete, (void*)safe_file_tag, strlen(safe_file_tag) + 1);
    
    // Agregar tamaño (4 bytes)
    agregar_a_paquete(paquete, &size, sizeof(uint32_t));
    
    // Agregar datos (size bytes)
    agregar_a_paquete(paquete, (void*)datos, size);
    
    // Enviar paquete al Master
    enviar_paquete(paquete, socket_master);
    
    log_info(logger, "Resultado READ enviado al Master - Query %u | File: %s | Tamaño: %u bytes", 
             query_id, safe_file_tag, size);
    
    eliminar_paquete(paquete);
}

// IMPLEMENTACION DE OPERACIONES

static bool ejecutar_CREATE(uint32_t id, char* file_tag) {
    log_info(logger, "## Query %u: Ejecutar CREATE %s", id, file_tag);

    // Parsear file_tag en filename y tag
    char* filename;
    char* tag;
    parse_file_tag(file_tag, &filename, &tag);
    
    log_info(logger, "CREATE parseado: filename='%s', tag='%s'", filename, tag);

    // ENVIAR PC ANTES DE LA OPERACIÓN
    enviar_pc_a_storage(current_pc, socket_storage);

    // Enviar código de operación
    int cod_op = htonl(OP_CREATE);
    if (send(socket_storage, &cod_op, sizeof(int), MSG_NOSIGNAL) <= 0) {
        log_error(logger, "Error al enviar OP_CREATE al Storage");
        free(filename);
        free(tag);
        return false;
    }

    log_info(logger, "Enviando CREATE para %s:%s al Storage", filename, tag);
    
    // Enviar filename
    enviar_string_storage(filename, socket_storage);
    
    // Enviar tag
    enviar_string_storage(tag, socket_storage);

    // Esperar respuesta
    bool resultado = recibir_respuesta_storage_simple(logger, socket_storage);
    
    // VERIFICACIÓN ESTRICTA
    if (resultado) {
        log_info(logger, "✅ CREATE %s:%s exitoso en Storage", filename, tag);
    } else {
        log_error(logger, "CREATE %s:%s falló en Storage", filename, tag);
        // DETENER LA QUERY INMEDIATAMENTE SI EL CREATE FALLA
        enviar_error_a_master(id, "CREATE falló - no se puede continuar la query");
    }
    
    free(filename);
    free(tag);
    return resultado;  // Si esto es false, la query debería detenerse
}

static bool ejecutar_TRUNCATE(uint32_t id, char* file_tag, char* size_str) {
    int32_t size = (uint32_t)atoi(size_str);
    log_info(logger, "## Query %u: Ejecutar TRUNCATE %s %u", id, file_tag, size);

    char* filename;
    char* tag;
    parse_file_tag(file_tag, &filename, &tag);
    
    log_info(logger, "TRUNCATE parseado: filename='%s', tag='%s', size=%u", filename, tag, size);

    enviar_pc_a_storage(current_pc, socket_storage);

    if (socket_storage < 0) {
        log_error(logger, "Socket de storage inválido");
        free(filename);
        free(tag);
        return false;  // CORREGIDO: retornar false en lugar de return sin valor
    }

    int cod_op = htonl(OP_TRUNCATE);
    send(socket_storage, &cod_op, sizeof(int), MSG_NOSIGNAL);

    log_info(logger, "Enviando TRUNCATE para %s:%s al Storage", filename, tag);
    
    enviar_string_storage(filename, socket_storage);
    enviar_string_storage(tag, socket_storage);
    
    uint32_t size_network = htonl(size);
    if (send(socket_storage, &size_network, sizeof(uint32_t), MSG_NOSIGNAL) != sizeof(uint32_t)) {
        log_error(logger, "Error enviando tamaño en TRUNCATE");
        free(filename);
        free(tag);
        return false;  // Retornar false en lugar de return sin valor
    }
    
    bool resultado = recibir_respuesta_storage_simple(logger, socket_storage);
    
    if (resultado) {
        log_info(logger, "TRUNCATE %s:%s exitoso", filename, tag);
    } else {
        log_error(logger, "Fallo TRUNCATE %s:%s", filename, tag);
        enviar_error_a_master(id, "TRUNCATE falló");
    }
    
    free(filename);
    free(tag);
    usleep(50000);
    return resultado;
}

static bool ejecutar_WRITE(uint32_t id, char* file_tag, char* direccion_str, char* contenido) {
    uint32_t offset = (uint32_t)atoi(direccion_str);
    uint32_t size = strlen(contenido);

    log_info(logger, "## Query %u: Ejecutar WRITE %s offset=%u size=%u contenido='%s'", 
             id, file_tag, offset, size, contenido);

    // Parsear filename:tag
    char* filename;
    char* tag;
    parse_file_tag(file_tag, &filename, &tag);

    // ENVIAR PC
    enviar_pc_a_storage(current_pc, socket_storage);

    // Enviar código de operación WRITE
    int cod_op_write = htonl(OP_WRITE);
    if (send(socket_storage, &cod_op_write, sizeof(int), MSG_NOSIGNAL) <= 0) {
        log_error(logger, "Error al enviar OP_WRITE");
        free(filename); free(tag);
        return false; // Error crítico
    }

    // Enviar filename
    enviar_string_storage(filename, socket_storage);

    // Enviar tag
    enviar_string_storage(tag, socket_storage);

    // Enviar offset (uint32_t)
    uint32_t offset_net = htonl(offset);
    if (send(socket_storage, &offset_net, sizeof(uint32_t), MSG_NOSIGNAL) <= 0) {
        log_error(logger, "Error al enviar offset");
        free(filename); free(tag);
        return false; // Error crítico
    }

    // Enviar tamaño real del contenido
    uint32_t size_net = htonl(size);
    if (send(socket_storage, &size_net, sizeof(uint32_t), MSG_NOSIGNAL) <= 0) {
        log_error(logger, "Error al enviar size");
        free(filename); free(tag);
        return false; // Error crítico
    }

    // Enviar los datos (solo size bytes, no un bloque completo)
    if (send(socket_storage, contenido, size, MSG_NOSIGNAL) != (ssize_t)size) {
        log_error(logger, "Error al enviar datos de WRITE");
        free(filename); free(tag);
        return false; // Error crítico
    }

    // Esperar respuesta
    bool resultado = recibir_respuesta_storage_simple(logger, socket_storage);

    if (!resultado) {
        // DIFERENCIAR TIPOS DE ERROR
        log_warning(logger, "WRITE no ejecutado en %s:%s (posiblemente COMMITTED)", filename, tag);
        // NO ENVIAR ERROR AL MASTER - CONTINUAR EJECUCIÓN
        // NO llamar a enviar_error_a_master aquí
    } else {
        log_info(logger, "WRITE Storage %s:%s exitoso", filename, tag);
    }

    free(filename);
    free(tag);
    
    // SIEMPRE RETORNAR TRUE PARA CONTINUAR LA QUERY
    // Incluso si el WRITE falló, continuamos con las siguientes instrucciones
    return true;
}

static bool ejecutar_READ(uint32_t id, char* file_tag, char* direccion_str, char* size_str) {
    uint32_t direccion = (uint32_t)atoi(direccion_str);
    uint32_t size_solicitado = (uint32_t)atoi(size_str);

    log_info(logger, "## Query %u: Ejecutar READ %s dir=%u size=%u (BLOCK_SIZE=%u)", 
             id, file_tag, direccion, size_solicitado, WORKER_BLOCK_SIZE);

    if (WORKER_BLOCK_SIZE == 0) {
        log_error(logger, "ERROR CRÍTICO: WORKER_BLOCK_SIZE es 0");
        enviar_error_a_master(id, "Error: tamaño de bloque no inicializado");
        return false; // Error crítico
    }

    enviar_pc_a_storage(current_pc, socket_storage);
    usleep(50000);

    // CALCULAR BLOQUES CON BLOCK_SIZE=16
    uint32_t bloque_inicial = direccion / WORKER_BLOCK_SIZE;
    uint32_t offset_inicial = direccion % WORKER_BLOCK_SIZE;
    uint32_t direccion_final = direccion + size_solicitado - 1;
    uint32_t bloque_final = direccion_final / WORKER_BLOCK_SIZE;
    uint32_t total_bloques = bloque_final - bloque_inicial + 1;
    
    log_info(logger, "READ: dir=%u, size=%u -> bloques %u-%u (%u bloques), offset_inicial=%u", 
             direccion, size_solicitado, bloque_inicial, bloque_final, total_bloques, offset_inicial);

    char* buffer_completo = malloc(size_solicitado + 1);
    if (!buffer_completo) {
        log_error(logger, "Error al allocar buffer para READ");
        // NO ES CRÍTICO - CONTINUAR QUERY
        log_warning(logger, "## Query %u: READ falló por falta de memoria, continuando query", id);
        return true;
    }
    memset(buffer_completo, 0, size_solicitado + 1);

    uint32_t total_bytes_leidos = 0;
    uint32_t bytes_restantes = size_solicitado;

    bool lectura_exitosa = true;

    // LEER CADA BLOQUE REQUERIDO
    for (uint32_t bloque_actual = bloque_inicial; 
         bloque_actual <= bloque_final && bytes_restantes > 0; 
         bloque_actual++) {
        
        uint32_t offset_en_bloque = (bloque_actual == bloque_inicial) ? offset_inicial : 0;
        uint32_t bytes_posibles_en_bloque = WORKER_BLOCK_SIZE - offset_en_bloque;
        uint32_t bytes_a_leer_de_bloque = (bytes_restantes < bytes_posibles_en_bloque) ? 
                                         bytes_restantes : bytes_posibles_en_bloque;

        log_info(logger, "Leyendo bloque %u: offset=%u, bytes_a_leer=%u, bytes_restantes=%u", 
                 bloque_actual, offset_en_bloque, bytes_a_leer_de_bloque, bytes_restantes);

        // Solicitar bloque al Storage
        int cod_op = OP_READ;
        int cod_op_network = htonl(cod_op);
        
        if (send(socket_storage, &cod_op_network, sizeof(int), MSG_NOSIGNAL) <= 0) {
            log_error(logger, "Error al enviar OP_READ al Storage");
            // NO ES CRÍTICO - CONTINUAR
            log_warning(logger, "## Query %u: Error de comunicación en READ, continuando query", id);
            lectura_exitosa = false;
            break;
        }

        // Enviar file_tag
        uint32_t tam_file_tag = strlen(file_tag) + 1;
        uint32_t tam_file_tag_network = htonl(tam_file_tag);
        
        if (send(socket_storage, &tam_file_tag_network, sizeof(uint32_t), MSG_NOSIGNAL) <= 0 ||
            send(socket_storage, file_tag, tam_file_tag, MSG_NOSIGNAL) <= 0) {
            log_error(logger, "Error al enviar file_tag al Storage");
            // NO ES CRÍTICO - CONTINUAR
            lectura_exitosa = false;
            break;
        }

        // Enviar número de bloque
        uint32_t bloque_network = htonl(bloque_actual);
        if (send(socket_storage, &bloque_network, sizeof(uint32_t), MSG_NOSIGNAL) <= 0) {
            log_error(logger, "Error al enviar número de bloque al Storage");
            // NO ES CRÍTICO - CONTINUAR
            lectura_exitosa = false;
            break;
        }

        log_info(logger, "Solicitado bloque %u para %s", bloque_actual, file_tag);

        // Recibir bloque del Storage
        t_buffer* bloque_buffer = recibir_pagina_storage(logger, socket_storage);
        
        if (!bloque_buffer) {
            log_error(logger, "Error al recibir bloque %u del Storage", bloque_actual);
            // NO ES CRÍTICO - CONTINUAR QUERY
            log_warning(logger, "## Query %u: Bloque %u no disponible, continuando query", id, bloque_actual);
            lectura_exitosa = false;
            break;
        }

        log_info(logger, "Bloque %u recibido - %u bytes", bloque_actual, bloque_buffer->size);

        // VERIFICAR SI EL STORAGE ENVIÓ UN ERROR
        if (bloque_buffer->size == 0 || bloque_buffer->stream == NULL) {
            log_warning(logger, "## Query %u: Bloque %u vacío o error del Storage", id, bloque_actual);
            free(bloque_buffer->stream);
            free(bloque_buffer);
            lectura_exitosa = false;
            break;
        }

        // Validar offset dentro del bloque
        if (offset_en_bloque >= bloque_buffer->size) {
            log_error(logger, "ERROR: offset=%u >= tamaño_bloque=%u", 
                     offset_en_bloque, bloque_buffer->size);
            free(bloque_buffer->stream);
            free(bloque_buffer);
            // NO ES CRÍTICO - CONTINUAR QUERY
            log_warning(logger, "## Query %u: Offset fuera de rango en READ, continuando query", id);
            lectura_exitosa = false;
            break;
        }

        // Calcular bytes disponibles desde el offset
        uint32_t bytes_disponibles = bloque_buffer->size - offset_en_bloque;
        uint32_t bytes_a_copiar = (bytes_disponibles < bytes_a_leer_de_bloque) ? 
                                 bytes_disponibles : bytes_a_leer_de_bloque;

        if (bytes_a_copiar == 0) {
            log_warning(logger, "No hay bytes para copiar del bloque %u", bloque_actual);
            free(bloque_buffer->stream);
            free(bloque_buffer);
            continue;
        }

        // Copiar datos al buffer completo
        memcpy(buffer_completo + total_bytes_leidos, 
               (char*)bloque_buffer->stream + offset_en_bloque, 
               bytes_a_copiar);
        
        total_bytes_leidos += bytes_a_copiar;
        bytes_restantes -= bytes_a_copiar;
        
        log_info(logger, "Copiados %u bytes desde bloque %u -> total_leidos=%u", 
                 bytes_a_copiar, bloque_actual, total_bytes_leidos);

        free(bloque_buffer->stream);
        free(bloque_buffer);
    }

    // MANEJAR RESULTADO DE LA LECTURA
    if (lectura_exitosa && total_bytes_leidos > 0) {
        if (total_bytes_leidos < size_solicitado) {
            log_warning(logger, "READ incompleto: solicitados=%u, leídos=%u", 
                       size_solicitado, total_bytes_leidos);
        } else {
            log_info(logger, "READ completado exitosamente: %u bytes leídos", total_bytes_leidos);
        }

        buffer_completo[total_bytes_leidos] = '\0';
        
        // Log de contenido leído
        log_info(logger, "Datos leídos (primeros %u bytes): '%s'", 
                 (total_bytes_leidos < 64 ? total_bytes_leidos : 64), buffer_completo);
        
        // Enviar resultado al Master
        enviar_resultado_read_a_master(id, file_tag, buffer_completo, total_bytes_leidos);
        
    } else {
        log_warning(logger, "## Query %u: READ no pudo completarse para %s", id, file_tag);
        
        // ENVIAR RESULTADO VACÍO AL MASTER EN LUGAR DE ERROR
        char* resultado_vacio = malloc(1);
        resultado_vacio[0] = '\0';
        enviar_resultado_read_a_master(id, file_tag, resultado_vacio, 0);
        free(resultado_vacio);
    }
    
    free(buffer_completo);
    
    // SIEMPRE RETORNAR TRUE PARA CONTINUAR LA QUERY
    // Incluso si READ falló, continuamos con las siguientes instrucciones
    return true;
}

static bool ejecutar_FLUSH(uint32_t id, char* file_tag) {
    log_info(logger, "## Query %u: Ejecutar FLUSH %s", id, file_tag);

    // Parsear file_tag en filename y tag
    char* filename;
    char* tag;
    parse_file_tag(file_tag, &filename, &tag);
    
    log_info(logger, "FLUSH parseado: filename='%s', tag='%s'", filename, tag);

    // ENVIAR PC
    enviar_pc_a_storage(current_pc, socket_storage);

    // Enviar código de operación
    int cod_op = htonl(OP_FLUSH);
    send(socket_storage, &cod_op, sizeof(int), MSG_NOSIGNAL);

    log_info(logger, "Enviando FLUSH para %s:%s al Storage", filename, tag);
    
    // Enviar filename
    enviar_string_storage(filename, socket_storage);
    
    // Enviar tag
    enviar_string_storage(tag, socket_storage);

    // Esperar un momento para que el Storage procese
    usleep(100000); // 100ms
    
    bool resultado = recibir_respuesta_storage_simple(logger, socket_storage);
    
    if (resultado) {
        log_info(logger, "FLUSH %s:%s exitoso", filename, tag);
    } else {
        log_error(logger, "Fallo FLUSH %s:%s", filename, tag);
    }
    
    free(filename);
    free(tag);
    return resultado;
}

static bool ejecutar_COMMIT(uint32_t id, char* file_tag) {
    log_info(logger, "## Query %u: Ejecutar COMMIT %s", id, file_tag);

    // Parsear file_tag en filename y tag
    char* filename;
    char* tag;
    parse_file_tag(file_tag, &filename, &tag);
    
    log_info(logger, "COMMIT parseado: filename='%s', tag='%s'", filename, tag);

    // ENVIAR PC
    enviar_pc_a_storage(current_pc, socket_storage);

    // Enviar código de operación
    int cod_op = htonl(OP_COMMIT);
    send(socket_storage, &cod_op, sizeof(int), MSG_NOSIGNAL);

    log_info(logger, "Enviando COMMIT para %s:%s al Storage", filename, tag);
    
    // Enviar filename
    enviar_string_storage(filename, socket_storage);
    
    // Enviar tag
    enviar_string_storage(tag, socket_storage);

    // Esperar un momento para que el Storage procese
    usleep(100000); // 100ms
    
    bool resultado = recibir_respuesta_storage_simple(logger, socket_storage);
    
    if (resultado) {
        log_info(logger, "COMMIT %s:%s exitoso", filename, tag);
    } else {
        log_error(logger, "Fallo COMMIT %s:%s", filename, tag);
    }
    
    free(filename);
    free(tag);
    return resultado;
}

static bool ejecutar_TAG(uint32_t id, char* origen, char* destino) {
    log_info(logger, "## Query %u: Ejecutar TAG %s -> %s", id, origen, destino);

    // DECLARAR VARIABLES AL INICIO
    char* filename_origen = NULL;
    char* source_tag = NULL;
    char* filename_destino = NULL;
    char* dest_tag = NULL;
    char* origen_completo = NULL;
    char* destino_completo = NULL;
    bool resultado = false;

    // Parsear origen
    char* separador = strchr(origen, ':');
    if (separador) {
        *separador = '\0';
        filename_origen = strdup(origen);
        source_tag = strdup(separador + 1);
        *separador = ':'; // Restaurar para construir origen_completo
    } else {
        filename_origen = strdup(origen);
        source_tag = strdup("BASE");
    }
    
    // Parsear destino
    separador = strchr(destino, ':');
    if (separador) {
        *separador = '\0';
        filename_destino = strdup(destino);
        dest_tag = strdup(separador + 1);
        *separador = ':'; // Restaurar para construir destino_completo
    } else {
        filename_destino = strdup(filename_origen);
        dest_tag = strdup(destino);
    }
    
    // Verificar que coincidan los filenames
    if (strcmp(filename_origen, filename_destino) != 0) {
        log_error(logger, "TAG: No se permite copia entre archivos diferentes");
        goto cleanup;
    }
    
    // Construir strings completos formato filename:tag para enviar al Storage
    origen_completo = malloc(strlen(filename_origen) + strlen(source_tag) + 2);
    sprintf(origen_completo, "%s:%s", filename_origen, source_tag);
    
    destino_completo = malloc(strlen(filename_destino) + strlen(dest_tag) + 2);
    sprintf(destino_completo, "%s:%s", filename_destino, dest_tag);
    
    log_info(logger, "TAG parseado: %s -> %s", origen_completo, destino_completo);

    enviar_pc_a_storage(current_pc, socket_storage);

    int cod_op = htonl(OP_TAG);
    if (send(socket_storage, &cod_op, sizeof(int), MSG_NOSIGNAL) <= 0) {
        log_error(logger, "TAG: Error al enviar código de operación");
        goto cleanup;
    }
    
    // Enviar origen completo (filename:tag)
    uint32_t tam_origen = strlen(origen_completo) + 1;
    uint32_t tam_origen_network = htonl(tam_origen);
    if (send(socket_storage, &tam_origen_network, sizeof(uint32_t), MSG_NOSIGNAL) <= 0 ||
        send(socket_storage, origen_completo, tam_origen, MSG_NOSIGNAL) <= 0) {
        log_error(logger, "TAG: Error al enviar origen");
        goto cleanup;
    }
    
    // Enviar destino completo (filename:tag)
    uint32_t tam_destino = strlen(destino_completo) + 1;
    uint32_t tam_destino_network = htonl(tam_destino);
    if (send(socket_storage, &tam_destino_network, sizeof(uint32_t), MSG_NOSIGNAL) <= 0 ||
        send(socket_storage, destino_completo, tam_destino, MSG_NOSIGNAL) <= 0) {
        log_error(logger, "TAG: Error al enviar destino");
        goto cleanup;
    }

    resultado = recibir_respuesta_storage_simple(logger, socket_storage);
    
    if (resultado) {
        log_info(logger, "TAG %s -> %s exitoso", origen_completo, destino_completo);
    } else {
        log_error(logger, "Fallo TAG %s -> %s", origen_completo, destino_completo);
        enviar_error_a_master(id, "TAG falló");
    }

cleanup:
    // Limpieza segura
    if (filename_origen) free(filename_origen);
    if (source_tag) free(source_tag);
    if (filename_destino) free(filename_destino);
    if (dest_tag) free(dest_tag);
    if (origen_completo) free(origen_completo);
    if (destino_completo) free(destino_completo);
    
    return resultado;
}

static bool ejecutar_DELETE(uint32_t id, char* file_tag) {
    log_info(logger, "## Query %u: Ejecutar DELETE %s", id, file_tag);

    // DECLARAR VARIABLES
    char* filename = NULL;
    char* tag = NULL;
    bool resultado = false;

    parse_file_tag(file_tag, &filename, &tag);
    
    log_info(logger, "DELETE parseado: filename='%s', tag='%s'", filename, tag);

    enviar_pc_a_storage(current_pc, socket_storage);

    int cod_op = htonl(OP_DELETE);
    send(socket_storage, &cod_op, sizeof(int), MSG_NOSIGNAL);

    log_info(logger, "Enviando DELETE para %s:%s al Storage", filename, tag);
    
    enviar_string_storage(file_tag, socket_storage);

    resultado = recibir_respuesta_storage_simple(logger, socket_storage);
    
    if (resultado) {
        log_info(logger, "DELETE %s:%s exitoso", filename, tag);
        memory_liberar_archivo(file_tag);
    } else {
        log_error(logger, "Fallo DELETE %s:%s", filename, tag);
        enviar_error_a_master(id, "DELETE falló");
    }
    
    // Limpieza
    free(filename);
    free(tag);
    
    return resultado;
}

// En queryHelper.c - Simplificar ejecutar_END

bool ejecutar_END(uint32_t id) {
    log_info(logger, "## Query %u: Ejecutar END (finalizar query)", id);

    // ENVIAR PC FINAL AL STORAGE ANTES DE FINALIZAR
    if (socket_storage >= 0) {
        log_info(logger, "Enviando PC final al Storage: %u", current_pc);
        enviar_pc_a_storage(current_pc, socket_storage);
    }

    // 1. Enviar END al Master (201)
    op_code cod_op = END;
    ssize_t bytes_sent = send(socket_master, &cod_op, sizeof(op_code), MSG_NOSIGNAL);
    
    if (bytes_sent <= 0) {
        log_error(logger, "❌ Error al enviar END al Master para query %u", id);
        return false;
    }
    
    log_info(logger, "END (201) enviado al Master para query %u (PC: %u)", id, current_pc);

    // 2. Informar al Storage con OP_END (209) si está conectado
    if (socket_storage != -1) {
        int cod_op_storage = htonl(OP_END);
        bytes_sent = send(socket_storage, &cod_op_storage, sizeof(int), MSG_NOSIGNAL);
        
        if (bytes_sent <= 0) {
            log_warning(logger, "Error al enviar OP_END al Storage");
        } else {
            // Enviar ID del worker al Storage
            uint32_t tam_id = strlen(WORKER_ID) + 1;
            uint32_t tam_id_network = htonl(tam_id);
            send(socket_storage, &tam_id_network, sizeof(uint32_t), MSG_NOSIGNAL);
            send(socket_storage, WORKER_ID, tam_id, MSG_NOSIGNAL);
            
            log_info(logger, "OP_END (209) enviado al Storage para worker %s", WORKER_ID);
        }
    }

    log_info(logger, "Query %u finalizada correctamente con END (PC final: %u)", id, current_pc);
    
    // NO RESETEAR current_pc aquí - se hace en ejecutar_query
    
    return true;
}