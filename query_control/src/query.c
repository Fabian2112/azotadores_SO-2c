#include "query.h"
#include <cliente.h>
#include <server.h>

t_log* logger = NULL;
t_config_query* config_global = NULL;
t_query_info* query_info_global = NULL;
pthread_t hilo_escucha;
bool sistema_activo = true;
int contador_queries = 0;

void enviar_nueva_query(char* archivo, int prioridad) {
    log_info(logger, "## Solicitud de ejecución de Query: %s, prioridad: %d",
             archivo, prioridad);

    if (query_info_global->socket_master <= 0) {
        log_error(logger, "Socket inválido: no hay conexión con el Master");
        return;
    }

    // ✅ Enviar directamente el tamaño del path
    uint32_t tam_path = strlen(archivo) + 1;
    uint32_t tam_path_network = htonl(tam_path);
    ssize_t bytes_sent = send(query_info_global->socket_master, &tam_path_network, sizeof(uint32_t), MSG_NOSIGNAL);
    if (bytes_sent <= 0) {
        log_error(logger, "Error al enviar tamaño del path");
        return;
    }
    log_info(logger, "Tamaño del path enviado: %u bytes", tam_path);

    // ✅ Enviar el path
    bytes_sent = send(query_info_global->socket_master, archivo, tam_path, MSG_NOSIGNAL);
    if (bytes_sent <= 0) {
        log_error(logger, "Error al enviar path");
        return;
    }
    log_info(logger, "Path enviado: '%s'", archivo);

    // ✅ Enviar la prioridad
    int prioridad_network = htonl(prioridad);
    bytes_sent = send(query_info_global->socket_master, &prioridad_network, sizeof(int), MSG_NOSIGNAL);
    if (bytes_sent <= 0) {
        log_error(logger, "Error al enviar prioridad");
        return;
    }
    log_info(logger, "Prioridad enviada: %d", prioridad);

    // INCREMENTAR CONTADOR DE QUERIES ENVIADAS
    contador_queries++;
    
    log_info(logger, "## Query enviada correctamente al Master");
    
    // MOSTRAR MENSAJE CON CONTADOR
    printf("\n");
    printf("╔════════════════════════════════════════════════╗\n");
    printf("║          QUERY ENVIADA EXITOSAMENTE            ║\n");
    printf("╠════════════════════════════════════════════════╣\n");
    printf("║ Archivo:   %-35s ║\n", archivo);
    printf("║ Prioridad: %-35d ║\n", prioridad);
    printf("║                                               ║\n");
    printf("║ Total de queries enviadas: %-19d ║\n", contador_queries);
    printf("╚════════════════════════════════════════════════╝\n");
    printf("\n");
}

void* escuchar_respuestas_master(void* args) {
    t_query_info* query_info = (t_query_info*)args;
    
    log_info(logger, "Hilo de escucha iniciado - Esperando respuestas del Master...");
    
    while (sistema_activo) {
        // Recibir código de operación
        op_code cod_op;
        ssize_t bytes_recibidos = recv(query_info->socket_master, &cod_op, 
                                      sizeof(op_code), MSG_WAITALL);
        
        if (bytes_recibidos <= 0) {
            if (bytes_recibidos == 0) {
                log_warning(logger, "Master cerró la conexión");
                printf("\n⚠ Master cerró la conexión\n\n");
            } else {
                log_error(logger, "Error al recibir código de operación: %s", 
                         strerror(errno));
            }
            sistema_activo = false;
            break;
        }
        
        log_debug(logger, "Código de operación recibido: %d", cod_op);
        
        switch (cod_op) {
            case RESULTADO_READ: {
                // Recibir tamaño del mensaje
                int32_t size;
                recv(query_info->socket_master, &size, sizeof(int32_t), MSG_WAITALL);
                
                if (size > 0) {
                    // Recibir datos leídos
                    char* datos = malloc(size + 1);
                    recv(query_info->socket_master, datos, size, MSG_WAITALL);
                    datos[size] = '\0';
                    
                    printf("\n");
                    printf("╔════════════════════════════════════════════════╗\n");
                    printf("║           RESULTADO READ RECIBIDO              ║\n");
                    printf("╠════════════════════════════════════════════════╣\n");
                    printf("║ Tamaño: %-38zu ║\n", strlen(datos));
                    printf("╠════════════════════════════════════════════════╣\n");
                    printf("║ Datos:                                         ║\n");
                    printf("║ '%s'%-*s║\n", datos, (int)(46 - strlen(datos)), "");
                    printf("╚════════════════════════════════════════════════╝\n");
                    printf("\nquery> ");
                    fflush(stdout);
                    
                    log_info(logger, "## Resultado READ recibido: '%s' (%zu bytes)", 
                            datos, strlen(datos));
                    
                    free(datos);
                }
                break;
            }
            
            case QUERY_FINALIZADA: {
                // Recibir tamaño del mensaje
                uint32_t size;
                recv(query_info->socket_master, &size, sizeof(uint32_t), MSG_WAITALL);
                
                // Recibir motivo de finalización
                char* motivo = malloc(size + 1);
                recv(query_info->socket_master, motivo, size, MSG_WAITALL);
                motivo[size] = '\0';
                
                printf("\n");
                printf("╔════════════════════════════════════════════════╗\n");
                printf("║            QUERY FINALIZADA                    ║\n");
                printf("╠════════════════════════════════════════════════╣\n");
                printf("║ Estado: %-38s ║\n", motivo);
                printf("╚════════════════════════════════════════════════╝\n");
                printf("\nquery> ");
                fflush(stdout);
                
                log_info(logger, "## Query Finalizada - %s", motivo);
                
                free(motivo);
                break;
            }
            
            case ERROR_EJECUCION: {
                // Recibir tamaño del mensaje
                uint32_t size;
                recv(query_info->socket_master, &size, sizeof(uint32_t), MSG_WAITALL);
                
                // Recibir mensaje de error
                char* error = malloc(size + 1);
                recv(query_info->socket_master, error, size, MSG_WAITALL);
                error[size] = '\0';
                
                printf("\n");
                printf("╔════════════════════════════════════════════════╗\n");
                printf("║              ERROR EN QUERY                    ║\n");
                printf("╠════════════════════════════════════════════════╣\n");
                printf("║ %s%-*s║\n", error, (int)(46 - strlen(error)), "");
                printf("╚════════════════════════════════════════════════╝\n");
                printf("\nquery> ");
                fflush(stdout);
                
                log_error(logger, "Error en ejecución de Query: %s", error);
                
                free(error);
                break;
            }
            
            case MENSAJE_LECTURA: {
                // Recibir el paquete completo
                void* buffer = NULL;
                int cod_interno = recibir_paquete_completo(logger, query_info->socket_master, &buffer);
                
                if (cod_interno > 0 && buffer != NULL) {
                    printf("\n");
                    printf("┌────────────────────────────────────────────────┐\n");
                    printf("│       NOTIFICACIÓN: Lectura en progreso        │\n");
                    printf("└────────────────────────────────────────────────┘\n");
                    printf("query> ");
                    fflush(stdout);
                    
                    log_info(logger, "## Notificación de lectura recibida");
                    
                    if (buffer != NULL) {
                        free(buffer);
                    }
                }
                break;
            }
            
            default:
                log_warning(logger, "Código de operación no reconocido: %d", cod_op);
                printf("\n⚠ Mensaje desconocido del Master (código: %d)\n", cod_op);
                printf("query> ");
                fflush(stdout);
                break;
        }
    }
    
    log_info(logger, "Hilo de escucha finalizado");
    return NULL;
}

void procesar_comando_query(char* comando) {
    // Dividir el comando en archivo y prioridad
    char* token = strtok(comando, " ");
    if (token == NULL) {
        printf("Error: Formato incorrecto. Use: QUERY <archivo> <prioridad>\n");
        return;
    }
    
    char* archivo = token;
    token = strtok(NULL, " ");
    
    if (token == NULL) {
        printf("Error: Falta la prioridad. Use: QUERY <archivo> <prioridad>\n");
        return;
    }
    
    int prioridad = atoi(token);
    
    if (prioridad < 0) {
        printf("Error: La prioridad debe ser un número positivo\n");
        return;
    }
    
    // Construir rutas dinámicamente usando el nombre del archivo proporcionado
    char ruta_1[512], ruta_2[512], ruta_3[512], ruta_4[512], ruta_5[512];
    
    snprintf(ruta_1, sizeof(ruta_1), "../utils/pruebas/%s", archivo);
    snprintf(ruta_2, sizeof(ruta_2), "../../utils/pruebas/%s", archivo);
    snprintf(ruta_3, sizeof(ruta_3), "../../../utils/pruebas/%s", archivo);
    snprintf(ruta_4, sizeof(ruta_4), "utils/pruebas/%s", archivo);
    snprintf(ruta_5, sizeof(ruta_5), "../%s", archivo);
    
    char* ubicaciones[] = {
        archivo,                // Ruta original (puede ser ruta completa)
        ruta_1,                 // Desde bin/ hacia utils/pruebas/
        ruta_2,                 // Desde query_control/ hacia utils/pruebas/
        ruta_3,                 // Desde tp-2025-2c-azotadores/
        ruta_4,                 // Ruta relativa desde raíz
        ruta_5,                 // Un nivel arriba desde bin/
        NULL
    };
    
    FILE* archivo_encontrado = NULL;
    char* ruta_final = NULL;
    
    for (int i = 0; ubicaciones[i] != NULL; i++) {
        archivo_encontrado = fopen(ubicaciones[i], "r");
        if (archivo_encontrado != NULL) {
            ruta_final = ubicaciones[i];
            printf("Archivo encontrado en: %s\n", ruta_final);
            fclose(archivo_encontrado);
            break;
        }
    }
    
    if (ruta_final == NULL) {
        printf("Error: No se puede abrir el archivo '%s'\n", archivo);
        printf("Se buscó en las siguientes ubicaciones:\n");
        for (int i = 0; ubicaciones[i] != NULL; i++) {
            printf("  - %s\n", ubicaciones[i]);
        }
        return;
    }
    
    // Enviar la query con la ruta encontrada
    enviar_nueva_query(ruta_final, prioridad);
}

void mostrar_ayuda(void) {
    printf("\n");
    printf("╔════════════════════════════════════════════════╗\n");
    printf("║               COMANDOS DISPONIBLES             ║\n");
    printf("╠════════════════════════════════════════════════╣\n");
    printf("║ QUERY <archivo> <prioridad>                    ║\n");
    printf("║   Envía una nueva query al Master              ║\n");
    printf("║   Ejemplo: QUERY query_ejemplo.txt 2           ║\n");
    printf("║                                                ║\n");
    printf("║ stats                                          ║\n");
    printf("║   Muestra estadísticas de queries enviadas     ║\n");
    printf("║                                                ║\n");
    printf("║ help                                           ║\n");
    printf("║   Muestra esta ayuda                           ║\n");
    printf("║                                                ║\n");
    printf("║ exit | quit                                    ║\n");
    printf("║   Sale del programa                            ║\n");
    printf("╚════════════════════════════════════════════════╝\n");
    printf("\n");
}

int main(int argc, char* argv[]) {
    saludar("query_control");
    
    // Validar argumentos mínimos
    if (argc < 2) {
        fprintf(stderr, "Uso: %s [archivo_config] [archivo_query] [prioridad]\n", argv[0]);
        fprintf(stderr, "Uso interactivo: %s [archivo_config]\n", argv[0]);
        fprintf(stderr, "Ejemplo: ./bin/query query.config query_ejemplo 5\n");
        fprintf(stderr, "Ejemplo interactivo: ./bin/query ../query.config\n");
        return EXIT_FAILURE;
    }
    
    // Mostrar directorio actual para debug
    char cwd[1024];
    if (getcwd(cwd, sizeof(cwd)) != NULL) {
        printf("Directorio actual: %s\n", cwd);
    }

    // Cargar configuración
    config_global = cargar_configuracion(argv[1]);
    if (config_global == NULL) {
        fprintf(stderr, "Error al cargar la configuración\n");
        return EXIT_FAILURE;
    }
    
    // Inicializar logger
    logger = log_create("query.log", "Query_Control", true, 
                        log_level_from_string(config_global->log_level));
    
    if (logger == NULL) {
        fprintf(stderr, "Error al crear el logger\n");
        destruir_config_query(config_global);
        return EXIT_FAILURE;
    }
    
    log_info(logger, "=== Iniciando Query Control ===");
    
    // Crear estructura de información de query
    query_info_global = malloc(sizeof(t_query_info));
    query_info_global->socket_master = -1;
    
    // Conectar con Master
    conectar_con_master(query_info_global, config_global);
    
    if (query_info_global->socket_master == -1) {
        log_error(logger, "No se pudo establecer conexión con Master");
        liberar_recursos(query_info_global, config_global);
        return EXIT_FAILURE;
    }

    log_info(logger, "Conexión con Master establecida correctamente");
    
    // Iniciar hilo para escuchar respuestas del Master
    pthread_create(&hilo_escucha, NULL, escuchar_respuestas_master, query_info_global);
    
    // Si se proporcionaron query y prioridad como argumentos, enviar query inicial
    if (argc >= 4) {
        char* archivo_inicial = argv[2];
        int prioridad_inicial = atoi(argv[3]);
        
        log_info(logger, "Archivo de Query: %s", archivo_inicial);
        log_info(logger, "Prioridad: %d", prioridad_inicial);
        
        printf("Enviando query inicial: %s (prioridad: %d)\n\n", 
               archivo_inicial, prioridad_inicial);
        
        enviar_nueva_query(archivo_inicial, prioridad_inicial);
    } else {
        printf("Modo interactivo. Escriba 'QUERY <archivo> <prioridad>' para enviar queries.\n");
        printf("Ejemplo: QUERY query_ejemplo.txt 2\n");
        printf("Escriba 'exit' para salir.\n\n");
    }
    
    // Bucle principal de comandos mejorado
    char comando[1024];
    while (sistema_activo) {
        printf("query> ");
        fflush(stdout);
        
        if (fgets(comando, sizeof(comando), stdin) == NULL) {
            break;
        }
        
        // Eliminar salto de línea
        comando[strcspn(comando, "\n")] = '\0';
        
        if (strlen(comando) == 0) {
            continue;
        }
        
        // Procesar comandos
        if (strcmp(comando, "exit") == 0 || strcmp(comando, "quit") == 0) {
            printf("Saliendo...\n");
            break;
        }
        else if (strncmp(comando, "QUERY ", 6) == 0) {
            // Formato: QUERY archivo prioridad
            procesar_comando_query(comando + 6); // Saltar "QUERY "
        }
        else if (strcmp(comando, "help") == 0) {
            mostrar_ayuda();
        }else if (strcmp(comando, "stats") == 0) {
            printf("\n");
            printf("╔════════════════════════════════════════════════╗\n");
            printf("║              ESTADÍSTICAS                      ║\n");
            printf("╠════════════════════════════════════════════════╣\n");
            printf("║ Queries enviadas al Master: %-17d ║\n", contador_queries);
            printf("║ Conexión activa: %-27s ║\n", 
                query_info_global->socket_master > 0 ? "SÍ" : "NO");
            printf("╚════════════════════════════════════════════════╝\n");
            printf("\n");
        }
        else {
            printf("Comando no reconocido: '%s'\n", comando);
            printf("Escriba 'help' para ver los comandos disponibles.\n");
        }
    }
    
    // Finalizar sistema
    sistema_activo = false;
    
    // Esperar que termine el hilo de escucha
    pthread_join(hilo_escucha, NULL);
    
    // Liberar recursos
    liberar_recursos(query_info_global, config_global);
    
    log_info(logger, "=== Query Control finalizado ===");
    log_destroy(logger);
    
    return EXIT_SUCCESS;
}

t_config_query* cargar_configuracion(char* path_config) {
    
        // Buscar archivo de configuración en ubicaciones relativas
    char* ubicaciones[] = {
        path_config,                    // Ruta original
        "../query.config",              // Un nivel arriba (desde bin/)
        "../../query/query.config",     // Desde raíz del proyecto
        "./query.config",               // Directorio actual
        "query.config",                 // Solo el nombre
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
        fprintf(stderr, "Error: No se pudo encontrar query.config en ninguna ubicación\n");
        return NULL;
    }
    
    
    
    
    t_config* config = config_create(ruta_final);
    
    if (config == NULL) {
        fprintf(stderr, "Error: No se pudo abrir el archivo de configuración: %s\n", ruta_final);
        return NULL;
    }
    
    t_config_query* config_query = malloc(sizeof(t_config_query));
    if (config_query == NULL) {
        fprintf(stderr, "Error: malloc falló\n");
        config_destroy(config);
        return NULL;
    }
    
        // Inicializar a valores por defecto
    config_query->ip_master = NULL;
    config_query->puerto_master = 0;
    config_query->log_level = NULL;
    
    // Validar propiedades requeridas
    char* propiedades[] = {
        "IP_MASTER", "PUERTO_MASTER", "LOG_LEVEL"
    };
    
    for (int i = 0; i < 3; i++) {
        if (!config_has_property(config, propiedades[i])) {
            fprintf(stderr, "Error: Falta la propiedad %s en el archivo de configuración\n", propiedades[i]);
            free(config_query);
            config_destroy(config);
            return NULL;
        }
    }
    
    // Cargar propiedades
    config_query->ip_master = strdup(config_get_string_value(config, "IP_MASTER"));
    config_query->puerto_master = config_get_int_value(config, "PUERTO_MASTER");
    config_query->log_level = strdup(config_get_string_value(config, "LOG_LEVEL"));
    
    // Verificar que se asignaron correctamente
    if (config_query->ip_master == NULL || config_query->log_level == NULL) {
        fprintf(stderr, "Error: strdup falló\n");
        destruir_config_query(config_query);
        config_destroy(config);
        return NULL;
    }
    
    printf("Configuración cargada:\n");
    printf("  IP_MASTER: %s\n", config_query->ip_master);
    printf("  PUERTO_MASTER: %d\n", config_query->puerto_master);
    printf("  LOG_LEVEL: %s\n", config_query->log_level);
    
    config_destroy(config);
    return config_query;
}

void realizar_handshake_con_master(int socket_master) {

    op_code handshake = HANDSHAKE_QUERY_CONTROL;

    log_info(logger, "Enviando handshake QUERY_CONTROL (%d) al Master...", handshake);

    // Enviar handshake
    if (send(socket_master, &handshake, sizeof(op_code), 0) <= 0) {
        log_error(logger, "Error enviando handshake al Master");
        close(socket_master);
        exit(EXIT_FAILURE);
    }

    log_info(logger, "Handshake enviado. Esperando confirmación...");

    // Esperar confirmación del Master
    op_code respuesta;
    int bytes = recv(socket_master, &respuesta, sizeof(op_code), MSG_WAITALL);

    if (bytes <= 0) {
        log_error(logger, "El Master se desconectó o no envió confirmación");
        close(socket_master);
        exit(EXIT_FAILURE);
    }

    if (respuesta == CONFIRMATION) {
        log_info(logger, "Handshake aceptado por Master (CONFIRMATION recibido)");
    } else {
        log_error(logger, "Master devolvió handshake inválido. Código recibido: %d", respuesta);
        close(socket_master);
        exit(EXIT_FAILURE);
    }
}


void conectar_con_master(t_query_info* query_info, t_config_query* config) {
    log_info(logger, "Intentando conectar con Master en %s:%d", 
             config->ip_master, config->puerto_master);
    
    query_info->socket_master = crear_conexion(logger, config->ip_master, 
                                               config->puerto_master);
    
    if (query_info->socket_master == -1) {
        log_error(logger, "Error al conectar con Master");
        return;
    }
    
    log_info(logger, "## Conexión al Master exitosa. IP: %s, Puerto: %d", 
             config->ip_master, config->puerto_master);

    // ENVIAR HANDSHAKE INMEDIATAMENTE AL CONECTAR
    realizar_handshake_con_master(query_info->socket_master);
        
    log_info(logger, "Conexión con Master establecida correctamente");
}

void enviar_solicitud_query(t_query_info* query_info) {
    log_info(logger, "## Solicitud de ejecución de Query: %s, prioridad: %d", 
             query_info->archivo_query, query_info->prioridad);
    
    // PRIMERO enviar el código de operación por separado
    op_code cod_op = QUERY_CONTROL;
    log_info(logger, "Enviando código de operación: %d", cod_op);
    
    ssize_t bytes_sent = send(query_info->socket_master, &cod_op, sizeof(op_code), MSG_NOSIGNAL);
    if (bytes_sent <= 0) {
        log_error(logger, "Error al enviar código de operación al Master");
        return;
    }
    
    // LUEGO crear y enviar el paquete con los datos
    t_paquete* paquete = crear_paquete(QUERY_CONTROL, logger);
    
    if (paquete == NULL) {
        log_error(logger, "Error al crear paquete para enviar query");
        return;
    }

    log_info(logger, "Paquete creado - Código OP: %d", QUERY_CONTROL);

    int tam_nombre = strlen(query_info->archivo_query) + 1;
    log_info(logger, "Agregando nombre de archivo: '%s' (tamaño: %d)", 
             query_info->archivo_query, tam_nombre);

    // Agregar nombre del archivo de query
    agregar_a_paquete(paquete, query_info->archivo_query, tam_nombre);
    
    // Agregar prioridad
    log_info(logger, "Agregando prioridad: %d", query_info->prioridad);
    agregar_a_paquete(paquete, &(query_info->prioridad), sizeof(int));
    
    // Enviar paquete
    log_info(logger, "Enviando paquete al Master...");
    enviar_paquete(paquete, query_info->socket_master);
    
    log_info(logger, "Paquete enviado exitosamente - Tamaño total: %d bytes", 
             paquete->buffer->size);

    // Liberar paquete
    eliminar_paquete(paquete);
    
    log_info(logger, "Solicitud de Query enviada exitosamente");
}

void esperar_respuestas_master(t_query_info* query_info) {
    log_info(logger, "Esperando respuestas del Master...");
    
    bool continuar = true;
    
    while (continuar) {
        // Recibir código de operación
        op_code cod_op;
        ssize_t bytes_recibidos = recv(query_info->socket_master, &cod_op, 
                                      sizeof(op_code), MSG_WAITALL);
        
        if (bytes_recibidos <= 0) {
            if (bytes_recibidos == 0) {
                log_warning(logger, "Master cerró la conexión");
            } else {
                log_error(logger, "Error al recibir código de operación: %s", 
                         strerror(errno));
            }
            continuar = false;
            break;
        }
        
        log_debug(logger, "Código de operación recibido: %d", cod_op);
        
        switch (cod_op) {
            case READ: {
                // Recibir tamaño del buffer
                int size;
                recv(query_info->socket_master, &size, sizeof(int), MSG_WAITALL);
                
                // Recibir nombre del File (formato File:Tag)
                int tam_file;
                recv(query_info->socket_master, &tam_file, sizeof(int), MSG_WAITALL);
                char* nombre_file = malloc(tam_file);
                recv(query_info->socket_master, nombre_file, tam_file, MSG_WAITALL);
                
                // Recibir contenido leído
                int tam_contenido;
                recv(query_info->socket_master, &tam_contenido, sizeof(int), MSG_WAITALL);
                char* contenido = malloc(tam_contenido);
                recv(query_info->socket_master, contenido, tam_contenido, MSG_WAITALL);
                
                log_info(logger, "## Lectura realizada: File %s, contenido: %s", 
                        nombre_file, contenido);
                
                free(nombre_file);
                free(contenido);
                break;
            }
            
            case EXIT: {
                // Recibir tamaño del buffer
                int size;
                recv(query_info->socket_master, &size, sizeof(int), MSG_WAITALL);
                
                // Recibir motivo de finalización
                int tam_motivo;
                recv(query_info->socket_master, &tam_motivo, sizeof(int), MSG_WAITALL);
                char* motivo = malloc(tam_motivo);
                recv(query_info->socket_master, motivo, tam_motivo, MSG_WAITALL);
                
                log_info(logger, "## Query Finalizada - %s", motivo);
                
                free(motivo);
                continuar = false;
                break;
            }
            
            case SYSCALL_ERROR: {
                // Recibir tamaño del buffer
                int size;
                recv(query_info->socket_master, &size, sizeof(int), MSG_WAITALL);
                
                // Recibir mensaje de error
                int tam_error;
                recv(query_info->socket_master, &tam_error, sizeof(int), MSG_WAITALL);
                char* error = malloc(tam_error);
                recv(query_info->socket_master, error, tam_error, MSG_WAITALL);
                
                log_error(logger, "Error en ejecución de Query: %s", error);
                log_info(logger, "## Query Finalizada - Error: %s", error);
                
                free(error);
                continuar = false;
                break;
            }
            
            default:
                log_warning(logger, "Código de operación no reconocido: %d", cod_op);
                break;
        }
    }
    
    log_info(logger, "Finalizada la espera de respuestas del Master");
}

void liberar_recursos(t_query_info* query_info, t_config_query* config) {
    if (query_info != NULL) {
        if (query_info->socket_master != -1) {
            liberar_conexion(query_info->socket_master);
            log_info(logger, "Conexión con Master cerrada");
        }
        if (query_info->archivo_query != NULL) {
            free(query_info->archivo_query);
        }
        free(query_info);
    }
    
    destruir_config_query(config);
}

void destruir_config_query(t_config_query* config) {
    if (config != NULL) {
        if (config->ip_master != NULL) {
            free(config->ip_master);
        }
        if (config->log_level != NULL) {
            free(config->log_level);
        }
        free(config);
    }
}