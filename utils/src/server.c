#include "server.h"


int iniciar_servidor(t_log* logger, int puerto)
{
    log_info(logger, "Se inicia servidor.");

    int socket_servidor;
    struct addrinfo hints, *servinfo;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    char puerto_char[6];
	sprintf(puerto_char, "%d", puerto); // convierte int a string

    int status = getaddrinfo(NULL, puerto_char, &hints, &servinfo);
    if (status != 0) {
    log_error(logger, "Error en getaddrinfo: %s", gai_strerror(status));
    return -1;
    }

    socket_servidor = socket(servinfo->ai_family, servinfo->ai_socktype, servinfo->ai_protocol);
    if (socket_servidor == -1) {
        log_error(logger, "Error al crear el socket de servidor.");
        freeaddrinfo(servinfo);
        return -1;
    }

    log_info(logger, "Se crea al socket %d del servidor en puerto %d.", socket_servidor, puerto);

    if (bind(socket_servidor, servinfo->ai_addr, servinfo->ai_addrlen) == -1) {
        log_error(logger, "Error al asociar el socket con el puerto.");
        close(socket_servidor);
        freeaddrinfo(servinfo);
        return -1;
    }

    log_info(logger, "Bind exitoso en puerto %d", puerto); 

    if (listen(socket_servidor, SOMAXCONN) == -1) {
        log_error(logger, "Error al escuchar conexiones entrantes. Errno: %d (%s)", errno, strerror(errno));
        close(socket_servidor);
        freeaddrinfo(servinfo);
        return -1;
    }

    log_info(logger, "Escuchando conexiones entrantes en el puerto %d", puerto);

    if (servinfo != NULL) {
        freeaddrinfo(servinfo);
    }
    log_info(logger, "Listo para escuchar a mi cliente");

    return socket_servidor;
}


int esperar_cliente(t_log* logger, int socket_servidor)
{

    log_info(logger, "inicio funcion esperar_cliente Esperando cliente...");

    int socket_cliente = accept(socket_servidor, NULL, NULL);
    if (socket_cliente == -1) {
        log_error(logger, "Error al aceptar la conexión del cliente.");
    } else {
        log_info(logger, "Cliente conectado.");
    }

    return socket_cliente;
}


int recibir_operacion(t_log* logger, int socket_cliente) {
    if (socket_cliente <= 0) {
        if (logger) log_error(logger, "Error: socket inválido: %d", socket_cliente);
        return -1;
    }
    
    int cod_op;
    ssize_t bytes_recibidos = recv(socket_cliente, &cod_op, sizeof(int), MSG_WAITALL);
    
    if (bytes_recibidos == 0) {
        if (logger) log_info(logger, "El cliente cerró la conexión");
        return -1;
    }
    
    if (bytes_recibidos != sizeof(int)) {
        if (logger) log_error(logger, "Error al recibir código de operación. Recibidos: %zd, esperados: %zu", bytes_recibidos, sizeof(int));
        return -1;
    }
    
    if (logger) log_debug(logger, "Código de operación recibido: %d", cod_op);
    return cod_op;
}

void* recibir_buffer(int* size, int socket_cliente) {   
    if (size == NULL || socket_cliente < 0) {
        log_error(logger, "Parámetros inválidos en recibir_buffer");
        return NULL;
    }

    log_info(logger, "Comenzando la recepción del buffer...");

    // Recibir tamaño
    ssize_t bytes_recibidos = recv(socket_cliente, size, sizeof(int), MSG_WAITALL);
    if (bytes_recibidos <= 0) {
        log_error(logger, "Error al recibir tamaño del buffer. Bytes: %zd, Error: %s", 
                 bytes_recibidos, strerror(errno));
        return NULL;
    } else if (bytes_recibidos != sizeof(int)) {
        log_error(logger, "Tamaño recibido incompleto. Esperados: %zu, Recibidos: %zd",
                 sizeof(int), bytes_recibidos);
        return NULL;
    }

    log_info(logger, "Tamaño de buffer recibido: %d bytes", *size);

    if (*size <= 0) { 
        log_error(logger, "Tamaño de buffer inválido: %d", *size);
        return NULL;
    }

    // Validar tamaño máximo razonable (por ejemplo, 10MB)
    if (*size > 10 * 1024 * 1024) {
        log_error(logger, "Tamaño de buffer excesivamente grande: %d bytes", *size);
        return NULL;
    }

    // Asignar memoria con verificación adicional
    void* buffer = malloc(*size);
    if (buffer == NULL) {
        log_error(logger, "Error al asignar %d bytes para buffer", *size);
        return NULL;
    }

    // Recibir datos con verificación completa
    bytes_recibidos = recv(socket_cliente, buffer, *size, MSG_WAITALL);
    if (bytes_recibidos != *size) {
        log_error(logger, "Error al recibir buffer. Esperados: %d, Recibidos: %zd", *size, bytes_recibidos);
        free(buffer);
        return NULL;
    }

    log_info(logger, "Buffer recibido correctamente (%d bytes)", *size);
    return buffer;
}

char* recibir_mensaje(t_log* logger, int socket_cliente)
{
    log_info(logger, "Comenzando la recepción del mensaje...");

    int size;
    char* buffer;

    if (recv(socket_cliente, &size, sizeof(int), MSG_WAITALL) <= 0 || size <= 0) {
        log_error(logger, "Error al recibir el tamaño del mensaje o tamaño inválido.");
        return NULL;
    }

    log_info(logger, "Tamaño del mensaje recibido: %d bytes", size);

    buffer = malloc(size + 1); // +1 para '\0'
    if (recv(socket_cliente, buffer, size, MSG_WAITALL) <= 0) {
        log_error(logger, "Error al recibir el mensaje.");
        free(buffer);
        return NULL;
    }

    buffer[size] = '\0'; // Aseguramos que termine en '\0'

    log_info(logger, "Mensaje recibido: %s", buffer);

    return buffer;  // Lo devolvemos para ser utilizado
}


// Función específica para recibir paquetes de t_paquete (con código de operación ya leído)
t_list* recibir_paquete_desde_buffer(t_log* logger, int socket_cliente) {
    if (socket_cliente <= 0) {
        if (logger) log_error(logger, "Error: socket inválido: %d", socket_cliente);
        return NULL;
    }
    
    int buffer_size;
    
    // Recibir el tamaño del buffer
    ssize_t bytes_recibidos = recv(socket_cliente, &buffer_size, sizeof(int), MSG_WAITALL);
    if (bytes_recibidos != sizeof(int)) {
        if (logger) log_error(logger, "Error al recibir tamaño del buffer. Recibidos: %zd, esperados: %zu", bytes_recibidos, sizeof(int));
        return NULL;
    }
    
    if (logger) log_info(logger, "Tamaño de buffer recibido: %d bytes", buffer_size);
    
    // Validar que el tamaño sea razonable
    if (buffer_size <= 0) {
        if (logger) log_warning(logger, "Paquete vacío recibido (size = %d)", buffer_size);
        return list_create();
    }
    
    // Validar tamaño máximo (10MB)
    if (buffer_size > 10 * 1024 * 1024) {
        if (logger) log_error(logger, "Tamaño de buffer excesivamente grande: %d bytes", buffer_size);
        return NULL;
    }
    
    // Recibir el contenido del buffer
    void* buffer = malloc(buffer_size);
    if (buffer == NULL) {
        if (logger) log_error(logger, "Error: no se pudo allocar memoria para el buffer de %d bytes", buffer_size);
        return NULL;
    }
    
    bytes_recibidos = recv(socket_cliente, buffer, buffer_size, MSG_WAITALL);
    if (bytes_recibidos != buffer_size) {
        if (logger) log_error(logger, "Error al recibir contenido del buffer. Recibidos: %zd, esperados: %d", bytes_recibidos, buffer_size);
        free(buffer);
        return NULL;
    }
    
    if (logger) log_info(logger, "Buffer recibido correctamente (%d bytes)", buffer_size);
    
    // Deserializar el buffer en una lista
    t_list* valores = list_create();
    int desplazamiento = 0;
    
    while (desplazamiento < buffer_size) {
        // Verificar que hay espacio suficiente para el tamaño del elemento
        if (desplazamiento + sizeof(int) > (size_t)buffer_size) {
            if (logger) log_error(logger, "Error: buffer corrupto al leer tamaño del elemento en desplazamiento %d", desplazamiento);
            list_destroy_and_destroy_elements(valores, free);
            free(buffer);
            return NULL;
        }
        
        // Leer el tamaño del siguiente elemento
        int tamanio_elemento;
        memcpy(&tamanio_elemento, buffer + desplazamiento, sizeof(int));
        desplazamiento += sizeof(int);
        
        if (logger) log_debug(logger, "Tamaño del elemento: %d", tamanio_elemento);
        
        // Validar tamaño del elemento
        if (tamanio_elemento <= 0 || tamanio_elemento > 1024) { // Máximo 1KB por elemento
            if (logger) log_error(logger, "Error: tamaño de elemento inválido: %d", tamanio_elemento);
            list_destroy_and_destroy_elements(valores, free);
            free(buffer);
            return NULL;
        }
        
        if (desplazamiento + tamanio_elemento > buffer_size) {
            if (logger) log_error(logger, "Error: tamaño de elemento excede el buffer. Elemento: %d, Disponible: %d", 
                                 tamanio_elemento, buffer_size - desplazamiento);
            list_destroy_and_destroy_elements(valores, free);
            free(buffer);
            return NULL;
        }
        
        // Leer el contenido del elemento
        char* elemento = malloc(tamanio_elemento);
        if (elemento == NULL) {
            if (logger) log_error(logger, "Error: no se pudo allocar memoria para elemento de %d bytes", tamanio_elemento);
            list_destroy_and_destroy_elements(valores, free);
            free(buffer);
            return NULL;
        }
        
        memcpy(elemento, buffer + desplazamiento, tamanio_elemento);
        desplazamiento += tamanio_elemento;
        
        // Agregar a la lista
        list_add(valores, elemento);
        
        if (logger) log_debug(logger, "Elemento %d agregado a la lista", list_size(valores));
    }
    
    free(buffer);
    
    if (logger) {
        log_info(logger, "Paquete deserializado exitosamente - %d elementos, tamaño total: %d bytes", 
                list_size(valores), buffer_size);
    }
    
    return valores;
}


t_list* recibir_paquete(t_log* logger, int socket_cliente) {
    if (socket_cliente <= 0) {
        if (logger) log_error(logger, "Error: socket inválido: %d", socket_cliente);
        return NULL;
    }
    
    int size;

    // Recibir el tamaño del buffer (se envía como int en crear_paquete/enviar_paquete)
    ssize_t bytes_recibidos = recv(socket_cliente, &size, sizeof(int), MSG_WAITALL);
    if (bytes_recibidos != sizeof(int)) {
        if (logger) log_error(logger, "Error al recibir tamaño del buffer. Recibidos: %zd, esperados: %zu", bytes_recibidos, sizeof(int));
        return NULL;
    }

    if (logger) log_info(logger, "Tamaño de buffer recibido: %d bytes", size);

    // Validar que el tamaño sea razonable
    if (size <= 0) {
        if (logger) log_warning(logger, "Paquete vacío recibido (size = %d)", size);
        return list_create();
    }

    // Validar tamaño máximo (10MB)
    if (size > 10 * 1024 * 1024) {
        if (logger) log_error(logger, "Tamaño de buffer excesivamente grande: %d bytes", size);
        return NULL;
    }

    // Recibir el contenido del buffer
    void* buffer = malloc(size);
    if (buffer == NULL) {
        if (logger) log_error(logger, "Error: no se pudo allocar memoria para el buffer de %d bytes", size);
        return NULL;
    }

    bytes_recibidos = recv(socket_cliente, buffer, size, MSG_WAITALL);
    if (bytes_recibidos != (ssize_t)size) {
        if (logger) log_error(logger, "Error al recibir contenido del buffer. Recibidos: %zd, esperados: %d", bytes_recibidos, size);
        free(buffer);
        return NULL;
    }

    if (logger) log_info(logger, "Buffer recibido correctamente (%d bytes)", size);
    
    // Deserializar el buffer en una lista
    t_list* valores = list_create();
    size_t desplazamiento = 0;
    
    while (desplazamiento < (size_t)size) {
        // Verificar que hay espacio suficiente para el tamaño del elemento
        if (desplazamiento + sizeof(int) > (size_t)size) {
            if (logger) log_error(logger, "Error: buffer corrupto al leer tamaño del elemento en desplazamiento %zu", desplazamiento);
            list_destroy_and_destroy_elements(valores, free);
            free(buffer);
            return NULL;
        }
        
        // Leer el tamaño del siguiente elemento
        int tamanio_elemento;
        memcpy(&tamanio_elemento, buffer + desplazamiento, sizeof(int));
        desplazamiento += sizeof(int);
        
        if (logger) log_debug(logger, "Tamaño del elemento: %d", tamanio_elemento);
        
        // Validar tamaño del elemento
        if (tamanio_elemento <= 0 || tamanio_elemento > 1024) { // Máximo 1KB por elemento
            if (logger) log_error(logger, "Error: tamaño de elemento inválido: %d", tamanio_elemento);
            list_destroy_and_destroy_elements(valores, free);
            free(buffer);
            return NULL;
        }
        
        if (desplazamiento + (size_t)tamanio_elemento > (size_t)size) {
            if (logger) log_error(logger, "Error: tamaño de elemento excede el buffer. Elemento: %d, Disponible: %zu", 
                                 tamanio_elemento, (size_t)size - desplazamiento);
            list_destroy_and_destroy_elements(valores, free);
            free(buffer);
            return NULL;
        }
        
        // Leer el contenido del elemento
        char* elemento = malloc(tamanio_elemento);
        if (elemento == NULL) {
            if (logger) log_error(logger, "Error: no se pudo allocar memoria para elemento de %d bytes", tamanio_elemento);
            list_destroy_and_destroy_elements(valores, free);
            free(buffer);
            return NULL;
        }
        
        memcpy(elemento, buffer + desplazamiento, tamanio_elemento);
        desplazamiento += tamanio_elemento;
        
        // Agregar a la lista
        list_add(valores, elemento);
        
        if (logger) log_debug(logger, "Elemento %d agregado a la lista", list_size(valores));
    }
    
    free(buffer);
    
    if (logger) {
        log_info(logger, "Paquete deserializado exitosamente - %d elementos, tamaño total: %d bytes", 
                list_size(valores), size);
    }
    
    return valores;
}

void* recibir_contenido_paquete(int* size, int socket_cliente) {
    if (size == NULL || socket_cliente < 0) {
        log_error(logger, "Parámetros inválidos en recibir_contenido_paquete");
        return NULL;
    }

    // 1. Recibir tamaño del contenido
    if(recv(socket_cliente, size, sizeof(int), MSG_WAITALL) != sizeof(int)) {
        log_error(logger, "Error al recibir tamaño del contenido");
        return NULL;
    }

    // Si no hay contenido, retornar NULL
    if(*size <= 0) {
        log_info(logger, "Paquete sin contenido (size: %d)", *size);
        return NULL;
    }

    // Validar tamaño máximo
    if (*size > 10 * 1024 * 1024) {
        log_error(logger, "Tamaño de contenido excesivamente grande: %d bytes", *size);
        return NULL;
    }

    // 2. Recibir el contenido
    void* contenido = malloc(*size);
    if(contenido == NULL) {
        log_error(logger, "Error al asignar memoria para contenido");
        return NULL;
    }

    if(recv(socket_cliente, contenido, *size, MSG_WAITALL) != *size) {
        log_error(logger, "Error al recibir contenido del paquete");
        free(contenido);
        return NULL;
    }

    log_info(logger, "Contenido recibido correctamente (%d bytes)", *size);
    return contenido;
}

/*
// Función para recibir un paquete completo (estructura t_paquete)
t_paquete* recibir_paquete_completo(t_log* logger, int socket_cliente) {
    if (socket_cliente <= 0) {
        if (logger) log_error(logger, "Error: socket inválido: %d", socket_cliente);
        return NULL;
    }
    
    // Crear el paquete
    t_paquete* paquete = malloc(sizeof(t_paquete));
    if (paquete == NULL) {
        if (logger) log_error(logger, "Error: no se pudo allocar memoria para el paquete");
        return NULL;
    }
    
    // El código de operación ya fue leído por recibir_operacion()
    // Solo necesitamos recibir el buffer
    
    // Crear el buffer
    paquete->buffer = malloc(sizeof(t_buffer));
    if (paquete->buffer == NULL) {
        if (logger) log_error(logger, "Error: no se pudo allocar memoria para el buffer");
        free(paquete);
        return NULL;
    }
    
    // Recibir tamaño del buffer
    ssize_t bytes_recibidos = recv(socket_cliente, &(paquete->buffer->size), sizeof(int), MSG_WAITALL);
    if (bytes_recibidos != sizeof(int)) {
        if (logger) log_error(logger, "Error al recibir tamaño del buffer. Recibidos: %zd, esperados: %zu", bytes_recibidos, sizeof(int));
        free(paquete->buffer);
        free(paquete);
        return NULL;
    }
    
    // Validar tamaño
    if (paquete->buffer->size > 10 * 1024 * 1024) {
        if (logger) log_error(logger, "Tamaño de buffer excesivamente grande: %d bytes", paquete->buffer->size);
        free(paquete->buffer);
        free(paquete);
        return NULL;
    }
    
    // Recibir contenido del buffer (si tiene datos)
    if (paquete->buffer->size > 0) {
        paquete->buffer->stream = malloc(paquete->buffer->size);
        if (paquete->buffer->stream == NULL) {
            if (logger) log_error(logger, "Error: no se pudo allocar memoria para el stream");
            free(paquete->buffer);
            free(paquete);
            return NULL;
        }
        
        bytes_recibidos = recv(socket_cliente, paquete->buffer->stream, paquete->buffer->size, MSG_WAITALL);
        if (bytes_recibidos != paquete->buffer->size) {
            if (logger) log_error(logger, "Error al recibir contenido del buffer. Recibidos: %zd, esperados: %d", bytes_recibidos, paquete->buffer->size);
            free(paquete->buffer->stream);
            free(paquete->buffer);
            free(paquete);
            return NULL;
        }
    } else {
        paquete->buffer->stream = NULL;
    }
    
    if (logger) {
        log_info(logger, "Paquete completo recibido exitosamente - Tamaño: %d bytes", paquete->buffer->size);
    }
    
    return paquete;
}
*/

bool limpiar_buffer(int socket_fd) {
    if (socket_fd <= 0) {
        log_error(logger, "Descriptor de socket inválido en limpiar_buffer: %d", socket_fd);
        return false;
    }

    char buffer_temp[1024];
    int bytes_leidos;
    bool buffer_limpio = false;
    int intentos = 0;
    const int max_intentos = 10; // Para evitar bucles infinitos

    // Configurar el socket como no bloqueante temporalmente
    int flags = fcntl(socket_fd, F_GETFL, 0);
    if (flags == -1) {
        log_error(logger, "Error al obtener flags del socket: %s", strerror(errno));
        return false;
    }
    
    if (fcntl(socket_fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        log_error(logger, "Error al configurar socket como no bloqueante: %s", strerror(errno));
        return false;
    }

    // Leer todo lo que haya en el buffer
    while (intentos < max_intentos && !buffer_limpio) {
        bytes_leidos = recv(socket_fd, buffer_temp, sizeof(buffer_temp), MSG_DONTWAIT);
        
        if (bytes_leidos > 0) {
            log_warning(logger, "Descartados %d bytes residuales del buffer del socket %d", 
                       bytes_leidos, socket_fd);
            intentos++;
            continue;
        } else if (bytes_leidos == 0) {
            // Conexión cerrada por el otro extremo
            log_info(logger, "Conexión cerrada mientras se limpiaba el buffer");
            buffer_limpio = true;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // No hay más datos para leer
                buffer_limpio = true;
            } else {
                // Error real
                log_error(logger, "Error al limpiar buffer del socket %d: %s", 
                         socket_fd, strerror(errno));
                break;
            }
        }
    }

    // Restaurar flags originales del socket
    if (fcntl(socket_fd, F_SETFL, flags) == -1) {
        log_error(logger, "Error al restaurar flags del socket: %s", strerror(errno));
    }

    if (intentos >= max_intentos) {
        log_warning(logger, "Se alcanzó el máximo de intentos al limpiar el buffer del socket %d", socket_fd);
    }

    return buffer_limpio;
}


int recibir_paquete_completo(t_log* logger, int socket, void** buffer) {
    int32_t header[2]; // cod_op y tamaño (en orden host)

    // 1. Recibir encabezado fijo (8 bytes)
    int recibido = recv(socket, header, sizeof(header), MSG_WAITALL);
    if (recibido != sizeof(header)) {
        if (recibido == 0) return -1; // Conexión cerrada
        return 0; // Error
    }

    int32_t cod_op = header[0];
    int32_t tamanio = header[1];

    if (logger) {
        log_info(logger, "Recibido cod_op: %d, tamaño: %d", cod_op, tamanio);
    }

    // 2. Reservar y recibir payload
    if (tamanio < 0) {
        if (logger) log_error(logger, "Tamaño negativo en paquete recibido: %d", tamanio);
        return 0;
    }

    *buffer = malloc(tamanio);
    recibido = recv(socket, *buffer, tamanio, MSG_WAITALL);
    if(recibido != (ssize_t)tamanio) {
        free(*buffer);
        return 0;
    }
    
    return cod_op;
}


int start_server(void* storage, t_log* logger) {
    log_info(logger, "Iniciando servidor en puerto %d", PORT);

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == -1) {
        log_error(logger, "Error al crear socket de servidor. Errno: %d (%s)", errno, strerror(errno));
        return -1;
    }
    log_info(logger, "Socket de servidor creado correctamente (fd=%d)", server_fd);

    struct sockaddr_in address;
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        log_error(logger, "Error al hacer bind en puerto %d. Errno: %d (%s)", PORT, errno, strerror(errno));
        close(server_fd);
        return -1;
    }
    log_info(logger, "Bind exitoso en puerto %d", PORT);

    if (listen(server_fd, MAX_CLIENTS) < 0) {
        log_error(logger, "Error al poner socket en modo escucha. Errno: %d (%s)", errno, strerror(errno));
        close(server_fd);
        return -1;
    }
    log_info(logger, "Servidor escuchando conexiones entrantes (máx. %d en cola).", MAX_CLIENTS);

    while (1) {
        int client_socket = accept(server_fd, NULL, NULL);
        if (client_socket < 0) {
            log_error(logger, "Error al aceptar cliente. Errno: %d (%s)", errno, strerror(errno));
            continue;
        }

        log_info(logger, "Nueva conexión aceptada (fd=%d)", client_socket);

        client_context_t* context = malloc(sizeof(client_context_t));
        if (!context) {
            log_error(logger, "Error al reservar memoria para el contexto de cliente.");
            close(client_socket);
            continue;
        }

        context->storage = storage;
        context->server_socket = client_socket;

        pthread_t thread_id;
        if (pthread_create(&thread_id, NULL, handle_client, context) != 0) {
            log_error(logger, "Error al crear hilo para cliente (fd=%d). Errno: %d (%s)",
                      client_socket, errno, strerror(errno));
            free(context);
            close(client_socket);
        } else {
            log_info(logger, "Hilo creado para cliente (fd=%d, tid=%lu)", client_socket, thread_id);
            pthread_detach(thread_id); // importante: evitar fugas de hilos
        }
    }

    // Nunca llega acá, pero es buena práctica cerrar el socket
    close(server_fd);
    return 0;
}

// =========================
// Función recibir_string
// =========================
char* recibir_string(int socket_cliente, t_log* logger) {
    if (socket_cliente <= 0) {
        if (logger) log_error(logger, "Socket inválido en recibir_string: %d", socket_cliente);
        return NULL;
    }

    int size;
    ssize_t bytes_recibidos = recv(socket_cliente, &size, sizeof(int), MSG_WAITALL);
    if (bytes_recibidos != sizeof(int) || size <= 0) {
        if (logger) log_error(logger, "Error al recibir tamaño de string. Recibidos: %zd, esperados: %zu", bytes_recibidos, sizeof(int));
        return NULL;
    }

    char* buffer = malloc(size + 1);
    if (!buffer) {
        if (logger) log_error(logger, "Error al asignar memoria para string de %d bytes", size);
        return NULL;
    }

    bytes_recibidos = recv(socket_cliente, buffer, size, MSG_WAITALL);
    if (bytes_recibidos != size) {
        if (logger) log_error(logger, "Error al recibir string. Recibidos: %zd, esperados: %d", bytes_recibidos, size);
        free(buffer);
        return NULL;
    }

    buffer[size] = '\0'; // asegurar terminación
    if (logger) log_info(logger, "String recibido: %s", buffer);
    return buffer;
}

// =========================
// Función handle_client
// =========================
void* handle_client(void* arg) {
    client_context_t* context = (client_context_t*)arg;
    t_log* logger = context->logger;
    int socket_cliente = context->server_socket;
    //void* storage = context->storage;

    if (logger) log_info(logger, "Manejando cliente en socket %d", socket_cliente);

    while (1) {
        // Recibir operación
        int cod_op = recibir_operacion(logger, socket_cliente);
        if (cod_op <= 0) {
            if (logger) log_info(logger, "Cliente desconectado o error al recibir operación (fd=%d)", socket_cliente);
            break;
        }

        // Ejemplo de manejo: recibir un string
        char* mensaje = recibir_string(socket_cliente, logger);
        if (mensaje) {
            if (logger) log_info(logger, "Procesando mensaje del cliente: %s", mensaje);
            free(mensaje);
        } else {
            if (logger) log_warning(logger, "No se pudo recibir mensaje del cliente.");
        }


    }

    close(socket_cliente);
    if (logger) log_info(logger, "Cliente desconectado, socket cerrado (fd=%d)", socket_cliente);
    free(context);
    return NULL;
}


// Función auxiliar para enviar respuesta OK
void enviar_ok(int socket_cliente) {
    // Usar OP_OK (210) en lugar de 209 para consistencia
    int codigo = htonl(OP_OK);  // 210 según conexion.h
    send(socket_cliente, &codigo, sizeof(int), MSG_NOSIGNAL);
    log_info(logger, "OP_OK enviado exitosamente (valor: %d, bytes: %zu)", OP_OK, sizeof(int));
}

// Función para enviar error
void enviar_error(int socket_cliente) {
    int codigo = htonl(OP_ERROR);  // 211 según conexion.h
    send(socket_cliente, &codigo, sizeof(int), MSG_NOSIGNAL);
    log_info(logger, "OP_ERROR enviado exitosamente (valor: %d)", OP_ERROR);
}

char* recibir_mensaje_error(int socket) {
    t_list* elementos = recibir_paquete(logger, socket);
    
    if (elementos == NULL || list_size(elementos) == 0) {
        return strdup("Error desconocido");
    }
    
    char* mensaje = strdup((char*)list_get(elementos, 0));
    list_destroy_and_destroy_elements(elementos, free);
    
    return mensaje;
}

void debug_recibir_tamaño_buffer(t_log* logger, int socket_qc) {
    // Leer los primeros 8 bytes crudos del socket
    unsigned char buffer_crudo[8];
    ssize_t bytes = recv(socket_qc, buffer_crudo, sizeof(buffer_crudo), MSG_PEEK);
    
    if (bytes >= 4) {
        log_info(logger, "🔍 DEBUG - Primeros 4 bytes crudos del buffer:");
        for (int i = 0; i < 4 && i < bytes; i++) {
            log_info(logger, "  Byte[%d]: 0x%02X (%u decimal)", i, buffer_crudo[i], buffer_crudo[i]);
        }
        
        // Interpretar como int
        int tamaño_int;
        memcpy(&tamaño_int, buffer_crudo, sizeof(int));
        log_info(logger, "🔍 DEBUG - Interpretado como int: %d", tamaño_int);
        
        // Interpretar como uint32_t con ntohl
        uint32_t tamaño_network;
        memcpy(&tamaño_network, buffer_crudo, sizeof(uint32_t));
        uint32_t tamaño_host = ntohl(tamaño_network);
        log_info(logger, "🔍 DEBUG - Interpretado como uint32_t (ntohl): %u", tamaño_host);
    }
}

t_list* deserializar_paquete(void* buffer, uint32_t buffer_size, t_log* logger) {
    if (buffer == NULL || buffer_size == 0) {
        if (logger) log_error(logger, "Buffer inválido en deserializar_paquete");
        return NULL;
    }
    
    t_list* elementos = list_create();
    size_t desplazamiento = 0;
    
    while (desplazamiento < buffer_size) {
        // Verificar que hay espacio suficiente para el tamaño del elemento
        if (desplazamiento + sizeof(int) > buffer_size) {
            if (logger) log_error(logger, "Error: buffer corrupto al leer tamaño del elemento en desplazamiento %zu", desplazamiento);
            list_destroy_and_destroy_elements(elementos, free);
            return NULL;
        }
        
        // Leer el tamaño del siguiente elemento
        int tamanio_elemento;
        memcpy(&tamanio_elemento, (char*)buffer + desplazamiento, sizeof(int));
        desplazamiento += sizeof(int);
        
        if (logger) log_debug(logger, "Tamaño del elemento: %d", tamanio_elemento);
        
        // Validar tamaño del elemento
        if (tamanio_elemento <= 0 || tamanio_elemento > 1024) { // Máximo 1KB por elemento
            if (logger) log_error(logger, "Error: tamaño de elemento inválido: %d", tamanio_elemento);
            list_destroy_and_destroy_elements(elementos, free);
            return NULL;
        }
        
        if (desplazamiento + (size_t)tamanio_elemento > buffer_size) {
            if (logger) log_error(logger, "Error: tamaño de elemento excede el buffer. Elemento: %d, Disponible: %zu", 
                                 tamanio_elemento, buffer_size - desplazamiento);
            list_destroy_and_destroy_elements(elementos, free);
            return NULL;
        }
        
        // Leer el contenido del elemento
        char* elemento = malloc(tamanio_elemento);
        if (elemento == NULL) {
            if (logger) log_error(logger, "Error: no se pudo allocar memoria para elemento de %d bytes", tamanio_elemento);
            list_destroy_and_destroy_elements(elementos, free);
            return NULL;
        }
        
        memcpy(elemento, (char*)buffer + desplazamiento, tamanio_elemento);
        desplazamiento += tamanio_elemento;
        
        // Agregar a la lista
        list_add(elementos, elemento);
        
        if (logger) log_debug(logger, "Elemento %d agregado a la lista", list_size(elementos));
    }
    
    if (logger) {
        log_info(logger, "Paquete deserializado exitosamente - %d elementos, tamaño total: %u bytes", 
                list_size(elementos), buffer_size);
    }
    
    return elementos;
}

t_list* recibir_paquete_mejorado(t_log* logger, int socket_cliente) {
    if (socket_cliente <= 0) {
        if (logger) log_error(logger, "Error: socket inválido: %d", socket_cliente);
        return NULL;
    }
    
    // 1. Recibir tamaño total del buffer
    uint32_t buffer_size_network;
    ssize_t bytes_recv = recv(socket_cliente, &buffer_size_network, sizeof(uint32_t), MSG_WAITALL);
    
    if (bytes_recv <= 0) {
        if (bytes_recv == 0) {
            if (logger) log_warning(logger, "Cliente cerró la conexión");
        } else {
            if (logger) log_error(logger, "Error al recibir tamaño del buffer: %s", strerror(errno));
        }
        return NULL;
    }
    
    uint32_t buffer_size = ntohl(buffer_size_network);
    if (logger) log_info(logger, "Tamaño de buffer recibido: %u bytes", buffer_size);
    
    // Validar tamaño razonable
    if (buffer_size == 0) {
        if (logger) log_warning(logger, "Paquete vacío recibido");
        return list_create();
    }
    
    if (buffer_size > 1000000) { // 1MB máximo
        if (logger) log_error(logger, "Tamaño de buffer excesivamente grande: %u bytes", buffer_size);
        return NULL;
    }
    
    // 2. Recibir buffer completo
    void* buffer = malloc(buffer_size);
    if (buffer == NULL) {
        if (logger) log_error(logger, "Error al allocar %u bytes para buffer", buffer_size);
        return NULL;
    }
    
    bytes_recv = recv(socket_cliente, buffer, buffer_size, MSG_WAITALL);
    
    if (bytes_recv <= 0) {
        if (logger) log_error(logger, "Error al recibir buffer: %s", strerror(errno));
        free(buffer);
        return NULL;
    }
    
    if ((uint32_t)bytes_recv != buffer_size) {
        if (logger) log_error(logger, "Buffer incompleto: %zd/%u bytes", bytes_recv, buffer_size);
        free(buffer);
        return NULL;
    }
    
    if (logger) log_info(logger, "✅ Buffer recibido correctamente (%u bytes)", buffer_size);
    
    // 3. Deserializar usando la nueva función
    t_list* elementos = deserializar_paquete(buffer, buffer_size, logger);
    free(buffer);
    
    if (elementos == NULL) {
        if (logger) log_error(logger, "❌ Error al deserializar paquete");
        return NULL;
    }
    
    if (logger) log_info(logger, "✅ Paquete deserializado - %d elementos", list_size(elementos));
    return elementos;
}