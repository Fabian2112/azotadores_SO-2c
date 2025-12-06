#include "memoryHelper.h"

extern t_log* logger;
static t_memoria_interna* memoria = NULL;

// FUNCIONES DE INICIALIZACIÓN Y DESTRUCCIÓN
void memory_init(uint32_t tam_memoria, uint32_t tam_pagina, uint32_t retardo, const char* algoritmo) {
    memoria = malloc(sizeof(t_memoria_interna));

    memoria->base_memoria = malloc(tam_memoria);
    memoria->tamanio = tam_memoria;
    memoria->tam_pagina = tam_pagina;
    memoria->cant_marcos = tam_memoria / tam_pagina;
    memoria->retardo = retardo;
    memoria->algoritmo = strdup(algoritmo);
    memoria->tablas = list_create();
    memoria->marcos_libres = list_create();
    memoria->puntero_clock = 0;

    for (uint32_t i = 0; i < memoria->cant_marcos; i++)
        list_add(memoria->marcos_libres, (void*)(intptr_t)i);

    if (logger) {
        log_info(logger, "Memoria inicializada: %u bytes, %u marcos de %u, algoritmo=%s", tam_memoria, memoria->cant_marcos, tam_pagina, algoritmo);
    }
}

void memory_destroy(void) {
    if (!memoria) return;

    // Liberar tablas y páginas
    for (int i = 0; i < list_size(memoria->tablas); i++) {
        t_tabla_paginas_interna* tabla = list_get(memoria->tablas, i);
        for (int j = 0; j < list_size(tabla->paginas); j++) {
            t_pagina* p = list_get(tabla->paginas, j);
            free(p->file_tag);
            free(p);
        }
        list_destroy(tabla->paginas);
        free(tabla->file_tag);
        free(tabla);
    }

    list_destroy(memoria->tablas);
    list_destroy(memoria->marcos_libres);

    free(memoria->algoritmo);
    free(memoria->base_memoria);
    free(memoria);
    memoria = NULL;

    log_info(logger, "Memoria interna destruida correctamente.");
}

// FUNCIONES DE TABLAS Y PÁGINAS
t_tabla_paginas_interna* memory_get_tabla(const char* file_tag) {
    // Busco existente
    for (int i = 0; i < list_size(memoria->tablas); i++) {
        t_tabla_paginas_interna* t = list_get(memoria->tablas, i);
        if (strcmp(t->file_tag, file_tag) == 0) 
            return t;
    }

    // Si no existe, creo una nueva
    t_tabla_paginas_interna* nueva = malloc(sizeof(t_tabla_paginas_interna));
    nueva->file_tag = strdup(file_tag);
    nueva->paginas = list_create();
    list_add(memoria->tablas, nueva);
    return nueva;
}

t_pagina* memory_buscar_pagina(const char* file_tag, uint32_t nro_pagina) {
    t_tabla_paginas_interna* tabla = memory_get_tabla(file_tag);
    for (int i = 0; i < list_size(tabla->paginas); i++) {
        t_pagina* p = list_get(tabla->paginas, i);
        if (p->nro_pagina == nro_pagina) 
            return p;
    }

    return NULL;
}

// FUNCIONES DE ACCESO A MEMORIA
void* memory_leer(const char* file_tag, uint32_t nro_pagina) {
    usleep(memoria->retardo * 1000);

    t_pagina* pagina = memory_buscar_pagina(file_tag, nro_pagina);
    if (!pagina || !pagina->presente) {
        log_info(logger, "PAGE FAULT en lectura (%s, pag=%u)", file_tag, nro_pagina);
        // Aquí se invocará luego a memory_cargar_pagina()
        return NULL;
    }

    pagina->last_used = memory_timestamp();
    pagina->usada = true;

    return memory_get_marco_ptr(pagina->marco);
}

void memory_escribir(const char* file_tag, uint32_t nro_pagina, void* contenido) {
    usleep(memoria->retardo * 1000);

    t_pagina* pagina = memory_buscar_pagina(file_tag, nro_pagina);
    if (!pagina || !pagina->presente) {
        log_info(logger, "PAGE FAULT en escritura (%s, pag=%u)", file_tag, nro_pagina);
        // cargamos la página desde storage si hace falta
        return;
    }

    void* ptr = memory_get_marco_ptr(pagina->marco);
    memcpy(ptr, contenido, memoria->tam_pagina);

    pagina->modificada = true;
    pagina->usada = true;
    pagina->last_used = memory_timestamp();
}

// FUNCIONES AUXILIARES
void* memory_get_marco_ptr(uint32_t marco) {
    return ((char*)memoria->base_memoria) + (marco * memoria->tam_pagina);
}

uint64_t memory_timestamp(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000u + (uint64_t)tv.tv_usec / 1000u;
}


// FUNCIONES PARA REEMPLAZO DE PÁGINAS

// Escribir página modificada al Storage antes de desalojarla
static void escribir_pagina_modificada_a_storage(t_pagina* pagina) {
    if (!pagina->modificada) {
        return; // No hace falta escribir
    }

    log_info(logger, "Escribiendo página modificada %s:%u al Storage antes de reemplazo", 
             pagina->file_tag, pagina->nro_pagina);

    // Parsear file_tag en filename y tag
    char* filename;
    char* tag;
    char* copia = strdup(pagina->file_tag);
    char* separador = strchr(copia, ':');
    
    if (separador) {
        *separador = '\0';
        filename = strdup(copia);
        tag = strdup(separador + 1);
    } else {
        filename = strdup(copia);
        tag = strdup("BASE");
    }
    free(copia);

    // Enviar OP_WRITE al Storage
    int cod_op_write = htonl(OP_WRITE);
    send(socket_storage, &cod_op_write, sizeof(int), MSG_NOSIGNAL);

    // Enviar filename
    uint32_t tam_filename = htonl((uint32_t)(strlen(filename) + 1));
    send(socket_storage, &tam_filename, sizeof(uint32_t), MSG_NOSIGNAL);
    send(socket_storage, filename, strlen(filename) + 1, MSG_NOSIGNAL);

    // Enviar tag
    uint32_t tam_tag = htonl((uint32_t)(strlen(tag) + 1));
    send(socket_storage, &tam_tag, sizeof(uint32_t), MSG_NOSIGNAL);
    send(socket_storage, tag, strlen(tag) + 1, MSG_NOSIGNAL);

    // Enviar número de página (= número de bloque)
    uint32_t pagina_network = htonl(pagina->nro_pagina);
    send(socket_storage, &pagina_network, sizeof(uint32_t), MSG_NOSIGNAL);

    // Enviar datos del marco
    void* ptr_marco = memory_get_marco_ptr(pagina->marco);
    uint32_t data_size = memoria->tam_pagina;
    uint32_t data_size_network = htonl(data_size);
    send(socket_storage, &data_size_network, sizeof(uint32_t), MSG_NOSIGNAL);
    send(socket_storage, ptr_marco, data_size, MSG_NOSIGNAL);

    // Esperar respuesta
    int respuesta_network;
    recv(socket_storage, &respuesta_network, sizeof(int), MSG_WAITALL);
    int respuesta = ntohl(respuesta_network);

    if (respuesta == OP_OK) {
        log_info(logger, "✓ Página %s:%u escrita exitosamente al Storage", 
                 pagina->file_tag, pagina->nro_pagina);
        pagina->modificada = false;
    } else {
        log_error(logger, "✗ Error al escribir página %s:%u al Storage", 
                  pagina->file_tag, pagina->nro_pagina);
    }

    free(filename);
    free(tag);
}

// Seleccionar víctima usando LRU
static t_pagina* seleccionar_victima_lru(void) {
    t_pagina* victima = NULL;
    uint64_t min_timestamp = UINT64_MAX;

    // Recorrer TODAS las páginas de TODAS las tablas
    for (int i = 0; i < list_size(memoria->tablas); i++) {
        t_tabla_paginas_interna* tabla = list_get(memoria->tablas, i);
        
        for (int j = 0; j < list_size(tabla->paginas); j++) {
            t_pagina* p = list_get(tabla->paginas, j);
            
            if (p->presente && p->last_used < min_timestamp) {
                min_timestamp = p->last_used;
                victima = p;
            }
        }
    }

    return victima;
}

// Seleccionar víctima usando CLOCK-M
static t_pagina* seleccionar_victima_clock(void) {
    // Crear lista plana de todas las páginas presentes
    t_list* todas_paginas = list_create();
    
    for (int i = 0; i < list_size(memoria->tablas); i++) {
        t_tabla_paginas_interna* tabla = list_get(memoria->tablas, i);
        for (int j = 0; j < list_size(tabla->paginas); j++) {
            t_pagina* p = list_get(tabla->paginas, j);
            if (p->presente) {
                list_add(todas_paginas, p);
            }
        }
    }

    if (list_is_empty(todas_paginas)) {
        list_destroy(todas_paginas);
        return NULL;
    }

    int total_paginas = list_size(todas_paginas);
    int intentos = 0;
    int max_intentos = total_paginas * 2; // Dos vueltas completas
    t_pagina* victima = NULL;

    while (intentos < max_intentos && !victima) {
        // Ajustar puntero si está fuera de rango
        if (memoria->puntero_clock >= total_paginas) {
            memoria->puntero_clock = 0;
        }

        t_pagina* p = list_get(todas_paginas, memoria->puntero_clock);

        // Prioridad 1: página no usada y no modificada
        if (!p->usada && !p->modificada) {
            victima = p;
            memoria->puntero_clock = (memoria->puntero_clock + 1) % total_paginas;
            break;
        }

        // Prioridad 2: página no usada pero modificada
        if (!p->usada && p->modificada) {
            victima = p;
            memoria->puntero_clock = (memoria->puntero_clock + 1) % total_paginas;
            break;
        }

        // Dar segunda oportunidad: limpiar bit de uso
        if (p->usada) {
            p->usada = false;
        }

        memoria->puntero_clock = (memoria->puntero_clock + 1) % total_paginas;
        intentos++;
    }

    // Si no encontramos ninguna después de dos vueltas, tomar la actual
    if (!victima && !list_is_empty(todas_paginas)) {
        victima = list_get(todas_paginas, memoria->puntero_clock);
        memoria->puntero_clock = (memoria->puntero_clock + 1) % total_paginas;
    }

    list_destroy(todas_paginas);
    return victima;
}

// Liberar marco y devolver a la lista de libres
static void liberar_marco(uint32_t marco) {
    list_add(memoria->marcos_libres, (void*)(intptr_t)marco);
}

// Obtener marco libre o aplicar reemplazo
static uint32_t obtener_marco_libre_o_reemplazar(void) {
    // Primero intentar obtener marco libre
    if (!list_is_empty(memoria->marcos_libres)) {
        uint32_t marco = (uint32_t)(intptr_t)list_remove(memoria->marcos_libres, 0);
        log_debug(logger, "Marco %u asignado (estaba libre)", marco);
        return marco;
    }

    // No hay marcos libres, aplicar algoritmo de reemplazo
    log_info(logger, "⚠ MEMORIA LLENA - Aplicando algoritmo de reemplazo: %s", 
             memoria->algoritmo);

    t_pagina* victima = NULL;

    if (strcmp(memoria->algoritmo, "LRU") == 0) {
        victima = seleccionar_victima_lru();
    } else if (strcmp(memoria->algoritmo, "CLOCK") == 0 || 
               strcmp(memoria->algoritmo, "CLOCK-M") == 0) {
        victima = seleccionar_victima_clock();
    } else {
        log_error(logger, "Algoritmo de reemplazo desconocido: %s", memoria->algoritmo);
        return (uint32_t)-1;
    }

    if (!victima) {
        log_error(logger, "No se pudo seleccionar página víctima");
        return (uint32_t)-1;
    }

    log_info(logger, "Página víctima: %s:%u (marco %u, modificada=%s)", 
             victima->file_tag, victima->nro_pagina, victima->marco,
             victima->modificada ? "SÍ" : "NO");

    // Si está modificada, escribir al Storage
    if (victima->modificada) {
        escribir_pagina_modificada_a_storage(victima);
    }

    uint32_t marco_liberado = victima->marco;

    // Marcar página como no presente
    victima->presente = false;
    victima->marco = (uint32_t)-1;
    victima->usada = false;

    log_info(logger, "✓ Marco %u liberado (reemplazo de %s:%u)", 
             marco_liberado, victima->file_tag, victima->nro_pagina);

    return marco_liberado;
}



void memory_cargar_pagina(const char* file_tag, uint32_t nro_pagina, t_buffer* buffer) {
    if (!memoria) {
        log_error(logger, "Memoria no inicializada");
        return;
    }

    // Buscar si ya existe la página
    t_pagina* pagina = memory_buscar_pagina(file_tag, nro_pagina);

    if (pagina && pagina->presente) {
        // Ya está cargada, solo actualizar timestamp
        log_debug(logger, "Página %s:%u ya presente en marco %u", 
                 file_tag, nro_pagina, pagina->marco);
        pagina->last_used = memory_timestamp();
        pagina->usada = true;
        return;
    }

    // Obtener marco (libre o mediante reemplazo)
    uint32_t marco = obtener_marco_libre_o_reemplazar();

    if (marco == (uint32_t)-1) {
        log_error(logger, "CRÍTICO: No se pudo obtener marco para %s:%u", 
                 file_tag, nro_pagina);
        return;
    }

    // Si la página no existe, crearla
    if (!pagina) {
        pagina = malloc(sizeof(t_pagina));
        pagina->file_tag = strdup(file_tag);
        pagina->nro_pagina = nro_pagina;
        pagina->presente = false;
        pagina->modificada = false;
        pagina->usada = false;
        pagina->marco = (uint32_t)-1;
        pagina->last_used = 0;

        t_tabla_paginas_interna* tabla = memory_get_tabla(file_tag);
        list_add(tabla->paginas, pagina);
    }

    // Copiar datos al marco
    void* ptr_marco = memory_get_marco_ptr(marco);
    
    if (buffer && buffer->stream && buffer->size > 0) {
        size_t copy_size = (buffer->size < memoria->tam_pagina) ? 
                          buffer->size : memoria->tam_pagina;
        memcpy(ptr_marco, buffer->stream, copy_size);

        // Llenar resto con ceros si es necesario
        if (copy_size < memoria->tam_pagina) {
            memset((char*)ptr_marco + copy_size, 0, memoria->tam_pagina - copy_size);
        }
    } else {
        memset(ptr_marco, 0, memoria->tam_pagina);
    }

    // Actualizar metadata de la página
    pagina->presente = true;
    pagina->marco = marco;
    pagina->modificada = false;
    pagina->usada = true;
    pagina->last_used = memory_timestamp();

    log_info(logger, "✓ Página %s:%u cargada en marco %u", file_tag, nro_pagina, marco);
}


// NUEVA FUNCIÓN (para DELETE)
void memory_liberar_archivo(const char* file_tag) {
    if (!memoria) return;

    log_info(logger, "Liberando páginas de: %s", file_tag);

    // Buscar la tabla correspondiente
    t_tabla_paginas_interna* tabla = NULL;
    int tabla_index = -1;

    for (int i = 0; i < list_size(memoria->tablas); i++) {
        t_tabla_paginas_interna* t = list_get(memoria->tablas, i);
        if (strcmp(t->file_tag, file_tag) == 0) {
            tabla = t;
            tabla_index = i;
            break;
        }
    }

    if (!tabla) {
        log_debug(logger, "No hay páginas de %s en memoria", file_tag);
        return;
    }

    int liberadas = 0;

    // Liberar todas las páginas de esta tabla
    for (int i = list_size(tabla->paginas) - 1; i >= 0; i--) {
        t_pagina* pagina = list_get(tabla->paginas, i);

        if (pagina->presente) {
            // NO escribir al Storage porque el archivo fue eliminado
            // Devolver marco a la lista de libres
            liberar_marco(pagina->marco);
            log_debug(logger, "Marco %u liberado (%s:%u)", 
                     pagina->marco, file_tag, pagina->nro_pagina);
        }

        // Liberar página
        free(pagina->file_tag);
        list_remove(tabla->paginas, i);
        free(pagina);
        liberadas++;
    }

    // Eliminar tabla si está vacía
    list_destroy(tabla->paginas);
    free(tabla->file_tag);
    list_remove(memoria->tablas, tabla_index);
    free(tabla);

    log_info(logger, "✓ Liberadas %d páginas de %s", liberadas, file_tag);
}