#ifndef MEMORY_HELPER_H_
#define MEMORY_HELPER_H_

#include "worker.h"


// ESTRUCTURAS
// Estructura de página (entrada en tabla de páginas)
typedef struct {
    char* file_tag;          // "MATERIAS:BASE"
    uint32_t nro_pagina;     // Número de página lógica
    bool presente;           // Está cargada en memoria?
    bool modificada;         // Bit M (modificada)
    bool usada;              // Bit U (usada) para CLOCK
    uint32_t marco;          // Número de marco físico
    uint64_t last_used;      // Timestamp para LRU
} t_pagina;

// Tabla de páginas por File:Tag
typedef struct {
    char* file_tag;          // Identificador del archivo
    t_list* paginas;         // Lista de t_pagina*
} t_tabla_paginas_interna;

// Estructura principal de la memoria interna
typedef struct {
    void* base_memoria;      // Puntero al malloc()
    uint32_t tamanio;        // Tamaño total
    uint32_t tam_pagina;     // Tamaño de página (= BLOCK_SIZE)
    uint32_t cant_marcos;    // Cantidad de marcos
    uint32_t retardo;        // RETARDO_MEMORIA en ms
    char* algoritmo;         // "LRU" o "CLOCK-M"
    t_list* tablas;          // Lista de t_tabla_paginas_interna*
    t_list* marcos_libres;   // Lista de marcos libres
    uint32_t puntero_clock;  // Para algoritmo CLOCK
} t_memoria_interna;

// FUNCIONES DE INICIALIZACIÓN Y DESTRUCCIÓN
void memory_init(uint32_t tam_memoria, uint32_t tam_pagina, uint32_t retardo, const char* algoritmo);
void memory_destroy(void);

// FUNCIONES DE ACCESO A MEMORIA
void* memory_leer(const char* file_tag, uint32_t nro_pagina);
void memory_escribir(const char* file_tag, uint32_t nro_pagina, void* contenido);
void memory_cargar_pagina(const char* file_tag, uint32_t nro_pagina, t_buffer* buffer);

// FUNCIONES AUXILIARES
t_tabla_paginas_interna* memory_get_tabla(const char* file_tag);
t_pagina* memory_buscar_pagina(const char* file_tag, uint32_t nro_pagina);
void* memory_get_marco_ptr(uint32_t marco);
uint64_t memory_timestamp(void);

// Nueva función para DELETE
void memory_liberar_archivo(const char* file_tag);

#endif
