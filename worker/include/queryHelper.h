#ifndef QUERY_HELPER_H
#define QUERY_HELPER_H

#include "worker.h"


// Tipos de instrucciones (renombrado para evitar conflicto)
typedef enum {
    QUERY_INST_INVALID = -1,
    QUERY_INST_CREATE,
    QUERY_INST_TRUNCATE,
    QUERY_INST_WRITE,
    QUERY_INST_READ,
    QUERY_INST_TAG,
    QUERY_INST_COMMIT,
    QUERY_INST_FLUSH,
    QUERY_INST_DELETE,
    QUERY_INST_END
} t_query_instruccion;

typedef enum {
    ERROR_CRITICO,      // Detiene la query
    ERROR_NO_CRITICO    // Continúa la ejecución
} t_tipo_error;

// Declaraciones de funciones
bool ejecutar_instruccion(uint32_t id, const char* line, uint32_t pc);
void enviar_mensaje_storage(op_code codigo, void* buffer, size_t size, int socket);
bool ejecutar_END(uint32_t id);

#endif