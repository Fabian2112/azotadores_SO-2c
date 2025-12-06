#ifndef QUERY_H_
#define QUERY_H_

#include <conexion.h>


// Estructura para la configuración del Query Control
typedef struct {
    char* ip_master;
    int puerto_master;
    char* log_level;
} t_config_query;

// Estructura para información de la query
typedef struct {
    char* archivo_query;
    int prioridad;
    int socket_master;
} t_query_info;

// Funciones principales
void inicializar_query_control(int argc, char* argv[]);
t_config_query* cargar_configuracion(char* path_config);
void conectar_con_master(t_query_info* query_info, t_config_query* config);
void enviar_solicitud_query(t_query_info* query_info);
void esperar_respuestas_master(t_query_info* query_info);
void procesar_mensaje_master(t_query_info* query_info, op_code cod_op);
void liberar_recursos(t_query_info* query_info, t_config_query* config);
void destruir_config_query(t_config_query* config);
void* escuchar_respuestas_master(void* args);
void enviar_nueva_query(char* archivo, int prioridad);

#endif // QUERY_H_