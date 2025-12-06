#ifndef CLIENTE_H_
#define CLIENTE_H_

#include "conexion.h"

// Librerías del sistema necesarias para la implementación


// Declaración de funciones que otros módulos pueden usar

//CLIENTE
void* serializar_paquete(t_paquete* paquete, int bytes);
int crear_conexion(t_log* logger, char *ip, int puerto);
t_paquete* crear_paquete(int code_op, t_log* logger);
void crear_buffer(t_paquete* paquete);
void agregar_a_paquete(t_paquete* paquete, void* valor, int tamanio);
void enviar_paquete(t_paquete* paquete, int socket_cliente);
void liberar_conexion(int socket_cliente);

//void terminar_programa(int conexion, t_log* logger, t_config* config);
//void eliminar_paquete(t_paquete* paquete);
void enviar_mensaje(char* mensaje, int socket_cliente, t_log* logger);


#endif // CLIENTE_H_
