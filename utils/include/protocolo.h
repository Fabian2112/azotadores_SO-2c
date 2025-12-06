#ifndef PROTOCOLO_H_
#define PROTOCOLO_H_


// Enumeración específica para instrucciones (puede usarse para CPU - Kernel)
typedef enum {
    INSTRUCCION_NO_OP,         // No Operation
    INSTRUCCION_WRITE,         // Write: Dirección, Datos
    INSTRUCCION_READ,          // Read: Dirección, Tamaño
    INSTRUCCION_GOTO,          // GOTO: Valor
    INSTRUCCION_IO,            // Syscall IO: Dispositivo, Tiempo
    INSTRUCCION_INIT_PROC,     // Syscall INIT_PROC: Archivo de instrucciones, Tamaño
    INSTRUCCION_DUMP_MEMORY,   // Syscall DUMP_MEMORY
    INSTRUCCION_EXIT,           // Syscall EXIT
    INSTRUCCION_COMPLETA,     // Instrucción completa
} instruccion_code;

#endif // PROTOCOLO_H_
