#include "worker.h"


int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Uso: %s <config_path> <WORKER_ID>\n", argv[0]);
        fprintf(stderr, "Ejemplo: ./worker worker.config 1\n");
        fprintf(stderr, "Ejemplo: ./worker ../worker.config 2\n");
        fprintf(stderr, "Ejemplo: ./worker ../../worker/worker.config 3\n");
        return EXIT_FAILURE;
    }

    char* cfg = argv[1];
    char* id  = argv[2];

    printf("Iniciando Worker %s...\n", id);
    printf("Archivo de configuración solicitado: %s\n", cfg);
    
    // Mostrar directorio actual y del ejecutable
    char cwd[1024];
    char ejecutable_path[1024];
    char* directorio_ejecutable = NULL;
    
    if (getcwd(cwd, sizeof(cwd)) != NULL) {
        printf("Directorio actual: %s\n", cwd);
    }
    
    // Obtener ruta del ejecutable para búsqueda inteligente
    ssize_t count = readlink("/proc/self/exe", ejecutable_path, sizeof(ejecutable_path)-1);
    if (count != -1) {
        ejecutable_path[count] = '\0';
        directorio_ejecutable = dirname(strdup(ejecutable_path));
        printf("Directorio del ejecutable: %s\n", directorio_ejecutable);
    }

    saludar("worker");

    // Inicializar logger primero con nivel por defecto
    printf("Inicializando logger...\n");
    logger = log_create("worker.log", "Worker", true, LOG_LEVEL_INFO);
    if (logger == NULL) {
        printf("ERROR: No se pudo crear el logger\n");
        if (directorio_ejecutable != NULL) free(directorio_ejecutable);
        return EXIT_FAILURE;
    }
    log_info(logger, "Logger inicializado - Worker ID: %s", id);

    // Cargar configuración con búsqueda dinámica
    printf("Cargando configuración...\n");
    config = iniciar_config(cfg);
    if (config == NULL) {
        log_error(logger, "ERROR: No se pudo cargar la configuración");
        if (directorio_ejecutable != NULL) free(directorio_ejecutable);
        finalizar_worker();
        return EXIT_FAILURE;
    }

    // Ahora inicializar el worker completo con la configuración cargada
    printf("Inicializando worker...\n");
    iniciar_worker(cfg, "worker.log", id);

    // Liberar memoria del directorio del ejecutable si se usó
    if (directorio_ejecutable != NULL) {
        free(directorio_ejecutable);
    }

    printf("=== INICIANDO CONEXIONES ===\n");

    // 1. Conectar al Storage
    printf("Paso 1: Conectando a Storage...\n");
    log_info(logger, "Conectando a Storage...");
    if (conectar_storage() != 0) {
        printf("ERROR: No se pudo conectar al Storage\n");
        log_error(logger, "No se pudo conectar al Storage");
        finalizar_worker();
        return EXIT_FAILURE;
    }
    printf("✓ Conexión con Storage exitosa\n");
    log_info(logger, "Conexión con Storage establecida");

    // 2. Handshake con Storage
    printf("Paso 2: Handshake con Storage...\n");
    log_info(logger, "Realizando handshake con Storage...");
    if (handshake_storage_pedir_blocksize() != 0) {
        printf("ERROR: Handshake con Storage falló\n");
        log_error(logger, "Handshake con Storage falló");
        finalizar_worker();
        return EXIT_FAILURE;
    }
    printf("✓ Handshake con Storage exitoso\n");
    log_info(logger, "Handshake con Storage exitoso - BLOCK_SIZE: %u", WORKER_BLOCK_SIZE);

    // 3. Inicializar memoria
    printf("Paso 3: Inicializando memoria...\n");
    log_info(logger, "Inicializando memoria interna...");
    iniciar_memoria();
    printf("✓ Memoria inicializada\n");
    log_info(logger, "Memoria interna inicializada");

    // 4. Conectar al Master
    printf("Paso 4: Conectando a Master...\n");
    log_info(logger, "Conectando a Master...");
    if (conectar_master() != 0) {
        printf("ERROR: No se pudo conectar al Master\n");
        log_error(logger, "No se pudo conectar al Master");
        finalizar_worker();
        return EXIT_FAILURE;
    }
    printf("✓ Conexión con Master exitosa\n");
    log_info(logger, "Conexión con Master establecida");

    // 5. Handshake con Master
    printf("Paso 5: Handshake con Master...\n");
    log_info(logger, "Realizando handshake con Master...");
    if (handshake_master_enviar_id() != 0) {
        printf("ERROR: Handshake con Master falló\n");
        log_error(logger, "Handshake con Master falló");
        finalizar_worker();
        return EXIT_FAILURE;
    }
    printf("✓ Handshake con Master exitoso\n");
    log_info(logger, "Handshake con Master exitoso");

    printf("=== TODAS LAS CONEXIONES ESTABLECIDAS ===\n");
    log_info(logger, "=== TODAS LAS CONEXIONES ESTABLECIDAS ===");
    log_info(logger, "Worker %s listo para recibir queries", id);
    
    // 6. Quedarse escuchando instrucciones del Master
    printf("Entrando en bucle de escucha del Master...\n");
    log_info(logger, "Iniciando bucle de escucha del Master...");
    bucle_escuchar_master();

    printf("Finalizando worker...\n");
    log_info(logger, "Finalizando worker...");
    finalizar_worker();
    
    printf("Worker finalizado correctamente\n");
    return EXIT_SUCCESS;
}
