// bitmap.c
#include "bitmap.h"


#define BITS_PER_BYTE 8

// Calcula el tamaño en bytes necesario para almacenar 'bits_count' bits
static size_t calculate_bitmap_size(size_t bits_count) {
    return (bits_count + BITS_PER_BYTE - 1) / BITS_PER_BYTE;
}

// Crea un nuevo bitmap
bitmap_t* bitmap_create(const char* filename, size_t bits_count) {
    
    log_info(logger, "Creando bitmap con %zu bits", bits_count);
    
    bitmap_t* bitmap = malloc(sizeof(bitmap_t));
    if (!bitmap) return NULL;

    size_t byte_size = calculate_bitmap_size(bits_count);
    
    // Crear archivo con tamaño apropiado
    int fd = open(filename, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd == -1) {
        free(bitmap);
        return NULL;
    }

    // Extender el archivo al tamaño necesario
    if (ftruncate(fd, byte_size) == -1) {
        close(fd);
        free(bitmap);
        return NULL;
    }

    // Mapear el archivo en memoria
    uint8_t* data = mmap(NULL, byte_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) {
        close(fd);
        free(bitmap);
        return NULL;
    }

    // Inicializar todos los bits a 0 (libres)
    memset(data, 0, byte_size);

    bitmap->data = data;
    bitmap->size = byte_size;
    bitmap->bits_count = bits_count;
    bitmap->fd = fd;

    log_info(logger, "Bitmap creado exitosamente (%zu bytes en %s)", byte_size, filename);

    return bitmap;
}

// Carga un bitmap existente
bitmap_t* bitmap_load(const char* filename) {
    int fd = open(filename, O_RDWR);
    if (fd == -1) return NULL;

    struct stat st;
    if (fstat(fd, &st) == -1) {
        close(fd);
        return NULL;
    }

    size_t byte_size = st.st_size;
    
    // Mapear el archivo en memoria
    uint8_t* data = mmap(NULL, byte_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) {
        close(fd);
        return NULL;
    }

    bitmap_t* bitmap = malloc(sizeof(bitmap_t));
    if (!bitmap) {
        munmap(data, byte_size);
        close(fd);
        return NULL;
    }

    bitmap->data = data;
    bitmap->size = byte_size;
    bitmap->bits_count = byte_size * BITS_PER_BYTE;
    bitmap->fd = fd;

    return bitmap;
}

// Libera recursos del bitmap
void bitmap_destroy(bitmap_t* bitmap) {
    if (bitmap) {
        munmap(bitmap->data, bitmap->size);
        close(bitmap->fd);
        free(bitmap);
    }
}

// Obtiene el valor de un bit específico
bool bitmap_get(bitmap_t* bitmap, size_t bit_index) {
    if (bit_index >= bitmap->bits_count) return false;
    
    size_t byte_index = bit_index / BITS_PER_BYTE;
    size_t bit_offset = bit_index % BITS_PER_BYTE;
    
    return (bitmap->data[byte_index] >> bit_offset) & 1;
}

// Establece el valor de un bit específico
void bitmap_set(bitmap_t* bitmap, size_t bit_index, bool value) {
    if (bit_index >= bitmap->bits_count) return;
    
    size_t byte_index = bit_index / BITS_PER_BYTE;
    size_t bit_offset = bit_index % BITS_PER_BYTE;
    
    if (value) {
        bitmap->data[byte_index] |= (1 << bit_offset);
    } else {
        bitmap->data[byte_index] &= ~(1 << bit_offset);
    }
}

// Establece un rango de bits
void bitmap_set_range(bitmap_t* bitmap, size_t start, size_t end, bool value) {
    for (size_t i = start; i <= end && i < bitmap->bits_count; i++) {
        bitmap_set(bitmap, i, value);
    }
}

// Encuentra bloques libres contiguos
size_t bitmap_find_free_blocks(bitmap_t* bitmap, size_t count) {
    size_t consecutive_free = 0;
    size_t start_index = 0;
    
    for (size_t i = 0; i < bitmap->bits_count; i++) {
        if (!bitmap_get(bitmap, i)) {
            consecutive_free++;
            if (consecutive_free == count) {
                return start_index;
            }
        } else {
            consecutive_free = 0;
            start_index = i + 1;
        }
    }
    
    return -1; // No se encontraron bloques contiguos
}

// Cuenta bits libres
size_t bitmap_count_free(bitmap_t* bitmap) {
    size_t free_count = 0;
    for (size_t i = 0; i < bitmap->bits_count; i++) {
        if (!bitmap_get(bitmap, i)) {
            free_count++;
        }
    }
    return free_count;
}

// Cuenta bits ocupados
size_t bitmap_count_used(bitmap_t* bitmap) {
    return bitmap->bits_count - bitmap_count_free(bitmap);
}

// Sincroniza el bitmap con el disco
bool bitmap_sync(bitmap_t* bitmap) {
    return msync(bitmap->data, bitmap->size, MS_SYNC) == 0;
}

// Guarda el bitmap (alias de sync)
bool bitmap_save(bitmap_t* bitmap) {
    return bitmap_sync(bitmap);
}