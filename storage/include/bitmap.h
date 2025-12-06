// bitmap.h
#ifndef BITMAP_H
#define BITMAP_H

#include "storage.h"

// Funciones públicas
bitmap_t* bitmap_create(const char* filename, size_t bits_count);
bitmap_t* bitmap_load(const char* filename);
void bitmap_destroy(bitmap_t* bitmap);

bool bitmap_get(bitmap_t* bitmap, size_t bit_index);
void bitmap_set(bitmap_t* bitmap, size_t bit_index, bool value);
void bitmap_set_range(bitmap_t* bitmap, size_t start, size_t end, bool value);

size_t bitmap_find_free_blocks(bitmap_t* bitmap, size_t count);
size_t bitmap_count_free(bitmap_t* bitmap);
size_t bitmap_count_used(bitmap_t* bitmap);

bool bitmap_save(bitmap_t* bitmap);
bool bitmap_sync(bitmap_t* bitmap);


#endif