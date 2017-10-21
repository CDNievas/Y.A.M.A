#include "../../Biblioteca/src/configParser.c"
#include "../../Biblioteca/src/Socket.c"
#include <commons/bitarray.h>


#ifndef STRUCTFS_H_
#define STRUCTFS_H_


//------------------DIRECOTORIO------------------------
typedef struct t_directory {
	int index;
	char nombre[255];
	int padre;
} directorio;

//------------------TABLA ARCHIVOS------------------------
typedef struct __attribute__((packed)) {
	uint32_t tamanio;
	char* tipo;
  char* bloque;
	t_list* copias;
} tablaArchivos;

typedef struct __attribute__((packed)) {
	char* nodo;
  uint32_t bloque;
	uint32_t bytes;
} Copia;

//------------------TABLA NODOS------------------------
typedef struct __attribute__((packed)) {
	uint32_t tamanio;
	uint32_t libres;
	t_list* nodo;
	t_list* contenidoXNodo;
} tablaNodos;

typedef struct __attribute__((packed)) {
	char* nodo;
	uint32_t total;
  uint32_t libre;
} contenidoNodo;


//------------------BITMAP------------------------

typedef struct __attribute__((packed)) {
	char* nodo;
	t_bitarray 	*bitarray;
} tablaBitmapXNodos;



#endif /* STRUCTFS_H_ */
