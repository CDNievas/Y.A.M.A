#include "../../Biblioteca/src/configParser.c"
#include "../../Biblioteca/src/Socket.c"
#include <commons/bitarray.h>
#include <sys/mman.h>

#ifndef STRUCTFS_H_
#define STRUCTFS_H_

// PROTOCOLO
#define REC_INFONODO 100
#define REC_BLOQUE 101
#define ESC_CORRECTA 102
#define ESC_INCORRECTA 103
#define ENV_LEER 104
#define ENV_ESCRIBIR 105
#define INFO_ARCHIVO_FS 13

//------------------DIRECOTORIO------------------------
typedef struct t_directory {
	int index;
	char nombre[255];
	int padre;
} directorio;

//------------------TABLA ARCHIVOS------------------------
typedef struct
	__attribute__((packed)) {
	char* nombreArchivo;
	uint32_t tamanio;
	char* tipo;
	t_list* bloques;
} tablaArchivos;

typedef struct
	__attribute__((packed)) {
	char* nodo;
	uint32_t bloque;
} copia;

typedef struct
	__attribute__((packed)) {
	char* bloque;
	copia * copia1;
	copia * copia2;
	uint32_t bytes;
} copiasXBloque;

//------------------TABLA NODOS------------------------
typedef struct
	__attribute__((packed)) {
	uint32_t tamanio;
	uint32_t libres;
	t_list* nodo;
	t_list* contenidoXNodo;
} tablaNodos;

typedef struct
	__attribute__((packed)) {
	char* nodo;
	uint32_t total;
	uint32_t libre;
} contenidoNodo;

//------------------BITMAP------------------------
typedef struct
	__attribute__((packed)) {
	char* nodo;
	t_bitarray *bitarray;
} tablaBitmapXNodos;

//-------------------CONEXION DE NODOS---------
typedef struct	__attribute__((packed)) {
	char* nodo;
	uint32_t soket;
} nodoConexion;

#endif /* STRUCTFS_H_ */
