/*
 * Estructuras.h
 *
 *  Created on: 25/11/2017
 *      Author: utnso
 */

//#include "../../Biblioteca/src/genericas.h"
//#include "../../Biblioteca/src/Socket.h"
#include <commons/bitarray.h>
#include <sys/mman.h>
#include "../../Biblioteca/src/configParser.h"
#include <readline/chardefs.h>
#include <readline/history.h>



#ifndef ESTRUCTURAS_H_
#define ESTRUCTURAS_H_


// PROTOCOLO
#define REC_INFONODO 100
#define REC_BLOQUE 101
#define ESC_CORRECTA 102
#define ESC_INCORRECTA 103
#define ENV_LEER 104
#define ENV_ESCRIBIR 105
#define INFO_ARCHIVO_FS 13
#define ARCHIVO_NO_ENCONTRADO -14
#define DATOS_NODO 12
#define ALMACENADO_FINAL 15
#define CORTO 0

#define PARAMETROS {"PUERTO_ESCUCHA"}

//------------------DIRECOTORIO------------------------
typedef struct t_directory {
  int index;
  char* nombre;
  int padre;
} t_directory;

//------------------TABLA ARCHIVOS------------------------
typedef struct
	__attribute__((packed)) {
	char* nombreArchivo;
	uint32_t tamanio;
	char* tipo;
	uint32_t disponible;
	int directorioPadre;
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
	copia* copia1;
	copia* copia2;
	uint32_t bytes;
	uint32_t disponible;
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
	uint32_t disponible;
	uint32_t total;
	uint32_t libre;
	uint32_t porcentajeOcioso;
	uint32_t socket;
} contenidoNodo;

//------------------BITMAP------------------------
typedef struct
	__attribute__((packed)) {
	char* nodo;
	t_bitarray *bitarray;
} tablaBitmapXNodos;

//------------------DATOS CONEXION NODO------------------------
typedef struct
	__attribute__((packed)) {
	char* nodo;
	uint32_t puerto;
	char* ip;
} datosConexionNodo;

int PUERTO_ESCUCHA;
char * PATH_METADATA;
char * PATH_BITMAPS;
char * PATH_ARCHIVOS;
char * PATH_PADRE;

bool estaFormateado;
bool seDesconectoUnNodo;

t_log* loggerFileSystem;
bool esEstadoSeguro;
tablaNodos* tablaGlobalNodos;
t_list* listaBitmap;
t_list* tablaGlobalArchivos;
bool hayEstadoAnterior;
t_list* listaConexionNodos;
t_list* listaDirectorios;
t_list* registroArchivos;
int socketClienteChequeado;
int socketEscuchaFS;
pthread_t hiloConsolaFS;

#endif /* ESTRUCTURAS_H_ */
