/*
 * filesystem.h
 *
 *  Created on: 3/12/2017
 *      Author: utnso
 */

#ifndef ESTRUCTURAS_H_
#define ESTRUCTURAS_H_

#include <commons/collections/list.h>
#include <commons/bitarray.h>
#include <commons/log.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include "../../Biblioteca/src/configParser.h"
#include "../../Biblioteca/src/Socket.h"
#include <commons/string.h>
#include <string.h>
#include "../../Biblioteca/src/algunasVariables.h"
#include <signal.h>
#include <commons/config.h>

// Tabla Directorios
typedef struct _directorio{
	int index;
	char* nombre;
	int padre;
} strDirectorio;


// Tabla Nodos
typedef struct _tablaNodos{
	uint32_t tamanioFSTotal;
	uint32_t tamanioFSLibre;
	t_list* listaNodos;
	t_list * nodos;
} strTablaNodos;

typedef struct _nodo{
	char * nombre;
	int socket;
	uint32_t porcentajeOscioso;
	uint32_t tamanioTotal;
	uint32_t tamanioLibre;
	bool conectado;
} strNodo;


// Tabla Archivos
typedef struct _archivo{
	char * nombre;
	uint32_t tamanio;
	char * tipo;
	bool disponible;
	uint32_t directorioPadre;
	t_list* bloques;
} strArchivo;

typedef struct _copia{
	char * nodo;
	u_int32_t nroBloque;
} strCopiaArchivo;

typedef struct _bloqueArchivo{
	uint32_t nro;
	uint32_t bytes;
	strCopiaArchivo * copia1;
	strCopiaArchivo * copia2;
	bool disponible;
} strBloqueArchivo;

//tabla de bitmap
typedef struct _bitmaps{
	char * nodo;
	t_bitarray * bitarray;
} strBitmaps;

//tabla de conexiones
typedef struct _conexiones{
	char * nodo;
	uint32_t puerto;
	char* ip;
} strConexiones;


// Protocolo
#define PEDIR_INFONODO 99
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
#define ALMACENADO_FINAL_TERMINADO 17
#define ERROR_ALMACENADO_FINAL 16
#define DESCONECTAR_NODO 106

// Variables archivo de configuracion
int PUERTO_ESCUCHA;
char * PATH_METADATA;

t_log * loggerFileSystem;
int socketListener,socketMaximo;
fd_set socketClientes, socketClientesAuxiliares;
t_list * tablaArchivos;
strTablaNodos * tablaNodos;
t_list * tablaDirectorios;
t_list * listaBitmaps; // liberado
t_list * socketsDatanode; // Solo para uso antes de format, no se puede liberar
t_list * listaConexionesNodos; //liberado
t_list * listaRegistroDeArchivosGuardados;//falta liberar
pthread_t hiloConsolaFS;
pthread_mutex_t mutex;

bool estadoSeguro,sistemaFormateado,estadoAnterior,seDesconectoUnNodo,envioDeInformacionADataNode;
char * commandChar;

//MUTEX
pthread_mutex_t mutexEnvioANodos;

#endif /* ESTRUCTURAS_H_ */
