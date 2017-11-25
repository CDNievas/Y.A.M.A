/*
 * estructuras.h
 *
 *  Created on: 22/9/2017
 *      Author: utnso
 */

#include "../../Biblioteca/src/Socket.h"

#ifndef ESTRUCTURAS_H_
#define ESTRUCTURAS_H_

#define TRANSFORMACION 1
#define TRANSFORMACION_TERMINADA 2
#define REPLANIFICAR 3
#define REDUCCION_LOCAL_TERMINADA 4
#define REDUCCION_GLOBAL_TERMINADA 5
#define ERROR_REDUCCION_LOCAL 6
#define ERROR_REDUCCION_GLOBAL 7
#define REDUCCION_LOCAL 8
#define REDUCCION_GLOBAL 9
#define ABORTAR 10
#define EN_PROCESO 11
#define DATOS_NODO 12 //CON ESTO LE PIDO A FS LOS DATOS DE CONEXION DEL NODO
#define INFO_ARCHIVO_FS 13 //CON ESTO LE PIDO A FS LA INFO DEL ARCHIVO
#define FINALIZADO 14
#define ALMACENAMIENTO_FINAL 15
#define CORTO 0
#define FALLO 16
#define FINALIZO 17
#define PATH_FILE_INCORRECTO -14
#define ERROR_ALMACENAMIENTO_FINAL 18

//ESTRUCTURAS PARA ADMINISTRAR LA INFO QUE SE LE MANDA A MASTER
typedef struct{
	char* nombreNodo;
	char* ipNodo;
	uint32_t puertoNodo;
}conexionNodo;

typedef struct{
	conexionNodo* conexion;
	uint32_t nroBloque;
	uint32_t bytesOcupados;
	char* nombreTemporal;
}infoNodo;


//ESTRUCTURA DE ADMINISTRACION DE YAMA
typedef struct{ //AL INICIALIZAR YAMA, CREAR ESTA ESTRUCTURA VACIA (TABLA DE ESTADOS)
	uint32_t nroJob;
	uint32_t nroMaster;
	char* nombreNodo;
	uint32_t nroBloque;
	uint32_t etapa; //UNO DE TRANSFORMACION|REDUCCION_GLOBAL|REDUCCION_LOCAL
	char* nameFile;
	uint32_t estado;//UNO DE EN_PROCESO|FINALIZADO|ERROR
} administracionYAMA;


//ESTRUCTURA PARA LOS DATOS RECIBIDOS DE FS
typedef struct{
	char* nombreNodo;
	uint32_t nroBloque;
}copia;

typedef struct{
	uint32_t nroBloque;
	copia* copia1;
	copia* copia2;
	uint32_t bytesOcupados;
}infoDeFs;


//ESTRUCTURA PARA BALANCEAR LAS CARGAS
typedef struct{
  char* nombreNodo;
  uint32_t availability;
  t_list* bloques; //LISTA DE INTS QUE REPRESENTAN LOS NUMEROS DE BLOQUE
}datosBalanceo;


//ESTRUCTURA PARA ADMINISTRAR TODOS LOS NODOS DEL SISTEMA
typedef struct{
  char* nombreNodo;
  uint32_t wl;
}nodoSistema;

t_log* loggerYAMA;
t_list* tablaDeEstados;
uint32_t socketFS;
bool estaFS;

//SEMAPHORE
pthread_mutex_t semTablaEstados;
pthread_mutex_t semNodosSistema;

//DATOS CONFIG
char* FS_IP;
uint32_t FS_PUERTO;
uint32_t RETARDO_PLANIFICACION;
char* ALGORITMO_BALANCEO;
uint32_t PUERTO_MASTERS;
uint32_t BASE_AVAILABILITY;

//PARA RANDOM NAMES
uint32_t contadorDeJobs;
uint32_t numeroDeTemporalTransformacion;
uint32_t numeroDeTemporalLocal;
uint32_t numeroDeTemporalGlobal;
uint32_t contadorDeMasters;

//LISTA DE NODOS DEL SISTEMA
t_list* nodosSistema;


#endif /* ESTRUCTURAS_H_ */
