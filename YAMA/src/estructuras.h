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
#define ERROR_TRANSFORMACION 3
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

typedef struct
{
	char* nombreNodo;
	char* ipNodo;
	int puertoNodo;
}conexionNodo;

typedef struct{
	conexionNodo* conexion;
	int nroBloque;
	long bytesOcupados;
	char* nombreTemporal;
}infoNodo;

typedef struct{ //AL INICIALIZAR YAMA, CREAR ESTA ESTRUCTURA VACIA (TABLA DE ESTADOS)
	int nroJob;
	int nroMaster;
	char* nombreNodo;
	int nroBloque;
	int etapa; //UNO DE TRANSFORMACION|REDUCCION_GLOBAL|REDUCCION_LOCAL
	char* nameFile;
	int estado;//UNO DE EN_PROCESO|FINALIZADO|ERROR
} administracionYAMA;

typedef struct{
	char* nombreNodo;
	int nroBloque;
}copia;

typedef struct{
	int nroBloque;
	copia* copia1;
	copia* copia2;
	int bytesOcupados;
}infoDeFs;

t_log* loggerYAMA;
t_list* tablaDeEstados;
int socketFS;

//DATOS CONFIG
char* FS_IP;
int FS_PUERTO;
int RETARDO_PLANIFICACION;
char* ALGORITMO_BALANCEO;
int PUERTO_MASTERS;

//PARA RANDOM NAMES
int contadorDeJobs;
int numeroDeTemporalTransformacion;
int numeroDeTemporalLocal;
int numeroDeTemporalGlobal;
int contadorDeMasters;


#endif /* ESTRUCTURAS_H_ */
