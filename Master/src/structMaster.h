#include "../../Biblioteca/src/Socket.h"

#ifndef STRUCTMASTER_H_
#define STRUCTMASTER_H_

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
#define ALMACENADO_FINAL_TERMINADO 16
#define ERROR_ALMACENADO_FINAL 17
#define ERROR_TRANSFORMACION 18

#define YES 1
#define NO 0


char* YAMA_IP;
char* WORKER_IP;
int YAMA_PUERTO;
int WORKER_PUERTO;
int socketYAMA;
t_log* loggerMaster;

char* scriptTransformador;
char* scriptReduccion;
char* archivoAModificar;
char* pathDondeGuardar;
/*
pthread_mutex_t mutexRespuestaTransformacion;
pthread_mutex_t mutexReplanificacionTransformacion;
*/
t_list * nombresNodos;
t_list * banderasReplanificacion;
t_list * hilosCreados; // Con esto recorro un hilo constantemente
int cantidadDeProcesosNodos;

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

typedef struct{
	conexionNodo* conexion;
	t_list* temporalesTransformacion; 
	char* temporalReduccionLocal;
} infoReduccionLocal;

typedef struct{
	conexionNodo* conexion;
	char* temporalReduccionLocal;
	char* temporalReduccionGlobal;	
} infoEncargadoRG; 

typedef struct{
	conexionNodo* conexion;
	char* temporalReduccionLocal;	
} infoReduccionGlobal; 

typedef struct {
	conexionNodo* conexion;
	char* resultadoReduccionGlobal;
} almacenamientoFinal;


typedef struct {
	pthread_t thread;
	char* nodo;
}infoHilo; // STRUCT PARA SABER QUE HILOS MATAR.

typedef struct{
	char* nodo;
	uint32_t FLAG_REPLANIFICACION;
	uint32_t FLAG_ABORTAR;
}banderasParaNodos; // STRUCT PARA SABER CUANDO ABORTAR O REPLANIFICAR CADA NODO


infoEncargadoRG * encargado;   // Var global donde se sabe quien es el encargado de la reducc global
// ACORDARSE DE HACER EL FREE DE ESTO.


#endif /* STRUCTMASTER_H_ */
