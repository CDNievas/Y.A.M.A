#include "../../Biblioteca/src/Socket.h"

#ifndef STRUCTMASTER_H_
#define STRUCTMASTER_H_


#define sizeMaxMsg 128

char* YAMA_IP;
char* WORKER_IP;
int YAMA_PUERTO;
int WORKER_PUERTO;
int socketYAMA;
t_log* loggerMaster;

// No se que nombre ponerles a las rutas de los archivos temporales.
// Ahora quedan horribles, despues los cambiamos

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
	conexionNodo * conexion;
	t_list* temporalesTransformacion; 
	char* temporalReduccionLocal;
} infoReduccionLocal;

typedef struct{
	conexionNodo * conexion;
	char* temporalesReduccionLocal;
	uint32_t encargado; // 1 si es encargado, 0 si no.
} infoReduccionLocalGlobal;


#endif /* STRUCTMASTER_H_ */
