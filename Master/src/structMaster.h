#include "../../Biblioteca/src/Socket.c"
#include "../../Biblioteca/src/configParser.c"

#ifndef STRUCTMASTER_H_
#define STRUCTMASTER_H_


#define sizeMaxMsg 128

// No se que nombre ponerles a las rutas de los archivos temporales.
// Ahora quedan horribles, despues los cambiamos

typedef struct{
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

typedef struct strReduccionLocal{
	char* nodo;
	char* worker_ip;
	char* temporalesTransformacion; // Pensaba hacer un array de char*. Cada posicion seria una direccion de archivo temporal de transformacion
	char* temporalesReduccionLocal;
} reduccionLocal ;

typedef struct strReduccionGlobal{
	char* nodo;
	char* worker_ip;
	char* temporalesReduccionLocal;
	int encargado; // 1 si es encargado, 0 si no.
} reduccionGlobal;


#endif /* STRUCTMASTER_H_ */
