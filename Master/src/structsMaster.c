#include "../../Biblioteca/src/funcionesSocket.c"

#define sizeMaxMsg 128

// No se que nombre ponerles a las rutas de los archivos temporales.
// Ahora quedan horribles, despues los cambiamos

typedef struct strTransformacion{
	char* nodo;
	char* worker_ip;
	int bloque;
	long int bytesOcupados;
	char* temporalesTransformacion;
} transformacion;


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
