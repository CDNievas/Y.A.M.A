#include "funcionesSocket.h"
#ifndef SRC_SOCKET_H_
#define SRC_SOCKET_H_
//
//typedef struct __attribute__((__packed__)){
//	int tipoMsj;
//	int tamMsj;
//	void* mensaje;
//}paquete;

int ponerseAEscuchar(int, int);
int aceptarConexion(int);
int conectarServer(char *, int);
int calcularSocketMaximo(int, int);
int recvDeNotificacion(int);
void sendDeNotificacion(int , int );
paquete *recvRemasterizado(int);
void sendRemasterizado(int, int, int, void*);

#endif /* SRC_SOCKET_H_ */
