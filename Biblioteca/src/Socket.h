#include "algunasVariables.h"


#ifndef SRC_SOCKET_H_
#define SRC_SOCKET_H_
//
//typedef struct __attribute__((__packed__)){
//	int tipoMsj;
//	int tamMsj;
//	void* mensaje;
//}paquete;


int ponerseAEscucharClientes(int, int);
int aceptarConexionDeCliente(int);
int conectarAServer(char *, int);
int calcularSocketMaximo(int, int);
int recvDeNotificacion(int);
void sendDeNotificacion(int , int );
void sendRemasterizado(int, int, int, void*);
void destruirPaquete(paquete*);

#endif /* SRC_SOCKET_H_ */
