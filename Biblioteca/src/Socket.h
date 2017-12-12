#include "algunasVariables.h"

#ifndef SRC_SOCKET_H_
#define SRC_SOCKET_H_
//
//typedef struct __attribute__((__packed__)){
//	int tipoMsj;
//	int tamMsj;
//	void* mensaje;
//}paquete;

void verificarErrorSocket(int);
void verificarErrorSetsockopt(int);
void verificarErrorListen(int);
void verificarErrorBind(int, struct sockaddr_in);
int ponerseAEscucharClientes(int, int);
int aceptarConexionDeCliente(int);
int conectarAServer(char *, int);
int calcularSocketMaximo(int, int);
int recvDeNotificacion(int);
void sendDeNotificacion(int , int );
void sendRemasterizado(int, int, int, void*);
uint32_t recibirUInt(int);
char* recibirString(int);
char* recibirStringModificado(int);
int calcularSocketMaximo(int, int);
void destruirPaquete(paquete*);
int calcularTamanioTotalPaquete(int);

#endif /* SRC_SOCKET_H_ */
