#include "Socket.h"
#include <commons/string.h>

void verificarErrorSocket(int unSocket) {
	if (unSocket == -1) {
		perror("Error de socket");
		close(unSocket);
		exit(-1);
	}
}

void verificarErrorSetsockopt(int unSocket) {
	int yes = 1;
	if (setsockopt(unSocket, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) {
		perror("Error de setsockopt");
		close(unSocket);
		exit(-1);
	}
}

void verificarErrorBind(int unSocket, struct sockaddr_in mySocket) {
	if (bind(unSocket, (struct sockaddr *) &mySocket, sizeof(mySocket)) == -1) {
		perror("Error de bind");
		close(unSocket);
		exit(-1);
	}
}

void verificarErrorListen(int unSocket) {
	if (listen(unSocket, BACKLOG) == -1) {
		perror("Error de listen");
		close(unSocket);
		exit(-1);
	}
}

int ponerseAEscucharClientes(int puerto, int protocolo) {
	struct sockaddr_in mySocket;
	int socketListener = socket(AF_INET, SOCK_STREAM, protocolo);
	verificarErrorSocket(socketListener);
	verificarErrorSetsockopt(socketListener);
	mySocket.sin_family = AF_INET;
	mySocket.sin_port = htons(puerto);
	mySocket.sin_addr.s_addr = INADDR_ANY;
	memset(&(mySocket.sin_zero), '\0', 8);
	verificarErrorBind(socketListener, mySocket);
	verificarErrorListen(socketListener);
	return socketListener;
}

int aceptarConexionDeCliente(int socketListener) {
	int socketAceptador;
	struct sockaddr_in su_addr;
	socklen_t sin_size;
	sin_size = sizeof(struct sockaddr_in);

	if ((socketAceptador = accept(socketListener, (struct sockaddr *) &su_addr,&sin_size)) == -1) {
		perror("Error al aceptar conexion");
		close(socketListener);
		exit(-1);
	}

	return socketAceptador;
}

int conectarAServer(char *ip, int puerto) { //Recibe ip y puerto, devuelve socket que se conecto

	int socket_server = socket(AF_INET, SOCK_STREAM, 0);
	struct hostent *infoDelServer;
	struct sockaddr_in direccion_server; // información del server

	//Obtengo info del server
	if ((infoDelServer = gethostbyname(ip)) == NULL) {
		perror("Error al obtener datos del server.");
		close(socket_server);
		exit(-1);
	}

	//Guardo datos del server
	direccion_server.sin_family = AF_INET;
	direccion_server.sin_port = htons(puerto);
	direccion_server.sin_addr = *(struct in_addr *) infoDelServer->h_addr; //h_addr apunta al primer elemento h_addr_lista
	memset(&(direccion_server.sin_zero), 0, 8);

	//Conecto con servidor, si hay error finalizo
	if (connect(socket_server, (struct sockaddr *) &direccion_server,sizeof(struct sockaddr)) == -1) {
		perror("Error al conectar con el servidor.");
		close(socket_server);
		exit(-1);
	}

	return socket_server;

}

int conectarAWorker(char *ip, int puerto) { //Recibe ip y puerto, devuelve socket que se conecto

	int socket_server = socket(AF_INET, SOCK_STREAM, 0);
	struct hostent *infoDelServer;
	struct sockaddr_in direccion_server; // información del server

	//Obtengo info del server
	if ((infoDelServer = gethostbyname(ip)) == NULL) {
		perror("Error al obtener datos del server.");
		socket_server = -1;
	}

	//Guardo datos del server
	direccion_server.sin_family = AF_INET;
	direccion_server.sin_port = htons(puerto);
	direccion_server.sin_addr = *(struct in_addr *) infoDelServer->h_addr; //h_addr apunta al primer elemento h_addr_lista
	memset(&(direccion_server.sin_zero), 0, 8);

	//Conecto con servidor, si hay error finalizo
	if (connect(socket_server, (struct sockaddr *) &direccion_server,sizeof(struct sockaddr)) == -1) {
		perror("Error al conectar con el servidor.");
		socket_server = -1;
	}

	return socket_server;

}

int calcularSocketMaximo(int socketNuevo, int socketMaximoPrevio){
	if(socketNuevo>socketMaximoPrevio){
		return socketNuevo;
	}
	else{
		return socketMaximoPrevio;
	}
}

int calcularTamanioTotalPaquete(int tamanioMensaje){
  int tamanio = sizeof(int)*2 + tamanioMensaje;
  return tamanio;
}

void sendRemasterizado(int aQuien, int tipoMsj, int tamanioMsj, void* peticionDeArchivo){
	void* bufferMensaje = malloc(tamanioMsj+sizeof(tipoMsj));
	memcpy(bufferMensaje, &tipoMsj, sizeof(int));
	memcpy(bufferMensaje+sizeof(int), peticionDeArchivo, tamanioMsj);
	if(send(aQuien, bufferMensaje, tamanioMsj+sizeof(int), 0) == -1){
		perror("Error al enviar mensaje.");
		exit(-1);
	}
	free(bufferMensaje);
}

void sendDeNotificacion(int aQuien, int notificacion){
	if(send(aQuien, &notificacion, sizeof(int),0)==-1){
		perror("Error al enviar notificacion.");
		exit(-1);
	}
}

uint32_t recibirUInt(int socket){
	uint32_t uintRecibido;
	if(recv(socket, &uintRecibido, sizeof(uint32_t), 0) == -1){
		perror("Error al recibir un uint.");
		exit(-1);
	}
	return uintRecibido;
}

char* recibirString(int socket){ //EL TAMAÑO DEL STRING SE RECIBE ADENTRO DE ESTA FUNCION xd
	uint32_t tamanio = recibirUInt(socket);
	void* string = malloc(tamanio);
	if(recv(socket, string, tamanio, MSG_WAITALL) == -1){
		perror("Error al recibir un string.");
		exit(-1);
	}
	char* stringRecibido = string_substring_until(string, tamanio);
	free(string);
	return stringRecibido;
}

int recvDeNotificacion(int deQuien){
	int notificacion;
	int resultadoRecv = recv(deQuien, &notificacion, sizeof(int), 0);
	if(resultadoRecv <= 0){
		return 0;
	}
	return notificacion;
}

void destruirPaquete(paquete* paqueteADestruir){
    free(paqueteADestruir->mensaje);
    free(paqueteADestruir);
}
