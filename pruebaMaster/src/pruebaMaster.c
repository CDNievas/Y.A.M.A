/*
 ============================================================================
 Name        : pruebaMaster.c
 Author      : 
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include "../../Biblioteca/src/Socket.c"

int main(void) {
	int socketYama = conectarAServer("127.0.0.1", 5050);
	sendDeNotificacion(socketYama, ES_MASTER);
	int quienEs = recvDeNotificacion(socketYama);
	printf("%d", quienEs);

	void *bufferTransformacion;
	char* pepitotxt = string_new();
	string_append(&pepitotxt, "pepito.txt");
	int tamaniopepito = string_length(pepitotxt);
	bufferTransformacion = malloc(sizeof(int)+tamaniopepito);
	memcpy(bufferTransformacion, &tamaniopepito, sizeof(int));
	memcpy(bufferTransformacion+sizeof(int), pepitotxt, tamaniopepito);
	sendRemasterizado(socketYama, 1, tamaniopepito+sizeof(int), bufferTransformacion);

	int nroBloque = 1;
	char* nombreNodo = string_new();
	nombreNodo = "NODO1";
	int tamanioNombre = string_length(nombreNodo);

	void* bufferReducLocal = malloc(sizeof(int)*2+tamanioNombre);
	memcpy(bufferReducLocal, &tamanioNombre, sizeof(int));
	memcpy(bufferReducLocal+sizeof(int), nombreNodo, tamanioNombre);
	memcpy(bufferReducLocal+tamanioNombre+sizeof(int), &nroBloque, sizeof(int));

	sendRemasterizado(socketYama, 2, tamanioNombre+sizeof(int)*2, bufferReducLocal);
	while(1){

	}
}
