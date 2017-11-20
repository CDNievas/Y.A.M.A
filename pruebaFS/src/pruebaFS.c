/*
 ============================================================================
 Name        : pruebaFS.c
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
	int socket = ponerseAEscucharClientes(5010, 0);
	while(1){
		int yama = aceptarConexionDeCliente(socket);
		int quienEs= recvDeNotificacion(yama);
		sendDeNotificacion(yama, 1);
		char* nombre = string_new();
		string_append(&nombre, "NODO1");
		void* mensaje = malloc(string_length(nombre)+sizeof(int));
		int tamanioNombre = string_length(nombre)+1;
		memcpy(mensaje, &tamanioNombre, sizeof(int));
		memcpy(mensaje+sizeof(int), nombre, tamanioNombre);
		sendRemasterizado(yama, ES_FS, tamanioNombre+sizeof(int), mensaje);
		int transformacion = recvDeNotificacion(yama);
		printf("%d", transformacion);

		char* nombrepepito = recibirString(yama);
		printf("%s", nombrepepito);

		int cantidadBloques = 1;
		int numeroBloque = 1;
		int numeroBloqueEnC1 = 1;
		int numeroBloqueEnC2 = 1;
		int bytesOcupados = 10;
		int tamNombre = string_length(nombre);

		void* bufferTransformacion = malloc(sizeof(int)*7+string_length(nombre)*2);
		memcpy(bufferTransformacion, &cantidadBloques, sizeof(int));
		memcpy(bufferTransformacion+sizeof(int), &numeroBloque, sizeof(int));
		memcpy(bufferTransformacion+sizeof(int)*2, &tamNombre, sizeof(int));
		memcpy(bufferTransformacion+sizeof(int)*3, nombre, tamNombre);
		memcpy(bufferTransformacion+sizeof(int)*3+string_length(nombre), &numeroBloqueEnC1, sizeof(int));
		memcpy(bufferTransformacion+sizeof(int)*4+string_length(nombre), &tamNombre, sizeof(int));
		memcpy(bufferTransformacion+sizeof(int)*5+string_length(nombre), nombre, tamNombre);
		memcpy(bufferTransformacion+sizeof(int)*5+string_length(nombre)*2, &numeroBloqueEnC2, sizeof(int));
		memcpy(bufferTransformacion+sizeof(int)*6+string_length(nombre)*2, &bytesOcupados, sizeof(int));

		sendRemasterizado(yama, 13, sizeof(int)*7+string_length(nombre)*2, bufferTransformacion);

		void* bufferIPsYPuertos;
		uint32_t puerto = 5050;
		char* ip = string_new();
		ip = "127.0.0.1";
		int tamanioIP = string_length(ip);
		bufferIPsYPuertos = malloc(sizeof(uint32_t)*2+string_length(ip));
		memcpy(bufferIPsYPuertos, &puerto, sizeof(uint32_t));
		memcpy(bufferIPsYPuertos+sizeof(uint32_t), &tamanioIP, sizeof(int));
		memcpy(bufferIPsYPuertos+sizeof(uint32_t)*2, ip, tamanioIP);

		sendRemasterizado(yama, 12, sizeof(uint32_t)*2+tamanioIP, bufferIPsYPuertos);

		void* bufferReducPuertos = malloc(sizeof(uint32_t)*2+string_length(ip));
		bufferIPsYPuertos = malloc(sizeof(uint32_t)*2+string_length(ip));
		memcpy(bufferReducPuertos, &puerto, sizeof(uint32_t));
		memcpy(bufferReducPuertos+sizeof(uint32_t), &tamanioIP, sizeof(int));
		memcpy(bufferReducPuertos+sizeof(uint32_t)*2, ip, tamanioIP);

		sendRemasterizado(yama, 12, sizeof(uint32_t)*2+tamanioIP, bufferReducPuertos);
	}
}
