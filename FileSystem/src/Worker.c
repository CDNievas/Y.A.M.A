/*
 * Worker.c
 *
 *  Created on: 25/11/2017
 *      Author: utnso
 */

#include "Worker.h"

void almacenarArchivoWorker(int socket){
	char* contenido = string_new();
	char* path = string_new();
	char* nombreArchivo = string_new();
	uint32_t tipo;
	contenido = recibirString(socket);
	nombreArchivo = recibirString(socket);
	path = recibirString(socket);
	tipo = recibirUInt(socket);
	FILE* archivo=fopen(nombreArchivo,"r+");
	fputs(contenido,archivo);
	fclose(archivo);
	if(tipo==21){
		almacenarArchivo(nombreArchivo,path,"B");
	}
	if(tipo==22){
		almacenarArchivo(nombreArchivo,path,"T");
	}
}

