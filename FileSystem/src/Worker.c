/*
 * Worker.c
 *
 *  Created on: 25/11/2017
 *      Author: utnso
 */

#include "Worker.h"
#include "../../Biblioteca/src/Socket.h"
#include "FuncionesFS.h"

void almacenarArchivoWorker(int socket){
	uint32_t tipo;
	char* contenido  = recibirString(socket);
	char* nombreArchivo  = recibirString(socket);
	char* path = recibirString(socket);
	tipo = recibirUInt(socket);

	FILE* archivo=fopen(nombreArchivo,"r+");

	if(archivo == NULL){
		free(path);
		free(nombreArchivo);
		free(contenido);
		log_error(loggerFileSystem,"Error al tratar de abrir el archivo al almacenar de Worker.");
		exit(-1);
	}

	fputs(contenido,archivo);
	free(contenido);
	fclose(archivo);

	if(tipo==21){
		almacenarArchivo(nombreArchivo,path,"B");
	}

	if(tipo==22){
		almacenarArchivo(nombreArchivo,path,"T");
	}

	free(path);
	free(nombreArchivo);

}

