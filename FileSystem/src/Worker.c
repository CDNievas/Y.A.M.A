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

	FILE* archivo=fopen(nombreArchivo,"w+");

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
		if(almacenarArchivo(nombreArchivo,path,"B")==true){
			sendDeNotificacion(socket,ALMACENADO_FINAL_TERMINADO);
		}else{
			sendDeNotificacion(socket,ERROR_ALMACENADO_FINAL);
		}
	}

	if(tipo==22){
		if(almacenarArchivo(nombreArchivo,path,"T")==true){
			sendDeNotificacion(socket,ALMACENADO_FINAL_TERMINADO);
		}else{
			sendDeNotificacion(socket,ERROR_ALMACENADO_FINAL);
		}
	}

	free(path);
	free(nombreArchivo);

}

