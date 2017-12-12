/*
 * funcionesWorker.c
 *
 *  Created on: 9/12/2017
 *      Author: utnso
 */

#include "funcionesWorker.h"

void almacenarArchivoWorker (int socket){
	log_debug(loggerFileSystem,"Se ha conectado un worker al sistema para el almacenado final.");
	char* contenidoArchivo =recibirString(socket);
	char* nombreArchivo=recibirString(socket);
	char* pathDestino=recibirString(socket);
	uint32_t tipo=recibirUInt(socket);

	FILE* archivoWorker=fopen(nombreArchivo,"w+");

	if(archivoWorker==NULL){
		log_error(loggerFileSystem,"Error al guardar el contenido del worker.");
		free(pathDestino);
		free(contenidoArchivo);
		free(nombreArchivo);
		fclose(archivoWorker);
		return ;
	}

	if(tipo!= 21 && tipo!=22){
		log_error(loggerFileSystem,"Error de tipo de archivo.");
		free(pathDestino);
		free(contenidoArchivo);
		free(nombreArchivo);
		fclose(archivoWorker);
		return ;
	}

	fputs(contenidoArchivo,archivoWorker);
	fclose(archivoWorker);
	free(contenidoArchivo);

	log_trace(loggerFileSystem,"Se procede a almacenar el archivo...");
	if(tipo==21){
		if(almacenarArchivo(nombreArchivo,pathDestino,"B")==true){
			sendDeNotificacion(socket,ALMACENADO_FINAL_TERMINADO);
			log_trace(loggerFileSystem,"El almacenado final ha terminado correctamente.");
		}else{
			sendDeNotificacion(socket,ERROR_ALMACENADO_FINAL);
		}
	}

	if(tipo==22){
		if(almacenarArchivo(nombreArchivo,pathDestino,"T")==true){
			sendDeNotificacion(socket,ALMACENADO_FINAL_TERMINADO);
			log_trace(loggerFileSystem,"El almacenado final ha terminado correctamente.");
		}else{
			sendDeNotificacion(socket,ERROR_ALMACENADO_FINAL);
		}
	}

	free(nombreArchivo);
	free(pathDestino);

}
