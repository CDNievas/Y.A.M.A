/*
 * funcionesYama.c
 *
 *  Created on: 9/12/2017
 *      Author: utnso
 */

#include "funcionesYama.h"

// ENVIAR LISTA NODO
uint32_t sacarTamanioMensaje (){
	uint32_t tamanio=0;
	int cont=0;
	for(;cont<list_size(tablaNodos->listaNodos);cont++){
		tamanio+=sizeof(uint32_t);
		uint32_t tamanioNombreNodo=string_length(list_get(tablaNodos->listaNodos,cont));
		tamanio+=tamanioNombreNodo;
	}
	tamanio+=sizeof(uint32_t);
	return tamanio;
}


void enviarListaNodos(int socket){
	log_info(loggerFileSystem,"Enviado la lista de nodos a YAMA");

	//ARMO EL TAMANIO DEL MENSAJE
	void* mensaje=malloc(sacarTamanioMensaje);
	uint32_t posicionActual=0;
	uint32_t cont=0;

	//AGREGO LA CANTIDAD DE NODOS QUE TIENE EL SISTEMA
	uint32_t cantNodo=list_size(tablaNodos->listaNodos);
	memcpy(mensaje,&cantNodo,sizeof(uint32_t));
	posicionActual+=sizeof(uint32_t);

	//AGREGO LOS NODOS
	for(;cont<list_size(tablaNodos->listaNodos);cont++){
		uint32_t tamanioNodo=string_length(list_get(tablaNodos->listaNodos,cont));
		memcpy(mensaje+posicionActual,&tamanioNodo,sizeof(uint32_t));
		posicionActual+=sizeof(uint32_t);
		memcpy(mensaje+posicionActual,list_get(tablaNodos->listaNodos,cont),tamanioNodo);
		posicionActual+=tamanioNodo;
	}
	uint32_t tamanioMsj=sacarTamanioMensaje();
	sendRemasterizado(socket,ES_FS,tamanioMsj,mensaje);
	free(mensaje);
}

//ENVIAR METADATA DE UN ARCHIVO

int tamanioStruct(strBloqueArchivo* contenido){
	return sizeof(uint32_t)*6 + string_length(contenido->copia1->nodo) + string_length(contenido->copia2->nodo);
}

void* serializarCopiaBloque(strBloqueArchivo* contenido){
	int posicionActual = 0;

	void* datosSerializados = malloc(tamanioStruct(contenido));

	// serializo bloque
	uint32_t nroBloque = contenido->nro;
	memcpy(datosSerializados + posicionActual, &nroBloque, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	//serializo copia1
	uint32_t tamanioCopia1 = string_length(contenido->copia1->nodo);
	memcpy(datosSerializados + posicionActual, &tamanioCopia1, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(datosSerializados + posicionActual, contenido->copia1->nodo, tamanioCopia1);
	posicionActual += tamanioCopia1;
	memcpy(datosSerializados + posicionActual, &contenido->copia1->nroBloque, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	// serializo copia2
	uint32_t tamanioCopia2 = string_length(contenido->copia2->nodo);
	memcpy(datosSerializados + posicionActual, &tamanioCopia2, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(datosSerializados + posicionActual, contenido->copia2->nodo, tamanioCopia2);
	posicionActual += tamanioCopia2;
	memcpy(datosSerializados + posicionActual, &contenido->copia2->nroBloque, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	//serializo bytes
	memcpy(datosSerializados + posicionActual, &contenido->bytes, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);

	return datosSerializados;

}

int sacarTamanio(strArchivo* entradaArchivo){
	uint32_t cont=0;
	uint32_t posicionActual=0;
	strBloqueArchivo* copiaBloque;
	while(cont<list_size(entradaArchivo->bloques)){
		copiaBloque=(strBloqueArchivo*)list_get(entradaArchivo->bloques,cont);
		posicionActual+=tamanioStruct(copiaBloque);
		cont++;
	}
	return posicionActual;
}


void enviarTablaAYama(int socket, strArchivo* entradaArchivo){
	uint32_t cont=0;
	uint32_t posicionActual=0;

	void* mensaje= malloc(sizeof(uint32_t)+sacarTamanio(entradaArchivo));

	uint32_t cantBloques=list_size(entradaArchivo->bloques);
	memcpy(mensaje,&cantBloques,sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);

	while(cont<list_size(entradaArchivo->bloques)){
		strBloqueArchivo* copiaBloque=(strBloqueArchivo*)list_get(entradaArchivo->bloques,cont);
		void* espacioASerializar = serializarCopiaBloque(copiaBloque);
		memcpy(mensaje+posicionActual,espacioASerializar,tamanioStruct(copiaBloque));
		posicionActual+=tamanioStruct(copiaBloque);
		free(espacioASerializar);

		cont++;
	}

	sendRemasterizado(socket, INFO_ARCHIVO_FS, posicionActual, mensaje);
	free(mensaje);
}


void enviarDatoArchivo(int socket){

	//RECIBO EL ARCHIVO
	char* archivoABuscar=recibirString(socket);

	char** ruta = string_split(archivoABuscar, "/");//TENGO QUE LIBERAR ESTO
	//OBTENGO EL NOMBRE DEL ARCHIVO
	char* nombreArchivo = obtenerNombreDirectorio(ruta);
	log_info(loggerFileSystem,"Enviando la metadata del archivo: %s a YAMA.",nombreArchivo);

	//BUSCO SI EL ARCHIVO SE ENCUENTRA EN EL SISTEMA
	bool esElArchivo(strArchivo* archivo){
		return(strcmp(archivo->nombre,nombreArchivo)==0);
	}
	strArchivo* entradaArchivo = list_find(tablaArchivos,(void*)esElArchivo);

	if(entradaArchivo==NULL){
		sendDeNotificacion(socket,ARCHIVO_NO_ENCONTRADO);
		log_error(loggerFileSystem,"Error al enviar la metadata del archivo: %s a YAMA.",nombreArchivo);

	} else {
		enviarTablaAYama(socket,entradaArchivo);
		log_info(loggerFileSystem,"Se ha enviado correctamente la metadata del archivo: %s a YAMA.",nombreArchivo);
	}

	free(archivoABuscar);
	free(nombreArchivo);

}

//ENVIAR DATOS DE CONEXION DE UN NODO
void enviarDatosConexionNodo(int socket){
	//RECIBO EL NODO
	char* nodo=recibirString(socket);

	log_info(loggerFileSystem,"Enviando IP y puerto de nodo: %s a YAMA.",nodo);

	bool esNodo(strConexiones* nodoSeleccionado){
		return(strcmp(nodoSeleccionado->nodo,nodo)==0);
	}
	strConexiones* datosNodo = list_find(listaConexionesNodos,(void*)esNodo);

	void* mensaje=malloc(sizeof(uint32_t)*2+string_length(datosNodo->ip));

	int tamanioIp=string_length(datosNodo->ip);
	int posicionActual=0;

	memcpy(mensaje,&datosNodo->puerto,sizeof(uint32_t));
	posicionActual+=sizeof(uint32_t);

	memcpy(mensaje+posicionActual,&tamanioIp,sizeof(uint32_t));
	posicionActual+=sizeof(uint32_t);

	memcpy(mensaje+posicionActual,datosNodo->ip,tamanioIp);
	posicionActual+=tamanioIp;

	sendRemasterizado(socket,DATOS_NODO,posicionActual,mensaje);
	free(mensaje);
	free(nodo);
}
