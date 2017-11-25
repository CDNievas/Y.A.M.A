/*
 * YAMA.c
 *
 *  Created on: 25/11/2017
 *      Author: utnso
 */

#include "YAMA.h"

int sacarTamanioMensaje() {
	int tamanio = 0;
	int i = 0;
	for (; i < list_size(tablaGlobalNodos->nodo); i++) {
		tamanio += sizeof(int);
		int tamanioNodo = string_length(list_get(tablaGlobalNodos->nodo, i));
		tamanio += tamanioNodo;
	}
	tamanio+=sizeof(uint32_t);
	return tamanio;
}

void enviarListaNodos(int socket) {
	void* mensaje = malloc(sacarTamanioMensaje());
	int posicionActual = 0;
	int i = 0;
	int cantNodo=list_size(tablaGlobalNodos->nodo);
	memcpy(mensaje,&cantNodo,sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	for (; i < list_size(tablaGlobalNodos->nodo); i++) {
		int tamanioNodo = string_length(list_get(tablaGlobalNodos->nodo, i));
		memcpy(mensaje+posicionActual, &tamanioNodo, sizeof(int));
		posicionActual += sizeof(int);
		memcpy(mensaje + posicionActual, list_get(tablaGlobalNodos->nodo, i),
				tamanioNodo);
		posicionActual += tamanioNodo;
	}
	int tamanioMsj = sacarTamanioMensaje();
	sendRemasterizado(socket, ES_FS, tamanioMsj, mensaje);
	free(mensaje);
}

//ENVIAR DATOS

int tamanioStruct(copiasXBloque* contenido){
	return sizeof(uint32_t)*6 + string_length(contenido->copia1->nodo) + string_length(contenido->copia2->nodo);
}

void* serializarCopiaBloque(copiasXBloque* contenido){
	int posicionActual = 0;

	void* datosSerializados = malloc(tamanioStruct(contenido));

	// serializo bloque
	uint32_t nroBloque = atoi(contenido->bloque);
	memcpy(datosSerializados + posicionActual, &nroBloque, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	//serializo copia1
	uint32_t tamanioCopia1 = string_length(contenido->copia1->nodo);
	memcpy(datosSerializados + posicionActual, &tamanioCopia1, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(datosSerializados + posicionActual, contenido->copia1->nodo, tamanioCopia1);
	posicionActual += tamanioCopia1;
	memcpy(datosSerializados + posicionActual, &contenido->copia1->bloque, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	// serializo copia2
	uint32_t tamanioCopia2 = string_length(contenido->copia2->nodo);
	memcpy(datosSerializados + posicionActual, &tamanioCopia2, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(datosSerializados + posicionActual, contenido->copia2->nodo, tamanioCopia2);
	posicionActual += tamanioCopia2;
	memcpy(datosSerializados + posicionActual, &contenido->copia2->bloque, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	//serializo bytes
	memcpy(datosSerializados + posicionActual, &contenido->bytes, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);

	return datosSerializados;

}

int sacarTamanio(tablaArchivos* entradaArchivo){
	uint32_t cont=0;
	uint32_t posicionActual=0;
	copiasXBloque* copiaBloque;
	while(cont<list_size(entradaArchivo->bloques)){
		copiaBloque=(copiasXBloque*)list_get(entradaArchivo->bloques,cont);
		posicionActual+=tamanioStruct(copiaBloque);
		cont++;
	}
//	free(copiaBloque);
	return posicionActual;
}


void enviarTablaAYama(int socket, tablaArchivos* entradaArchivo){
	uint32_t cont=0;
	uint32_t posicionActual=0;

	void* mensaje= malloc(sizeof(uint32_t)+sacarTamanio(entradaArchivo));

	int cantBloques=list_size(entradaArchivo->bloques);
	memcpy(mensaje,&cantBloques,sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);

	while(cont<list_size(entradaArchivo->bloques)){
		copiasXBloque* copiaBloque=(copiasXBloque*)list_get(entradaArchivo->bloques,cont);
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
  char* archivoABuscar=recibirString(socket);
	bool esArchivo(tablaArchivos* archivo){
		return(string_equals_ignore_case(archivo->nombreArchivo,archivoABuscar));
	}
	tablaArchivos* entradaArchivo = list_find(tablaGlobalArchivos,(void*)esArchivo);
	if(entradaArchivo==NULL){
		sendDeNotificacion(socket,ARCHIVO_NO_ENCONTRADO);
	}
	enviarTablaAYama(socket,entradaArchivo);
	free(archivoABuscar);
}

//ENVIAR IP Y PUERTO
void enviarDatosConexionNodo(int socket){
	char* nodo;
	nodo=recibirString(socket);
	bool esNodo(datosConexionNodo* nodoSeleccionado){
		return(string_equals_ignore_case(nodoSeleccionado->nodo,nodo));
	}
	datosConexionNodo* datosNodo = list_find(listaConexionNodos,(void*)esNodo);
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
