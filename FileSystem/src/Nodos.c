/*
 * Nodos.c
 *
 *  Created on: 25/11/2017
 *      Author: utnso
 */

#include "Nodos.h"

int cantBloquesLibres(t_bitarray* bitarray) {
	int i = 0;
	int cont = 0;
	int tamaniobitarray = bitarray_get_max_bit(bitarray);
	for (; i < tamaniobitarray; i++) {
		if (!bitarray_test_bit(bitarray, i)) {
			cont++;
		}
	}
	return cont;
}


int sacarPorcentajeOcioso(int bloquesLibres, int cantBloques){
	int numero = (bloquesLibres*100)/cantBloques;
	if((bloquesLibres*100)%cantBloques != 0){
	 numero++;
	}
	return numero;
}

//void hayUnEstadoEstable(){
//	uint32_t cantidadDeArchivos=list_size(tablaGlobalArchivos);
//	uint32_t cont=0;
//	while(cont<cantidadDeArchivos){
//		tablaArchivos* entradaArchivos=list_get(tablaGlobalArchivos,cont);
//
//		uint32_t cantidadDeBloques=list_size(entradaArchivos->bloques);
//		uint32_t contNodo=0;
//
//		while(contNodo<cantidadDeBloques){
//			copiasXBloque* entradaDelBloque=list_get(entradaArchivos.bloques,contNodo);
//
//			bool estaDisponibleLaCopia (contenidoNodo* nodo){
//				return (nodo->disponible==1 && (strcmp(entradaDelBloque->copia1->nodo,nodo->nodo)==0 || strcmp(entradaDelBloque->copia2->nodo,nodo->nodo)==0));
//			}
//			bool hayUnaCopia=list_any_satisfy(tablaGlobalNodos->contenidoXNodo,(void*)estaDisponibleLaCopia );
//
//
//		}
//
//
//
//	}
//
//}
//
//
//
//
//void perteneceAlSistema(char* nombreNodo, int socket, char* ip, uint32_t puerto){
//
//	bool estaEnElSistema(char* nodoSeleccionado){
//		return(strcmp(nodoSeleccionado,nombreNodo)==0);
//	}
//	bool estabaEnElEstadoAnterior=list_any_satisfy(tablaGlobalNodos->nodo,(void*)estaEnElSistema);
//	if(estabaEnElEstadoAnterior){
//		bool esElNodo(contenidoNodo* nodo){
//			return(strcmp(nodo->nodo,nombreNodo)==0);
//		}
//		contenidoNodo* nodoElegido =list_find(tablaGlobalNodos->contenidoXNodo,(void*)esElNodo);
//
//		nodoElegido->disponible=1;
//		nodoElegido->socket=socket;
//
//		hayUnEstadoEstable();
//
//	}
//
//
//
//}
//



void registrarNodo(int socket) {

	char * nombreNodo=string_new();
	char* ip=string_new();
	int cantBloques,puerto;
	t_bitarray * bitarray;

	nombreNodo = recibirString(socket);

	cantBloques = recibirUInt(socket);

	ip=recibirString(socket);

	puerto=recibirUInt(socket);


	// Checkeo estado anterior
	if(hayEstadoAnterior){
//		perteneceAlSistema(nombreNodo,socket,ip,puerto);

	} else {
		bitarray = crearBitmap(nombreNodo,cantBloques);

		// Creo el bloque de info del nodo
		contenidoNodo* bloque = malloc(sizeof(contenidoNodo));
		bloque->nodo = string_new();
		string_append(&bloque->nodo, nombreNodo);
		bloque->disponible=1;
		bloque->total = cantBloques;
		bloque->libre = cantBloquesLibres(bitarray);
		bloque->porcentajeOcioso=sacarPorcentajeOcioso(bloque->libre,cantBloques);
		bloque->socket=socket;

		// Aniado a la tabla de info de nodos
		tablaGlobalNodos->tamanio += bloque->total;
		tablaGlobalNodos->libres += bloque->libre;
		list_add(tablaGlobalNodos->nodo, nombreNodo);
		list_add(tablaGlobalNodos->contenidoXNodo, bloque);

		// Aniado a la tala de bitmaps
		tablaBitmapXNodos* bitmapNodo = malloc(sizeof(tablaBitmapXNodos));
		bitmapNodo->nodo=string_new();
		string_append(&bitmapNodo->nodo, nombreNodo);
		bitmapNodo->bitarray = bitarray;
		list_add(listaBitmap, bitmapNodo);



		persistirTablaNodo();

	}
	//Aniado los datos de conexion del nodo
	datosConexionNodo* datosConexion=malloc(sizeof(datosConexionNodo));
	datosConexion->nodo=string_new();
	string_append(&datosConexion->nodo, nombreNodo);
	datosConexion->ip=string_new();
	string_append(&datosConexion->ip, ip);
	datosConexion->puerto=puerto;
	list_add(listaConexionNodos,datosConexion);


	hayNodos++;

	free(ip);
	free(nombreNodo);
}

