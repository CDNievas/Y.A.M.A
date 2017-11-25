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
	int tamaniobitarray = (bitarray_get_max_bit(bitarray)/8);
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

void registrarNodo(int socket) {

	char * nombreNodo;
	char* ip;
	int cantBloques,puerto;
	t_bitarray * bitarray;

	nombreNodo = recibirString(socket);

	cantBloques = recibirUInt(socket);

	ip=recibirString(socket);
	puerto=recibirUInt(socket);


	// Checkeo estado anterior
	if(hayEstadoAnterior){
		//bitarray = abrirBitmap(nombreNodo,cantBloques);

	} else {
		bitarray = crearBitmap(nombreNodo,cantBloques);

	// Creo el bloque de info del nodo
	contenidoNodo* bloque = malloc(sizeof(contenidoNodo));
	bloque->nodo = string_new();
	string_append(&bloque->nodo, nombreNodo);
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

	//Aniado los datos de conexion del nodo
	datosConexionNodo* datosConexion=malloc(sizeof(datosConexionNodo));
	datosConexion->nodo=string_new();
	string_append(&datosConexion->nodo, nombreNodo);
	datosConexion->ip=string_new();
	string_append(&datosConexion->ip, ip);
	datosConexion->puerto=puerto;
	list_add(listaConexionNodos,datosConexion);

	persistirTablaNodo();

	}
	hayNodos++;

	free(ip);
	//free(nombreNodo);
}

