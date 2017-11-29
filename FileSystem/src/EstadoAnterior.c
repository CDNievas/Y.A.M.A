/*
 * EstadoAnterior.c
 *
 *  Created on: 25/11/2017
 *      Author: utnso
 */

#include "EstadoAnterior.h"

void cargarEstructuraBitmap(){
	uint32_t cantidadDeNodo = list_size(tablaGlobalNodos->nodo);
	uint32_t cont=0;
	while(cont<cantidadDeNodo){
		contenidoNodo* nodo=list_get(tablaGlobalNodos->contenidoXNodo,cont);

		tablaBitmapXNodos* bitmapNodo = malloc(sizeof(tablaBitmapXNodos));
		bitmapNodo->nodo=string_new();
		string_append(&bitmapNodo->nodo, nodo->nodo);

		t_bitarray* bitarray = abrirBitmap(nodo->nodo,nodo->total);

		bitmapNodo->bitarray = bitarray;
		list_add(listaBitmap, bitmapNodo);

	}
}


void cargarEstructuraDirectorio(t_config* archivoDirectorio){
	uint32_t cantidadDeDirectorio=config_keys_amount(archivoDirectorio);
	uint32_t posicion=0;
	while(posicion<=cantidadDeDirectorio){
		char* etiqueta=string_new();
		string_append(&etiqueta,"DIRECTORIO");
		string_append(&etiqueta,string_itoa(posicion));
		char** arrayDirectorioPosicion = config_get_array_value(archivoDirectorio, etiqueta);
		t_directory* entradaDirectorio=malloc(sizeof(t_directory));
		entradaDirectorio->nombre=string_new();
		entradaDirectorio->index=arrayDirectorioPosicion[0];
		entradaDirectorio->nombre=arrayDirectorioPosicion[1];
		entradaDirectorio->padre=arrayDirectorioPosicion[2];
		list_add(listaDirectorios,entradaDirectorio);
	}
}

void cargarEstructuraNodos(t_config* arhivoNodos){
	uint32_t tamanio = config_get_int_value(arhivoNodos, "TAMANIO");
	tablaGlobalNodos->tamanio=tamanio;

	uint32_t libre = config_get_int_value(arhivoNodos, "LIBRE");
	tablaGlobalNodos->libres=libre;

	char** arrayListaNodos = config_get_array_value(arhivoNodos, "NODOS");
	uint32_t cont=0;
	while(arrayListaNodos[cont]!=NULL){
		list_add(tablaGlobalNodos->nodo,arrayListaNodos[cont]);
		cont++;
	}

	cont=0;
	while(cont<list_size(tablaGlobalNodos->nodo)){
		char* nodo=list_get(tablaGlobalNodos->nodo,cont);
		contenidoNodo* nodoSeleccionado=malloc(sizeof(contenidoNodo));
		nodoSeleccionado->nodo=string_new();
		string_append(&nodoSeleccionado->nodo,nodo);

		nodoSeleccionado->disponible=0;

		string_append(&nodo,"TOTAL");
		uint32_t totalBloques = config_get_int_value(arhivoNodos, nodo);
		nodoSeleccionado->total=totalBloques;

		char* nombreNodo=nodoSeleccionado->nodo;
		string_append(&nombreNodo,"LIBRE");
		uint32_t bloquesLibres = config_get_int_value(arhivoNodos, nombreNodo);
		nodoSeleccionado->libre=bloquesLibres;

		nodoSeleccionado->porcentajeOcioso=(nodoSeleccionado->libre*100)/nodoSeleccionado->total;

		list_add(tablaGlobalNodos->contenidoXNodo,nodoSeleccionado);
	}

}

void cargarTablaArchivo(char* pathArchivo){
	char** rutaDesmembrada = string_split(pathArchivo, "/");
	char* nombreArchivo=obtenerNombreDirectorio(rutaDesmembrada);
	int directorioPadre=obtenerDirectorioPadre(rutaDesmembrada);

	t_config* archivo=config_create(pathArchivo);
	tablaArchivos* entradaArchivo=malloc(sizeof(tablaArchivos));
	entradaArchivo->nombreArchivo=string_new();
	entradaArchivo->tipo=string_new();
	string_append(&entradaArchivo->nombreArchivo,nombreArchivo);

	entradaArchivo->directorioPadre=directorioPadre;

	entradaArchivo->disponible=0;

	uint32_t tamanio=config_get_int_value(archivo,"TAMANIO");
	entradaArchivo->tamanio=tamanio;

	char* tipo=config_get_string_value(archivo,"TIPO");
	string_append(entradaArchivo->tipo,tipo);

	uint32_t cantidadDeCopiasXBloque=config_keys_amount(archivo)-2;
	uint32_t contBloque=0;
	while(contBloque<=cantidadDeCopiasXBloque){
		copiasXBloque* copiaBloque=malloc(sizeof(copiasXBloque));
		copiaBloque->bloque=string_new();
		copiaBloque->copia1=malloc(sizeof(copia));
		copiaBloque->copia2=malloc(sizeof(copia));

		string_append(&copiaBloque->bloque,string_itoa(contBloque));

		char* etiqueta1=string_new();
		string_append(&etiqueta1,"BLOQUE");
		string_append(&etiqueta1,string_itoa(contBloque));
		string_append(&etiqueta1,"COPIA0");
		char** copia0=config_get_array_value(archivo,etiqueta1);

		string_append(&copiaBloque->copia1->nodo,copia0[0]);
		copiaBloque->copia1->bloque=copia0[1];

		free(etiqueta1);

		char* etiqueta2=string_new();
		string_append(&etiqueta2,"BLOQUE");
		string_append(&etiqueta2,string_itoa(contBloque));
		string_append(&etiqueta2,"COPIA1");
		char** copia1=config_get_array_value(archivo,etiqueta2);

		string_append(&copiaBloque->copia2->nodo,copia1[0]);
		copiaBloque->copia2->bloque=copia1[1];

		free(etiqueta2);

		list_add(entradaArchivo->bloques,copiaBloque);
	}
	list_add(tablaGlobalArchivos,entradaArchivo);
}

void cargarEstructuraArchivos(t_config* archivoRegistroArchivos){
	uint32_t cantidadDeDirectorio=config_keys_amount(archivoRegistroArchivos);
	uint32_t posicion=0;
	while(posicion<=cantidadDeDirectorio){
		char* etiqueta=string_new();
		string_append(&etiqueta,"ARCHIVO");
		string_append(&etiqueta,string_itoa(posicion));
		char* pathArchivoSeleccionado = config_get_string_value(archivoRegistroArchivos, etiqueta);
		cargarTablaArchivo(pathArchivoSeleccionado);
	}
}


bool hayUnEstadoAnterior(){
	//t_config * archivoDirectorios= config_create("home/utnso/workspace/tp-2017-2c-ElTPEstaBien/FileSystem/Debug/yamafs:/metadata/directorios.dat");
	char* pathNodo=string_new();
	string_append(&pathNodo,PATH_METADATA);
	string_append(&pathNodo,"nodos.bin");
	t_config* archivoNodos=config_create(pathNodo);
	char* pathRegistroArchivos=string_new();
	string_append(&pathRegistroArchivos,PATH_ARCHIVOS);
	string_append(&pathNodo,"registro.dat");
	t_config* archivoRegistroArchivos=config_create(pathRegistroArchivos);

	if( !(archivoNodos==NULL) && !(archivoRegistroArchivos==NULL)){
		//cargarEstructuraDirectorio(archivoDirectorios);
		cargarEstructuraNodos(archivoNodos);
		cargarEstructuraArchivos(archivoRegistroArchivos);
		cargarEstructuraBitmap();
		return true;
	}else{
		return false;
	}
}
