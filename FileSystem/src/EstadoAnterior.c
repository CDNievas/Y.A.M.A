/*
 * EstadoAnterior.c
 *
 *  Created on: 25/11/2017
 *      Author: utnso
 */

#include "EstadoAnterior.h"
#include "FuncionesFS.h"

void cargarEstructuraBitmap(){
	log_debug(loggerFileSystem,"Cargando los bitmaps del sistema...");
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

		cont++;

	}
}


void cargarEstructuraDirectorio(t_config* archivoDirectorio){
	log_debug(loggerFileSystem,"Cargando los directorios...");
	uint32_t cantidadDeDirectorio=config_keys_amount(archivoDirectorio);
	uint32_t posicion=0;
	while(posicion<cantidadDeDirectorio){
		char* etiqueta=string_new();
		string_append(&etiqueta,"DIRECTORIO");
		string_append(&etiqueta,string_itoa(posicion));
		char** arrayDirectorioPosicion = config_get_array_value(archivoDirectorio, etiqueta);
		t_directory* entradaDirectorio=malloc(sizeof(t_directory));
		entradaDirectorio->nombre=string_new();
		entradaDirectorio->index=atoi(arrayDirectorioPosicion[0]);
		entradaDirectorio->nombre=arrayDirectorioPosicion[1];
		entradaDirectorio->padre=atoi(arrayDirectorioPosicion[2]);
		list_add(listaDirectorios,entradaDirectorio);
		posicion++;
	}
}

void cargarEstructuraNodos(t_config* arhivoNodos){
	log_debug(loggerFileSystem,"Cargando las estrcutura de nodos...");
	uint32_t tamanio = config_get_int_value(arhivoNodos, "TAMANIO");
	tablaGlobalNodos->tamanio=tamanio;

	uint32_t libre = config_get_int_value(arhivoNodos, "LIBRE");
	tablaGlobalNodos->libres=libre;

	char** arrayListaNodos = config_get_array_value(arhivoNodos, "NODOS");
	uint32_t cont=0;
	while(arrayListaNodos[cont]!=NULL){
		char* nodo=string_new();
		string_append(&nodo,arrayListaNodos[cont]);
		list_add(tablaGlobalNodos->nodo,nodo);
		cont++;
	}

	cont=0;
	while(cont<list_size(tablaGlobalNodos->nodo)){
		char* nodo=string_new();
		string_append(&nodo,list_get(tablaGlobalNodos->nodo,cont));
		contenidoNodo* nodoSeleccionado=malloc(sizeof(contenidoNodo));
		nodoSeleccionado->nodo=string_new();
		string_append(&nodoSeleccionado->nodo,nodo);

		nodoSeleccionado->disponible=0;

		string_append(&nodo,"TOTAL");
		uint32_t totalBloques = config_get_int_value(arhivoNodos, nodo);
		nodoSeleccionado->total=totalBloques;

		char* nombreNodo=string_new();
		string_append(&nombreNodo, nodoSeleccionado->nodo);
		string_append(&nombreNodo,"LIBRE");
		uint32_t bloquesLibres = config_get_int_value(arhivoNodos, nombreNodo);
		nodoSeleccionado->libre=bloquesLibres;

		nodoSeleccionado->porcentajeOcioso=(nodoSeleccionado->libre*100)/nodoSeleccionado->total;

		nodoSeleccionado->socket=0;

		list_add(tablaGlobalNodos->contenidoXNodo,nodoSeleccionado);
		cont++;
	}

}

void cargarTablaArchivo(char* pathArchivo){
	log_debug(loggerFileSystem,"Cargando la tabla de archivos...");
	char** rutaDesmembrada = string_split(pathArchivo, "/");
	char* nombreArchivo=obtenerNombreDirectorio(rutaDesmembrada);

	uint32_t posicion=0;
	while(rutaDesmembrada[posicion] != NULL){
	   posicion++;
	}

	t_config* archivo=config_create(pathArchivo);
	tablaArchivos* entradaArchivo=malloc(sizeof(tablaArchivos));
	entradaArchivo->nombreArchivo=string_new();
	entradaArchivo->tipo=string_new();
	entradaArchivo->bloques=list_create();
	string_append(&entradaArchivo->nombreArchivo,nombreArchivo);

	entradaArchivo->directorioPadre= atoi(rutaDesmembrada[posicion-2]);

	entradaArchivo->disponible=0;

	uint32_t tamanio=config_get_int_value(archivo,"TAMANIO");
	entradaArchivo->tamanio=tamanio;

	char* tipo=config_get_string_value(archivo,"TIPO");
	string_append(&entradaArchivo->tipo,tipo);

	uint32_t cantidadDeCopiasXBloque=(config_keys_amount(archivo)-2)/3;
	uint32_t contBloque=0;
	while(contBloque<cantidadDeCopiasXBloque){
		copiasXBloque* copiaBloque=malloc(sizeof(copiasXBloque));
		copiaBloque->bloque=string_new();
		copiaBloque->copia1=malloc(sizeof(copia));
		copiaBloque->copia1->nodo=string_new();
		copiaBloque->copia2=malloc(sizeof(copia));
		copiaBloque->copia2->nodo=string_new();
		copiaBloque->disponible=0;
		copiaBloque->bytes=0;


		string_append(&copiaBloque->bloque,string_itoa(contBloque));

		char* etiqueta1=string_new();
		string_append(&etiqueta1,"BLOQUE");
		string_append(&etiqueta1,string_itoa(contBloque));
		string_append(&etiqueta1,"COPIA0");
		char** copia0=config_get_array_value(archivo,etiqueta1);

		char* copia0Seleccionada=copia0[0];
		string_append(&copiaBloque->copia1->nodo,copia0Seleccionada);
		copiaBloque->copia1->bloque=atoi(copia0[1]);

		free(etiqueta1);
		liberarComandoDesarmado(copia0);

		char* etiqueta2=string_new();
		string_append(&etiqueta2,"BLOQUE");
		string_append(&etiqueta2,string_itoa(contBloque));
		string_append(&etiqueta2,"COPIA1");
		char** copia1=config_get_array_value(archivo,etiqueta2);

		char* copia1Seleccionada=copia1[0];
		string_append(&copiaBloque->copia2->nodo,copia1Seleccionada);
		copiaBloque->copia2->bloque=atoi(copia1[1]);

		free(etiqueta2);
		liberarComandoDesarmado(copia1);

		char* etiqueta3=string_new();
		string_append(&etiqueta3,"BLOQUE");
		string_append(&etiqueta3,string_itoa(contBloque));
		string_append(&etiqueta3,"BYTES");
		copiaBloque->bytes=config_get_int_value(archivo,etiqueta3);

		free(etiqueta3);

		list_add(entradaArchivo->bloques,copiaBloque);
		contBloque++;
	}
	list_add(tablaGlobalArchivos,entradaArchivo);
}

void cargarEstructuraArchivos(t_config* archivoRegistroArchivos){
	uint32_t cantidadDeDirectorio=config_keys_amount(archivoRegistroArchivos);
	uint32_t posicion=0;
	while(posicion<cantidadDeDirectorio){
		char* etiqueta=string_new();
		string_append(&etiqueta,"ARCHIVO");
		string_append(&etiqueta,string_itoa(posicion));
		char* pathArchivoSeleccionado = config_get_string_value(archivoRegistroArchivos, etiqueta);
		cargarTablaArchivo(pathArchivoSeleccionado);
		posicion++;
	}
}


bool hayUnEstadoAnterior(){
	char* pathDirectorios=string_new();
	string_append(&pathDirectorios,PATH_METADATA);
	string_append(&pathDirectorios,"directorios.dat");
	t_config * archivoDirectorios= config_create(pathDirectorios);


	char* pathNodo=string_new();
	string_append(&pathNodo,PATH_METADATA);
	string_append(&pathNodo,"nodos.bin");
	t_config* archivoNodos=config_create(pathNodo);


	char* pathRegistroArchivos=string_new();
	string_append(&pathRegistroArchivos,PATH_ARCHIVOS);
	string_append(&pathRegistroArchivos,"registro.dat");
	t_config* archivoRegistroArchivos=config_create(pathRegistroArchivos);


	if(!(archivoDirectorios==NULL) && !(archivoNodos==NULL) && !(archivoRegistroArchivos==NULL)){
		log_debug(loggerFileSystem,"Se encontro un estado anterior en el sistema.");
		cargarEstructuraDirectorio(archivoDirectorios);
		cargarEstructuraNodos(archivoNodos);
		cargarEstructuraArchivos(archivoRegistroArchivos);
		cargarEstructuraBitmap();
		hayEstadoAnterior=true;
		estaFormateado=true;
		return true;
	}else{
		return false;
	}
}
