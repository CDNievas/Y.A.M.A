/*
 * estadoAnterior.c
 *
 *  Created on: 10/12/2017
 *      Author: utnso
 */

#include "estadoAnterior.h"


//CARGA DE DIRECTORIO
void cargarEstructuraDirectorio(t_config* archivoDirectorio){
	log_debug(loggerFileSystem,"Cargando los directorios...");
	uint32_t cantidadDeDirectorio=config_keys_amount(archivoDirectorio);
	uint32_t posicion=0;
	while(posicion<cantidadDeDirectorio){
		char* etiqueta=string_from_format("DIRECTORIO%d",posicion);
		char** arrayDirectorioPosicion = config_get_array_value(archivoDirectorio, etiqueta);//TENGO QUE LIBERAR ESTO
		strDirectorio* entradaDirectorio=malloc(sizeof(strDirectorio));
		entradaDirectorio->nombre=string_new();
		entradaDirectorio->index=atoi(arrayDirectorioPosicion[0]);
		string_append(&entradaDirectorio->nombre,arrayDirectorioPosicion[1]);
		entradaDirectorio->padre=atoi(arrayDirectorioPosicion[2]);
		list_add(tablaDirectorios,entradaDirectorio);
		posicion++;
		free(etiqueta);
		liberarRutaDesarmada(arrayDirectorioPosicion);
	}
}

//CARGA BITMAP

void cargarEstructuraBitmap(strNodo* nodo){
	log_debug(loggerFileSystem,"Cargando el bitmap del %s",nodo->nombre);
	char* pathBitmapNodo=obtenerPathBitmap(nodo->nombre);
	FILE* archivoBitmap=fopen(pathBitmapNodo,"r+");

	free(pathBitmapNodo);

	if(archivoBitmap==NULL){
		log_error(loggerFileSystem,"Error al tratar de abrir el archivo de bitmap del %s",nodo->nombre);
		exit (-1);
	}

	strBitmaps* bitmapNodo=malloc(sizeof(strBitmaps));
	bitmapNodo->nodo=string_new();
	string_append(&bitmapNodo->nodo,nodo->nombre);
	bitmapNodo->bitarray=crearBitmap(nodo->tamanioTotal);


	fread(bitmapNodo->bitarray->bitarray, sizeof(char), bitmapNodo->bitarray->size, archivoBitmap);
	fclose(archivoBitmap);

	list_add(listaBitmaps,bitmapNodo);

}



//CARGA DE TABLA NODOS
void cargarEstructuraNodos(t_config* archivoNodos){
	log_debug(loggerFileSystem,"Cargando las estrcutura de nodos...");
	uint32_t tamanio = config_get_int_value(archivoNodos, "TAMANIO");
	tablaNodos->tamanioFSTotal=tamanio;

	uint32_t libre = config_get_int_value(archivoNodos, "LIBRE");
	tablaNodos->tamanioFSLibre=libre;

	uint32_t cantidadDeNodos = config_get_int_value(archivoNodos, "NODOS");
	uint32_t contador=0;

	while(contador<cantidadDeNodos){
		//CREO LAS ETIQUETAS DEL CONFIG
		char* etiqueta1=string_from_format("NOMBRE_NODO%d",contador);
		char* etiqueta2=string_from_format("TOTAL%d",contador);
		char* etiqueta3=string_from_format("LIBRE%d",contador);

		//CREO LA INSTANCIA DE UN NODO
		strNodo* nodoActual=malloc(sizeof(strNodo));
		nodoActual->nombre=string_new();
		nodoActual->conectado=false;

		//OBTENGO LOS VALORES DEL CONFIG
		char* nombreDelNodo=config_get_string_value(archivoNodos,etiqueta1);
		uint32_t total=config_get_int_value(archivoNodos, etiqueta2);
		uint32_t libre=config_get_int_value(archivoNodos, etiqueta3);

		string_append(&nodoActual->nombre,nombreDelNodo);
		list_add(tablaNodos->listaNodos,nodoActual->nombre); //DEBO AGREGAR EL NOMBRE DE LA INSTANCIA O DEL CONFIG?

		nodoActual->tamanioLibre=libre;
		nodoActual->tamanioTotal=total;
		nodoActual->porcentajeOscioso=(nodoActual->tamanioLibre*100)/nodoActual->tamanioTotal;
		nodoActual->socket=0;

		list_add(tablaNodos->nodos,nodoActual);
		contador++;

		cargarEstructuraBitmap(nodoActual);

		free(etiqueta1);
		free(etiqueta2);
		free(etiqueta3);
	}
}

/*char* obtenerNombreDirectorio(char** rutaDesmembrada){
  int posicion = 0;
  char* nombreArchivo = string_new();
  while(1){
    if(rutaDesmembrada[posicion+1] == NULL){
      string_append(&nombreArchivo, rutaDesmembrada[posicion]);
      break;
    }
    posicion++;
  }
  return nombreArchivo;
}*/

//CARGAR TABLA DE ARCHIVOS
void cargarTablaArchivo(char* pathArchivo){
	log_debug(loggerFileSystem,"Cargando la tabla de archivos...");
	char** rutaDesmembrada = string_split(pathArchivo, "/");//TENGO QUE LBERARLO
	char* nombreArchivo=obtenerNombreUltimoPath(rutaDesmembrada);//DEBO LIBERARLO?

	t_config* archivo=config_create(pathArchivo);

	strArchivo* entradaArchivo=malloc(sizeof(strArchivo));
	entradaArchivo->nombre=string_new();
	entradaArchivo->tipo=string_new();
	entradaArchivo->bloques=list_create();

	string_append(&entradaArchivo->nombre,nombreArchivo);

	uint32_t posicion=0;
	while(rutaDesmembrada[posicion] != NULL){
	   posicion++;
	}

	entradaArchivo->directorioPadre= atoi(rutaDesmembrada[posicion-2]);//REVISAR

	entradaArchivo->disponible=false;

	uint32_t tamanio=config_get_int_value(archivo,"TAMANIO");
	entradaArchivo->tamanio=tamanio;

	char* tipo=config_get_string_value(archivo,"TIPO");
	string_append(&entradaArchivo->tipo,tipo);
	//free(tipo);

	uint32_t cantidadDeCopiasXBloque=(config_keys_amount(archivo)-2)/3;
	uint32_t contBloque=0;
	while(contBloque<cantidadDeCopiasXBloque){
		strBloqueArchivo* copiaBloque=malloc(sizeof(strBloqueArchivo));
		copiaBloque->copia1=malloc(sizeof(strCopiaArchivo));
		copiaBloque->copia1->nodo=string_new();
		copiaBloque->copia2=malloc(sizeof(strCopiaArchivo));
		copiaBloque->copia2->nodo=string_new();
		copiaBloque->disponible=false;
		copiaBloque->bytes=0;

		copiaBloque->nro=contBloque;

		char* etiqueta1=string_from_format("BLOQUE%dCOPIA0",contBloque);
		char** copia0=config_get_array_value(archivo,etiqueta1);//TENGO QUE LIBERARLO

		string_append(&copiaBloque->copia1->nodo,copia0[0]);
		copiaBloque->copia1->nroBloque=atoi(copia0[1]);

		free(etiqueta1);
		liberarRutaDesarmada(copia0);

		char* etiqueta2=string_from_format("BLOQUE%dCOPIA1",contBloque);
		char** copia1=config_get_array_value(archivo,etiqueta2);//TENGO QUE LIBERARLO

		string_append(&copiaBloque->copia2->nodo,copia1[0]);
		copiaBloque->copia2->nroBloque=atoi(copia1[1]);

		free(etiqueta2);
		liberarRutaDesarmada(copia1);

		char* etiqueta3=string_from_format("BLOQUE%dBYTES",contBloque);
		copiaBloque->bytes=config_get_int_value(archivo,etiqueta3);

		free(etiqueta3);

		list_add(entradaArchivo->bloques,copiaBloque);
		contBloque++;
	}
	list_add(tablaArchivos,entradaArchivo);
	free(nombreArchivo);
	config_destroy(archivo);
	liberarRutaDesarmada(rutaDesmembrada);
}

void cargarEstructuraArchivos(t_config* archivoRegistroArchivos){
	uint32_t cantidadDeDirectorio=config_keys_amount(archivoRegistroArchivos);
	uint32_t posicion=0;

	while(posicion<cantidadDeDirectorio){
		char* etiqueta=string_from_format("ARCHIVO%d",posicion);
		char* pathRegistroArchivo =string_new();
		char* pathArchivoSeleccionado= config_get_string_value(archivoRegistroArchivos, etiqueta);
		string_append(&pathRegistroArchivo,pathArchivoSeleccionado);
		cargarTablaArchivo(pathArchivoSeleccionado);
		posicion++;
		list_add(listaRegistroDeArchivosGuardados,pathRegistroArchivo);
		free(etiqueta);
	}
}

//ESTADO ANTERIOR

bool presentaUnEstadoAnterior(){
	char* pathDirectorios=string_new();
	string_append(&pathDirectorios,PATH_METADATA);
	string_append(&pathDirectorios,"/directorios.dat");
	t_config * archivoDirectorios= config_create(pathDirectorios);
	free(pathDirectorios);


	char* pathNodo=string_new();
	string_append(&pathNodo,PATH_METADATA);
	string_append(&pathNodo,"/nodos.bin");
	t_config* archivoNodos2=config_create(pathNodo);

	char* pathRegistroArchivos=string_new();
	string_append(&pathRegistroArchivos,PATH_METADATA);
	string_append(&pathRegistroArchivos,"/archivos/registroArchivo.dat");
	t_config* archivoRegistroArchivos=config_create(pathRegistroArchivos);
	free(pathRegistroArchivos);

	if(!(archivoDirectorios==NULL) && !(archivoNodos2==NULL) && !(archivoRegistroArchivos==NULL)){
		log_debug(loggerFileSystem,"Se encontro un estado anterior en el sistema.");
		cargarEstructuraDirectorio(archivoDirectorios);
		cargarEstructuraNodos(archivoNodos2);
		cargarEstructuraArchivos(archivoRegistroArchivos);

		config_destroy(archivoDirectorios);
		config_destroy(archivoNodos2);
		config_destroy(archivoRegistroArchivos);
		estadoAnterior=true;
		free(pathNodo);
		return true;
	}else{
		if(archivoDirectorios != NULL){
			config_destroy(archivoDirectorios);
		}
		if(archivoNodos2 != NULL){
			config_destroy(archivoNodos2);
		}
		if(archivoRegistroArchivos != NULL){
			config_destroy(archivoRegistroArchivos);
		}
		free(pathNodo);
		return false;
	}
}
