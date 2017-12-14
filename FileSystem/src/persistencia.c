/*
 * persistencia.c
 *
 *  Created on: 7/12/2017
 *      Author: utnso
 */

/*
 * persistencia.c
 *
 *  Created on: 7/12/2017
 *      Author: utnso
 */

#include "persistencia.h"

//PERSISTENCIA DE BITMAPS

void persistirTablaBitmap(char* nombreNodo){
	char* pathBitmapNodo=obtenerPathBitmap(nombreNodo);
	FILE* archivoBitmaps=fopen(pathBitmapNodo,"w+");

	free(pathBitmapNodo);

	if(archivoBitmaps==NULL){
		log_error(loggerFileSystem,"Error al tratar de abrir el archivo de bitmap del %s",nombreNodo);
		exit (-1);
	}

	bool esElNodo(strBitmaps* BitmapNodo){
		return (strcmp(nombreNodo,BitmapNodo->nodo)==0);
	}

	strBitmaps* bitmapNodo=list_find(listaBitmaps,(void*)esElNodo);

	fwrite(bitmapNodo->bitarray->bitarray, sizeof(char), bitmapNodo->bitarray->size, archivoBitmaps);
	fclose(archivoBitmaps);

}

//PERSISTENCIA DE NODOS

void persistirTablaNodo(){

	char* pathTablaNodo=obtenerPathTablaNodo();
	FILE* archivoNodos=fopen(pathTablaNodo,"w+");

	free(pathTablaNodo);

	if(archivoNodos==NULL){
		log_error(loggerFileSystem,"Error al tratar de abrir el archivo con la tabla de nodos");
		exit (-1);
	}

	fprintf(archivoNodos,"TAMANIO=%d\n",tablaNodos->tamanioFSTotal);
	fprintf(archivoNodos,"LIBRE=%d\n",tablaNodos->tamanioFSLibre);

	uint32_t cantidadDeNodo=list_size(tablaNodos->nodos);
	uint32_t contador=0;

	fprintf(archivoNodos,"NODOS=%d\n",cantidadDeNodo);

	while(contador<cantidadDeNodo){
		strNodo* nodo=list_get(tablaNodos->nodos,contador);
		fprintf(archivoNodos,"NOMBRE_NODO%d=%s\n",contador,nodo->nombre);
		fprintf(archivoNodos,"TOTAL%d=%d\n",contador,nodo->tamanioTotal);
		fprintf(archivoNodos,"LIBRE%d=%d\n",contador,nodo->tamanioLibre);
		persistirTablaBitmap(nodo->nombre);
		contador++;
	}

	fclose(archivoNodos);
}

//PERSISTENCIA DE ARCHIVO

void persistirRegistroArchivo(){
	char* path=string_new();
	string_append(&path,PATH_METADATA);
	string_append(&path,"/archivos/registroArchivo.dat");
	FILE* archivoRegistro=fopen(path,"w+");

	free(path);

	if(archivoRegistro==NULL){
		log_error(loggerFileSystem,"Error al tratar de abrir el documento con los registros de archivos del sistema");
		exit (-1);
	}

	uint32_t contador=0;
	uint32_t cantidadDeArchivos=list_size(listaRegistroDeArchivosGuardados);

	while(contador<cantidadDeArchivos){
		char* rutaArchivo=list_get(listaRegistroDeArchivosGuardados,contador);
		fprintf(archivoRegistro,"ARCHIVO%d=%s\n",contador,rutaArchivo);
		contador++;
	}

	fclose(archivoRegistro);

}


void persistirArchivo(strArchivo* archivoElegido){
	char* directorioArchivo=obtenerPathArchivo(archivoElegido->directorioPadre);
	//string_append(&directorioArchivo,"/");

	char* comando=string_new();
	string_append(&comando,"mkdir ");
	string_append(&comando,directorioArchivo);
	system(comando);
	free(comando);

	string_append(&directorioArchivo,archivoElegido->nombre);

	FILE* infoArchivo=fopen(directorioArchivo,"w+");



	if(infoArchivo==NULL){
		log_error(loggerFileSystem,"Error al tratar de abrir el archivo %s.",archivoElegido->nombre);
		exit (-1);
	}

	fprintf(infoArchivo,"TAMANIO=%d\n",archivoElegido->tamanio);
	fprintf(infoArchivo,"TIPO=%s\n",archivoElegido->tipo);

	uint32_t contador=0;
	uint32_t cantidadBloques=list_size(archivoElegido->bloques);

	while(contador<cantidadBloques){
		strBloqueArchivo* bloque=list_get(archivoElegido->bloques,contador);
		fprintf(infoArchivo,"BLOQUE%dCOPIA%d=[%s,%d]\n",contador,0,bloque->copia1->nodo,bloque->copia1->nroBloque);
		fprintf(infoArchivo,"BLOQUE%dCOPIA%d=[%s,%d]\n",contador,1,bloque->copia2->nodo,bloque->copia2->nroBloque);
		fprintf(infoArchivo,"BLOQUE%dBYTES=%d\n",bloque->nro,bloque->bytes);
		contador++;
	}

	fclose(infoArchivo);

	list_add(listaRegistroDeArchivosGuardados,directorioArchivo);

	persistirRegistroArchivo(directorioArchivo);

}

//PERSISTENCIA DE DIRECTORIOS

void persistirTablaDirectorio(){
	char* pathDirectorio =obtenerPathDirectorio();
	FILE* archivoDirectorio=fopen(pathDirectorio,"w+");
	free(pathDirectorio);

	if(archivoDirectorio==NULL){
		log_error(loggerFileSystem,"Error al tratar de abrir el archivo con la tabla de directorios");
		exit (-1);
	}

	uint32_t contador=0;
	uint32_t cantidadDeDirectorio=list_size(tablaDirectorios);

	while(contador<cantidadDeDirectorio){
		strDirectorio* directorioSeleccionado=list_get(tablaDirectorios,contador);
		fprintf(archivoDirectorio,"DIRECTORIO%d=[%d,%s,%d]\n",contador,directorioSeleccionado->index,directorioSeleccionado->nombre,directorioSeleccionado->padre);
		contador++;
	}

	fclose(archivoDirectorio);

}


