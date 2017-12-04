/*
 * Persistencia.c
 *
 *  Created on: 25/11/2017
 *      Author: utnso
 */

#include "Persistencia.h"

void persistirTablaNodo(){
	log_debug(loggerFileSystem,"Se persiste la tabla de nodos");
//	FILE* archivoNodos=fopen("/home/utnso/workspace/tp-2017-2c-ElTPEstaBien/FileSystem/metadata/nodos.bin","r+");
	char * path = obtenerPathTablaNodo();
	FILE* archivoNodos=fopen(path,"w+");

	if(archivoNodos == NULL){
		log_error(loggerFileSystem,"Error al tratar de abrir el archivo de nodos.");
		exit(-1);
	}

	free(path);

	fputs("TAMANIO=",archivoNodos);
	char* tamanio = string_itoa(tablaGlobalNodos->tamanio);
	fputs(tamanio,archivoNodos);
	free(tamanio);
	fputc('\n',archivoNodos);

	fputs("LIBRE=",archivoNodos);
	char* libre = string_itoa(tablaGlobalNodos->libres);
	fputs(libre,archivoNodos);
	free(libre);
	fputc('\n',archivoNodos);

	fputs("NODOS=[",archivoNodos);
	int i=0;
	while(i<list_size(tablaGlobalNodos->nodo)){
		fputs(list_get(tablaGlobalNodos->nodo,i),archivoNodos);
		i++;

		if(i<list_size(tablaGlobalNodos->nodo)){
			fputc(',',archivoNodos);
		}

	}
	fputc(']',archivoNodos);
	fputc('\n',archivoNodos);

	int s=0;
	char* nodoSeleccionado;
	while(s<list_size(tablaGlobalNodos->nodo)){
		nodoSeleccionado=list_get(tablaGlobalNodos->nodo,s);
		fputs(nodoSeleccionado,archivoNodos);
		fputs("TOTAL=",archivoNodos);
		bool esNodo(contenidoNodo* contenidoDeUnNodo){
			return(strcmp(contenidoDeUnNodo->nodo,nodoSeleccionado)==0);
		}
		contenidoNodo* nodoElegido=list_find(tablaGlobalNodos->contenidoXNodo ,(void*)esNodo);
		char* totalNodoElegido = string_itoa(nodoElegido->total);
		fputs(totalNodoElegido,archivoNodos);
		free(totalNodoElegido);
		fputc('\n',archivoNodos);

		fputs(nodoSeleccionado,archivoNodos);
		fputs("LIBRE=",archivoNodos);
		char* totalNodoLibre = string_itoa(nodoElegido->libre);
		fputs(totalNodoLibre ,archivoNodos);
		free(totalNodoLibre);
		fputc('\n',archivoNodos);

		s++;
	}
	fclose(archivoNodos);

}

void persistirRegistroArchivo(){
	char* path=string_new();
	string_append(&path,PATH_ARCHIVOS);
	string_append(&path,"registro.dat");

	FILE* archivoRegistro=fopen(path,"w+");

	free(path);

	if(archivoRegistro == NULL){
		log_error(loggerFileSystem,"Error al tratar de abrir el archivo en persistir registro archivo.");
		exit(-1);
	}

	int cont=0;
	int cantTotal=list_size(registroArchivos);
	while(cont<cantTotal){
		fputs("ARCHIVO",archivoRegistro);
		fputs(string_itoa(cont),archivoRegistro);
		fputs("=",archivoRegistro);
		char* pathArchivo=list_get(registroArchivos,cont);
		fputs(pathArchivo,archivoRegistro);
		fputc('\n',archivoRegistro);
		cont++;
	}
	fclose(archivoRegistro);

}


void persistirTablaArchivo(tablaArchivos* entradaArchivo){

	log_debug(loggerFileSystem,"Se persiste el archivo &d", entradaArchivo->nombreArchivo);
	char* path=string_new();
	string_append(&path,PATH_ARCHIVOS);
	string_append(&path,string_itoa(entradaArchivo->directorioPadre));
	string_append(&path,"/");
	char* comando=string_new();
	string_append(&comando,"mkdir ");
	string_append(&comando,path);
	system(comando);
	free(comando);
	string_append(&path,entradaArchivo->nombreArchivo);

	FILE* archivo=fopen(path,"w+");

	if(archivo == NULL){
		log_error(loggerFileSystem,"Error al tratar de abrir el archivo en persistir tabla archivo.");
		exit(-1);
	}

	fputs("TAMANIO=",archivo);
	char* tamanioCadena = string_new();
	tamanioCadena=string_itoa(entradaArchivo->tamanio);
	fputs(tamanioCadena,archivo);
	free(tamanioCadena);
	fputc('\n',archivo);
	fputs("TIPO=",archivo);
	fputs(entradaArchivo->tipo,archivo);
	fputc('\n',archivo);
	int i=0;
	while(i<list_size(entradaArchivo->bloques)){
		fputs("BLOQUE",archivo);
		copiasXBloque* copia=list_get(entradaArchivo->bloques,i);
		fputs(copia->bloque,archivo);
		fputs("COPIA0=[",archivo);
		fputs(copia->copia1->nodo,archivo);
		fputc(',',archivo);
		fputs(string_itoa(copia->copia1->bloque),archivo);
		fputc(']',archivo);
		fputc('\n',archivo);

		fputs("BLOQUE",archivo);
		fputs(copia->bloque,archivo);
		fputs("COPIA1=[",archivo);
		fputs(copia->copia2->nodo,archivo);
		fputc(',',archivo);
		fputs(string_itoa(copia->copia2->bloque),archivo);
		fputc(']',archivo);
		fputc('\n',archivo);

		fputs("BLOQUE",archivo);
		fputs(copia->bloque,archivo);
		fputs("BYTES=",archivo);
		fputs(string_itoa(copia->bytes),archivo);
		fputc('\n',archivo);

		i++;
	}

	fclose(archivo);

	list_add(registroArchivos,path);

	persistirRegistroArchivo();

}


//void persistirBitmap(tablaBitmapXNodos* nodo){
//	char* pathBitmap=obtenerPathBitmap(nodo->nodo);
//	FILE* archivoBitmapNodo=fopen(pathBitmap,"w+");
//
//	fputs(nodo->bitarray->bitarray,archivoBitmapNodo);
//	fclose(archivoBitmapNodo);
//}

void persistirDirectorio(){

	log_debug(loggerFileSystem,"Se persiste la tabla de directorios");
	char* path=string_new();
	string_append(&path,PATH_METADATA);
	string_append(&path,"directorios.dat");

	FILE* archivoDirectorio=fopen(path,"w+");
	free(path);

	if(archivoDirectorio == NULL){
		log_error(loggerFileSystem,"Error al tratar de abrir el archivo en persistir tabla archivo.");
		exit(-1);
	}

	int cont=0;
	int cantTotal=list_size(listaDirectorios);

	while(cont<cantTotal){
		fputs("DIRECTORIO",archivoDirectorio);
		fputs(string_itoa(cont),archivoDirectorio);
		fputs("=[",archivoDirectorio);
		t_directory* directorioSeleccionado=list_get(listaDirectorios,cont);
		fputs(string_itoa(directorioSeleccionado->index),archivoDirectorio);
		fputc(',',archivoDirectorio);
		fputs(directorioSeleccionado->nombre,archivoDirectorio);
		fputc(',',archivoDirectorio);
		fputs(string_itoa(directorioSeleccionado->padre),archivoDirectorio);
		fputc(']',archivoDirectorio);
		fputc('\n',archivoDirectorio);
		cont++;
	}
	fclose(archivoDirectorio);
}
