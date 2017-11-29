/*
 * Persistencia.c
 *
 *  Created on: 25/11/2017
 *      Author: utnso
 */

#include "Persistencia.h"

void persistirTablaNodo(){

//	FILE* archivoNodos=fopen("/home/utnso/workspace/tp-2017-2c-ElTPEstaBien/FileSystem/metadata/nodos.bin","r+");
	char * path = obtenerPathTablaNodo();
	FILE* archivoNodos=fopen(path,"w+");

	fputs("TAMANIO=",archivoNodos);
	char* tamanioCadena = string_itoa(tablaGlobalNodos->tamanio);
	fputs(tamanioCadena,archivoNodos);
	free(tamanioCadena);
	fputc('\n',archivoNodos);
	fputs("BLOQUES=[",archivoNodos);
	int i=0;
	while(i<list_size(tablaGlobalNodos->nodo)){
		fputs(list_get(tablaGlobalNodos->nodo,i),archivoNodos);
		fputc(',',archivoNodos);
		i++;
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

	FILE* archivoRegistro=fopen("/home/utnso/workspace/tp-2017-2c-ElTPEstaBien/FileSystem/Debug/yamafs:/metadata/archivos/registro.dat","w+");
	int cont=0;
	int cantTotal=list_size(registroArchivos);
	while(cont<=cantTotal){
		fputs("ARCHIVO",archivoRegistro);
		fputs(string_itoa(cont),archivoRegistro);
		fputs("=",archivoRegistro);
		char* pathArchivo=list_get(registroArchivos,cont);
		fputs(pathArchivo,archivoRegistro);
		fputc('\n',archivoRegistro);
	}
	fclose(archivoRegistro);

}


void persistirTablaArchivo(tablaArchivos* entradaArchivo){
	char* path=string_new();
	string_append(&path,"/home/utnso/workspace/tp-2017-2c-ElTPEstaBien/FileSystem/Debug/yamafs:/metadata/archivos/");
	string_append(&path,string_itoa(entradaArchivo->directorioPadre));
	string_append(&path,"/");
	char* comando=string_new();
	string_append(&comando,"mkdir ");
	string_append(&comando,path);
	system(comando);
	string_append(&path,entradaArchivo->nombreArchivo);


	FILE* archivo=fopen(path,"w+");

	fputs("TAMANIO=",archivo);
	char* tamanioCadena = string_itoa(entradaArchivo->tamanio);
	fputs(tamanioCadena,archivo);
	free(tamanioCadena);
	fputc('\n',archivo);
	fputs("TIPO=[",archivo);
	fputs(entradaArchivo->tipo,archivo);
	fputc('\n',archivo);
	int i=0;
	while(i<list_size(entradaArchivo->bloques)){
		fputs("BLOQUE",archivo);
		copiasXBloque* copia=list_get(tablaGlobalNodos->nodo,i);
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
		fputc(']',archivo);
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
	FILE* archivoDirectorio=fopen("/home/utnso/workspace/tp-2017-2c-ElTPEstaBien/FileSystem/Debug/yamafs:/metadata/directorios.dat","w+");
	int cont=0;
	int cantTotal=list_size(listaDirectorios);

	while(cont<=cantTotal){
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
	}
	fclose(archivoDirectorio);

}
