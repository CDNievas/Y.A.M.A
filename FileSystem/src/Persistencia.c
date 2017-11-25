/*
 * Persistencia.c
 *
 *  Created on: 25/11/2017
 *      Author: utnso
 */

#include "Persistencia.h"

void persistirTablaNodo(){


	//FILE* archivoNodos=fopen("/home/utnso/workspace/tp-2017-2c-ElTPEstaBien/FileSystem/metadata/nodos.bin","r+");
//	char * path = obtenerPathTablaNodo();
	FILE* archivoNodos=fopen("asd.txt","w");

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

void persistirTablaArchivo(tablaArchivos* entradaArchivo){
	char* path=string_new();
	string_append(&path,"/home/utnso/workspace/tp-2017-2c-ElTPEstaBien/FileSystem/metadata/archivos/");
	string_append(&path,entradaArchivo->directorioPadre);
	string_append(&path,"/");
	string_append(&path,entradaArchivo->nombreArchivo);
	FILE* archivo=fopen(path,"r+");

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
		fputc(copia->copia1->nodo,archivo);
		fputc(',',archivo);
		fputs(string_itoa(copia->copia1->bloque),archivo);
		fputc(']',archivo);
		fputc('\n',archivo);

		fputs("BLOQUE",archivo);
		fputs(copia->bloque,archivo);
		fputs("COPIA1=[",archivo);
		fputc(copia->copia2->nodo,archivo);
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

	FILE* archivoRegistro=fopen("/home/utnso/workspace/tp-2017-2c-ElTPEstaBien/FileSystem/metadata/archivos/registroArchivos.dat","r+");
	fputs(entradaArchivo->nombreArchivo,archivoRegistro);
	fputs("=",archivoRegistro);

}

