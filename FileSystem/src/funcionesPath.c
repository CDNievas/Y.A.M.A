/*
 * funcionesPath.c
 *
 *  Created on: 7/12/2017
 *      Author: utnso
 */

#include "funcionesPath.h"

char* obtenerNombreUltimoPath(char** rutaDesmembrada){//FIJAR SI TENGO QUE LIBERAR

	int posicion = 0;
	char* ultimoPath = string_new();
	while(1){

		if(rutaDesmembrada[posicion+1] == NULL){
			string_append(&ultimoPath, rutaDesmembrada[posicion]);
			break;
		}
		posicion++;

	}

	return ultimoPath;

}


bool existePath(char * path){

	char ** pathDesc = string_split(path,"/");
	bool existe = recorrerPath(pathDesc,0,-1);
	liberarRutaDesarmada(pathDesc);
	return existe;

}

bool recorrerPath(char ** pathDesc,int indice,int idPadre){

	char * pathAct = pathDesc[indice];
	char * pathProx = pathDesc[indice+1];

	strArchivo * archivo = buscaArchivo(pathAct,idPadre);

	if (archivo != NULL){

		if(pathProx == NULL){
			return true;
		} else {
			return false;
		}

	} else {

		strDirectorio * directorio = buscaDirectorio(pathAct,idPadre);
		if (directorio != NULL){

			if(pathProx == NULL){
				return true;
			} else {
				return recorrerPath(pathDesc,indice+1,directorio->index);
			}

		} else {
			return false;
		}

	}

}

strArchivo * buscaArchivo(char * nombreArchivo,int idPadre){
	bool buscaPorNombre(strArchivo * archivo){
		return strcmp(archivo->nombre,nombreArchivo) == 0 && archivo->directorioPadre == idPadre;
	}
	return list_find(tablaArchivos, (void *)buscaPorNombre);
}

strDirectorio * buscaDirectorio(char * nombreDirectorio, int idPadre){
	bool buscaPorNombre(strDirectorio * directorio){
		return strcmp(directorio->nombre,nombreDirectorio) == 0 && directorio->padre==idPadre;
	}
	return list_find(tablaDirectorios, (void *)buscaPorNombre);
}


int directorioInexistente(char ** pathDesc, int indice, int idPadre){

	char * pathAct = pathDesc[indice];
	char * pathProx = pathDesc[indice+1];

	strArchivo * archivo = buscaArchivo(pathAct,idPadre);

	if (archivo != NULL){

		// Es un archivo
		return -2;

	} else {

		strDirectorio * directorio = buscaDirectorio(pathAct,idPadre);

		if (directorio == NULL){

			if(pathProx == NULL){
				// Path disponible
				return idPadre;
			} else {
				// Path inexistente
				return -3;
			}

		} else {

			if(pathProx == NULL){
				// Path existente
				return -4;
			} else {
				// Sigo recorriendo path
				return directorioInexistente(pathDesc,indice+1,directorio->index);
			}

		}

	}


}

bool existePathLocal(char * path){

	FILE *archivo = fopen(path, "r");

	if(archivo == NULL){
		return false;
	} else {
		fclose(archivo);
		return true;
	}

}


uint32_t obtenerIdDirectorio(char ** pathDesc,int indice,int idPadre){

	char * pathAct = pathDesc[indice];
	char * pathProx = pathDesc[indice+1];

	strArchivo * archivo = buscaArchivo(pathAct,idPadre);

	if (archivo != NULL){
		if(pathProx == NULL){
			// Ruta incorrecta
			return -2;
		} else {
			// Ruta incorrecta
			return -2;
		}
	} else {
		strDirectorio * directorio = buscaDirectorio(pathAct,idPadre);
		if (directorio != NULL){
			if(pathProx == NULL){
				return directorio->index;
			} else {
				return obtenerIdDirectorio(pathDesc,indice+1,directorio->index);
			}
		} else {
			// Ruta incorrecta
			return -2;
		}
	}
}

uint32_t obtenerIdPadreDirectorio(char ** pathDesc,int indice,int idPadre){

	char * pathAct = pathDesc[indice];
	char * pathProx = pathDesc[indice+1];

	strArchivo * archivo = buscaArchivo(pathAct,idPadre);

	if (archivo != NULL){
		if(pathProx == NULL){
			// Ruta incorrecta
			return -2;
		} else {
			// Ruta incorrecta
			return -2;
		}
	} else {
		strDirectorio * directorio = buscaDirectorio(pathAct,idPadre);
		if (directorio != NULL){
			if(pathProx == NULL){
				return directorio->padre;
			} else {
				return obtenerIdPadreDirectorio(pathDesc,indice+1,directorio->index);
			}
		} else {
			// Ruta incorrecta
			return -2;
		}
	}
}



uint32_t obtenerIdPadreArchivo(char ** pathDesc,int indice,int idPadre){

	char * pathAct = pathDesc[indice];
	char * pathProx = pathDesc[indice+1];

	strArchivo * archivo = buscaArchivo(pathAct,idPadre);

	if (archivo != NULL){
		if(pathProx == NULL){
			// Ruta
			return archivo->directorioPadre;
		} else {
			// Ruta incorrecta
			return -2;
		}
	} else {
		strDirectorio * directorio = buscaDirectorio(pathAct,idPadre);
		if (directorio != NULL){
			if(pathProx == NULL){
				return -2;
			} else {
				return obtenerIdPadreArchivo(pathDesc,indice+1,directorio->index);
			}
		} else {
			// Ruta incorrecta
			return -2;
		}
	}
}
