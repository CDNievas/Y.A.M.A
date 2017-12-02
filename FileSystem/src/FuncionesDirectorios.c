/*
 * FuncionesDirectorios.c
 *
 *  Created on: 2/12/2017
 *      Author: utnso
 */

#include "FuncionesDirectorios.h"

int buscarYamafs(char ** pathDesc){

	int i=0;

	while(pathDesc[i] != NULL){
		if(strcmp(pathDesc[i],"yamafs:") == 0){
			return i;
		} else {
			i++;
		}
	}

	if(pathDesc[i] == NULL){
		return -1;
	} else {
		return i;
	}

}


tablaArchivos* esArchivoPath(char * nombreArchivo,int idPadre){
	bool buscaPorNombre(tablaArchivos * archivo){
		return strcmp(archivo->nombreArchivo,nombreArchivo) == 0 && archivo->directorioPadre == idPadre;
	}
	return list_find(tablaGlobalArchivos, (void *)buscaPorNombre);;
}

t_directory* esDirectorioPath(char * nombreDirectorio, int idPadre){
	bool buscaPorNombre(t_directory * directorio){
		return strcmp(directorio->nombre,nombreDirectorio) == 0 && directorio->padre==idPadre;
	}
	return list_find(listaDirectorios, (void *)buscaPorNombre);
}

bool recorrerPath(char ** pathDesc,int indice,int idPadre){

	char * pathAct = pathDesc[indice];
	char * pathProx = pathDesc[indice+1];

	tablaArchivos * archivo = esArchivoPath(pathAct,idPadre);

	if (archivo != NULL){

		if(pathProx == NULL){
			return true;
		} else {
			return false;
		}

	} else {

		t_directory * directorio = esDirectorioPath(pathAct,idPadre);
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

bool existePath(char * pathDirectorio){

	char ** pathDesc = string_split(pathDirectorio,"/");
	int indice = buscarYamafs(pathDesc);

	if(indice == -1){
		log_warning(loggerFileSystem,"El directorio no corresponde a yamafs");
		return false;
	} else {
		return recorrerPath(pathDesc,indice,-1);
	}

	free(pathDesc);

}

//-------------------------------- NEW DIRECTORIOS

//BORRAR DIRECTORIO

char* obtenerNombreUltimoPath(char** rutaDesmembrada){

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

void borrarArchivo(char * path){

	char ** pathDesc = string_split(path,"/");
	int indice = buscarYamafs(pathDesc);

	if(indice == -1){
		log_warning(loggerFileSystem,"El directorio no corresponde a yamafs");
	} else {

		int idPadre = obtenerIdPadreArchivo(pathDesc,indice,-1);

		if(idPadre == -2){
			log_error(loggerFileSystem,"El path es inexistente");
		} else if (idPadre == -3){
			log_error(loggerFileSystem,"No se puede hacer un rm de un directorio");
		} else {

			char * nombreArchivo = obtenerNombreUltimoPath(pathDesc);

			bool eliminarArchivo(tablaArchivos* archivo){
				return (strcmp(archivo->nombreArchivo,nombreArchivo)==0 && archivo->directorioPadre==idPadre);
			}

			tablaArchivos* archivo=list_remove_by_condition(tablaGlobalNodos->contenidoXNodo,(void*)eliminarArchivo);

			if(archivo!=NULL){

				free(archivo->nombreArchivo);
				free(archivo->tipo);
				free(archivo);

			}

			log_debug(loggerFileSystem,"Se ha removido el archivo correctamente");

			//tieneQueModificarBitArrays

		}

	}

}


int obtenerIdPadreArchivo(char ** pathDesc,int indice,int idPadre){

	char * pathAct = pathDesc[indice];
	char * pathProx = pathDesc[indice+1];

	tablaArchivos * archivo = esArchivoPath(pathAct,idPadre);

	if (archivo != NULL){

		if(pathProx == NULL){
			return archivo->directorioPadre;
		} else {
			// Ruta incorrecta
			return -2;
		}

	} else {

		t_directory * directorio = esDirectorioPath(pathAct,idPadre);
		if (directorio != NULL){

			if(pathProx == NULL){
				// No se puede hacer un rm de un directorio
				return -3;
			} else {
				return obtenerIdPadreArchivo(pathDesc,indice+1,directorio->index);
			}

		} else {
			// Ruta incorrecta
			return -2;
		}

	}

}

//void copiaArchivo(tablaArchivos* entradaArchivo,char* directorioFilesystem){
//	uint32_t bloquesDelArchivo=list_size(entradaArchivo->bloques);
//	uint32_t cont=0;
//
//	while(cont<bloquesDelArchivo){
//		copiasXBloque* entradaNodo=list_get(entradaArchivo->bloques,cont);
//
//
//		void* mensaje=malloc(sizeof(uint32_t)*3+tamanio);
//		uint32_t posicionActual=0;
//		contenidoNodo* nodo0;
//		contenidoNodo* nodo1;
//		bool ordenarPorPorcentajeOcioso(contenidoNodo* nodoSeleccionado1, contenidoNodo* nodoSeleccionado2){
//			return(nodoSeleccionado1->porcentajeOcioso > nodoSeleccionado2->porcentajeOcioso);
//		}
//		list_sort(tablaGlobalNodos->contenidoXNodo,(void*)ordenarPorPorcentajeOcioso);
//
//		nodo0=list_get(tablaGlobalNodos->contenidoXNodo,0);
//
//		nodo0->libre--;
//		nodo0->porcentajeOcioso=sacarPorcentajeOcioso(nodo0->libre,nodo0->total);
//		tablaGlobalNodos->libres--;
//
//		nodo1=list_get(tablaGlobalNodos->contenidoXNodo,1);
//		nodo1->libre--;
//		nodo1->porcentajeOcioso=sacarPorcentajeOcioso(nodo1->libre,nodo1->total);
//		tablaGlobalNodos->libres--;
//
//		persistirTablaNodo();
//
//		uint32_t bloqueAsignado=asignarBloqueNodo(nodo0);
//
//		copiaBloque->copia1=malloc(sizeof(copia));
//
//		copiaBloque->copia1->nodo=nodo0->nodo;
//		copiaBloque->copia1->bloque=bloqueAsignado;
//
//
//		memcpy(mensaje,&bloqueAsignado,sizeof(uint32_t));
//		posicionActual+=sizeof(uint32_t);
//
//		memcpy(mensaje+posicionActual,&tamanio,sizeof(uint32_t));
//		posicionActual+=sizeof(uint32_t);
//
//		memcpy(mensaje+posicionActual,&tamanio,sizeof(uint32_t));
//		posicionActual+=sizeof(uint32_t);
//
//		memcpy(mensaje+posicionActual,contenidoAEnviar,tamanio);
//		posicionActual+=tamanio;
//
//		sendRemasterizado(nodo0->socket,ENV_ESCRIBIR,posicionActual,mensaje);
//
//
//		if(recvDeNotificacion(nodo0->socket)==ESC_INCORRECTA){
//				//CACHER ERROR
//		}
//
//		mensaje=realloc(mensaje,sizeof(uint32_t)*3+tamanio);
//
//		bloqueAsignado=asignarBloqueNodo(nodo1);
//			posicionActual = 0;
//			memcpy(mensaje,&bloqueAsignado,sizeof(uint32_t));
//			posicionActual+=sizeof(uint32_t);
//
//			memcpy(mensaje+posicionActual,&tamanio,sizeof(uint32_t));
//			posicionActual+=sizeof(uint32_t);
//
//			memcpy(mensaje+posicionActual,&tamanio,sizeof(uint32_t));
//			posicionActual+=sizeof(uint32_t);
//
//			memcpy(mensaje+posicionActual,contenidoAEnviar,tamanio);
//			posicionActual+=tamanio;
//
//
//			sendRemasterizado(nodo1->socket,ENV_ESCRIBIR,posicionActual,mensaje);
//
//			copiaBloque->copia2=malloc(sizeof(copia));
//
//			copiaBloque->copia2->nodo=nodo1->nodo;
//			copiaBloque->copia2->bloque=bloqueAsignado;
//
//
//			if(recvDeNotificacion(nodo1->socket)==ESC_INCORRECTA){
//					//CACHER ERROR
//			}
//
//			free(mensaje);
//
//
//		}
//}
//
//
//
//void copiaArchivoYamafsAlLocal(char* pathArchivoOrigen,char* directorioFilesystem){
//
//	if(existePath(pathArchivoOrigen)==true){
//		char* nombreArchivo=obtenerNombreUltimoPath(pathArchivoOrigen);
//		bool esElArchivo(tablaArchivos* entradaArchivo){
//			return(strcmp(entradaArchivo->nombreArchivo,nombreArchivo)==0);
//		}
//		tablaArchivos* archivoSeleccionado=list_find(tablaGlobalArchivos,(void*)esElArchivo);
//		if(archivoSeleccionado!=NULL){
//			copiaArchivo(archivoSeleccionado,directorioFilesystem);
//		}
//	}
//
//
//}
//
