/*
 * funcionesConsola.c
 *
 *  Created on: 7/12/2017
 *      Author: utnso
 */

#include "funcionesConsola.h"


void liberarRutaDesarmada(char** ruta){
	uint32_t cont=0;
	for(;ruta[cont]!=NULL;cont++){
		free(ruta[cont]);
	}
	free(ruta);
}


int cantParam(char ** com){

	int i=0;
	while(com[i] != NULL){
		i++;
	}
	return i;

}

bool chequearParamCom(char ** com, int cantMin, int cantMax){
	int x = cantParam(com);
	return cantMin <= x && x <= cantMax;
}

bool contieneYamafs(char * path){

	char ** pathDesc = string_split(path,"/");

	int i = 0;
	while(pathDesc[i] != NULL){
		if(strcmp(pathDesc[i],"yamafs:") == 0){
			free(pathDesc);
			return true;
		} else {
			i++;
		}
	}
	free(pathDesc);
	return false;
}

uint32_t sacarTamanioArchivo (FILE* archivo){
	fseek(archivo,0,SEEK_END);
	uint32_t tamanio=ftell(archivo);
	return tamanio;
}

int asignarBloqueNodo(strNodo* nodoOriginal){
	int posicionEnELBitarray=0;
	bool esElNodo(strBitmaps* BitmapNodo){
		return (strcmp(nodoOriginal->nombre,BitmapNodo->nodo)==0);
	}
	strBitmaps* BitarrayNodo = list_find(listaBitmaps,(void*)esElNodo);
	for(;posicionEnELBitarray < nodoOriginal->tamanioTotal; posicionEnELBitarray++){
		if(!bitarray_test_bit(BitarrayNodo->bitarray,posicionEnELBitarray)){
			bitarray_set_bit(BitarrayNodo->bitarray,posicionEnELBitarray);
			return posicionEnELBitarray;
		}
	}
	return 0;//TENGO QUE CACHEAR ESTE ERROR?
}



bool asignarEnviarANodo(void* contenidoAEnviar, uint32_t tamanioContenido, strBloqueArchivo* copiasBloque){

	//ARMO EL MENSAJE QUE LE VOY A ENVIAR A DATANODE
	void* mensaje=malloc(sizeof(uint32_t)*3+tamanioContenido);
	uint32_t posicionActualDelMensaje=0;

	//FILTRO LA LISTA GLOBAL DE NODOS POR SI ESTAN DISPONIBLES Y SI TIENEN ESPACIO LIBRE
	bool estaDisponible(strNodo* nodoSeleccionado){
		return (nodoSeleccionado->conectado==true && nodoSeleccionado->porcentajeOscioso>0);
	}
	t_list* listaNodosDisponiblesEnElSistema=list_filter(tablaNodos->nodos,(void*)estaDisponible);

	//ORDENO LA LISTA FILTRADA POR EL POCENTAJE QUE TENGA OCIOSO
	bool ordenarPorPorcentajeOcioso(strNodo* nodoSeleccionado1, strNodo* nodoSeleccionado2){
		return(nodoSeleccionado1->porcentajeOscioso > nodoSeleccionado2->porcentajeOscioso);
	}
	list_sort(listaNodosDisponiblesEnElSistema,(void*)ordenarPorPorcentajeOcioso);

	//ELiJO EL NODO QUE TENGA EL ORIGINAL
	strNodo* nodoOriginal=list_get(listaNodosDisponiblesEnElSistema,0);

	if(nodoOriginal==NULL){
		log_error(loggerFileSystem,"No se pudo asignar un nodo para el bloque original del archivo. No hay nodos suficientes.");
		exit (-1);
	}
		//MODIFICO LOS DATOS DEL NODO
	nodoOriginal->tamanioLibre--;
	nodoOriginal->porcentajeOscioso=(nodoOriginal->tamanioLibre*100)/nodoOriginal->tamanioTotal;
	//DISMINUYO LA CANTIDAD DE BLOQUES LIBRES DEL FS
	tablaNodos->tamanioFSLibre--;

		//ASIGNO UN BLOQUE LIBRE AL NODO
	uint32_t bloqueAsignado=asignarBloqueNodo(nodoOriginal);

	//ENVIO EL ORIGINAL
	copiasBloque->copia1=malloc(sizeof(strCopiaArchivo));
	copiasBloque->copia1->nroBloque=bloqueAsignado;
	copiasBloque->copia1->nodo=string_new();

	string_append(&copiasBloque->copia1->nodo,nodoOriginal->nombre);

	memcpy(mensaje,&bloqueAsignado,sizeof(uint32_t));
	posicionActualDelMensaje+=sizeof(uint32_t);

	memcpy(mensaje+posicionActualDelMensaje,&tamanioContenido,sizeof(uint32_t));
	posicionActualDelMensaje+=sizeof(uint32_t);

	memcpy(mensaje+posicionActualDelMensaje,&tamanioContenido,sizeof(uint32_t));
	posicionActualDelMensaje+=sizeof(uint32_t);

	memcpy(mensaje+posicionActualDelMensaje,contenidoAEnviar,tamanioContenido);
	posicionActualDelMensaje+=tamanioContenido;

	sendRemasterizado(nodoOriginal->socket,ENV_ESCRIBIR,posicionActualDelMensaje,mensaje);

	pthread_mutex_lock(&mutexEnvioANodos);

	if(envioDeInformacionADataNode==false){
		log_error(loggerFileSystem,"No se pudo almacenar el archivo en el %d", nodoOriginal->nombre);
		return false;
	}

	//ELIJO EL NODO QUE TIENE LA COPIA
	strNodo* nodoCopia=list_get(listaNodosDisponiblesEnElSistema,1);
	if(nodoCopia==NULL){
		nodoCopia=list_get(listaNodosDisponiblesEnElSistema,0);
		log_error(loggerFileSystem,"Por falta de nodos para almacenar la copia, se procede a guardar la copia en el mismo lugar que el original.");
	}
		//MODIFICO LOS DATOS DEL NODO
	nodoCopia->tamanioLibre--;
	nodoCopia->porcentajeOscioso=(nodoCopia->tamanioLibre*100)/nodoCopia->tamanioTotal;
	//DISMINUYO LA CANTIDAD DE BLOQUES LIBRES DEL FS
	tablaNodos->tamanioFSLibre--;

	bloqueAsignado=asignarBloqueNodo(nodoCopia);

	//ENVIO LA COPIA
	mensaje=realloc(mensaje,sizeof(uint32_t)*3+tamanioContenido);

	posicionActualDelMensaje=0;

	copiasBloque->copia2=malloc(sizeof(strCopiaArchivo));
	copiasBloque->copia2->nroBloque=bloqueAsignado;
	copiasBloque->copia2->nodo=string_new();

	string_append(&copiasBloque->copia2->nodo,nodoCopia->nombre);

	memcpy(mensaje,&bloqueAsignado,sizeof(uint32_t));
	posicionActualDelMensaje+=sizeof(uint32_t);

	memcpy(mensaje+posicionActualDelMensaje,&tamanioContenido,sizeof(uint32_t));
	posicionActualDelMensaje+=sizeof(uint32_t);

	memcpy(mensaje+posicionActualDelMensaje,&tamanioContenido,sizeof(uint32_t));
	posicionActualDelMensaje+=sizeof(uint32_t);

	memcpy(mensaje+posicionActualDelMensaje,contenidoAEnviar,tamanioContenido);
	posicionActualDelMensaje+=tamanioContenido;

	sendRemasterizado(nodoCopia->socket,ENV_ESCRIBIR,posicionActualDelMensaje,mensaje);

	pthread_mutex_lock(&mutexEnvioANodos);

	if(envioDeInformacionADataNode==false){
		log_error(loggerFileSystem,"No se pudo almacenar el archivo en el %d", nodoCopia->nombre);
		return false;
	}
	free(mensaje);
	return true;


}



void enviarDatosANodo(t_list* posicionesBloquesAGuardar,FILE* archivoALeer,strArchivo* entradaArchivoAGuardar){
	uint32_t bloqueActual=0;

	void enviarInfoNodoPorPosicion(uint32_t posicion){
		//GENERO LAS INSTANCIAS DE LOS BLOQUES DEL ARCHIVO
		strBloqueArchivo* copiasBloque=malloc(sizeof(strBloqueArchivo));
		copiasBloque->nro=bloqueActual;

		if(bloqueActual==0){
			void* contenidoAEnviar=malloc(posicion);
			fread(contenidoAEnviar,posicion,1,archivoALeer);
			//ASIGNO LOS BYTES QUE OCUPA EL BLOQUE
			copiasBloque->bytes=posicion;
			//ASIGNO LOS NODOS A DONDO QUIERO GUARDAR EL CONTENIDO
			if(asignarEnviarANodo(contenidoAEnviar,posicion,copiasBloque)==false){
				return;
			}
			free(contenidoAEnviar);
		}else{
			uint32_t posicionAnterior = (uint32_t) list_get(posicionesBloquesAGuardar,bloqueActual-1);
			//DETERMINO LA CANTIDAD EXACTA QUE TENGO QUE GUARDAR DEL BLOQUE
			uint32_t tamanioALeer=posicion-posicionAnterior;
			void* contenidoAEnviar=malloc(tamanioALeer);
			fread(contenidoAEnviar,tamanioALeer,1,archivoALeer);
			//ASIGNO LOS BYTES QUE OCUPA EL BLOQUE
			copiasBloque->bytes=tamanioALeer;
			//ASIGNO LOS NODOS DONDE QUIERO GUARDAR EL CONTENIDO
			if(asignarEnviarANodo(contenidoAEnviar,tamanioALeer,copiasBloque)==false){
				return;
			}
			free(contenidoAEnviar);
		}
		bloqueActual++;
		list_add(entradaArchivoAGuardar->bloques,copiasBloque);
	}
	list_iterate(posicionesBloquesAGuardar,(void*) enviarInfoNodoPorPosicion);
}


int obtenerDirectorioPadre(char** rutaDesmembrada){
  char* fathersName = string_new();
  bool isMyFather(strDirectorio* directory){
    return strcmp(fathersName, directory->nombre) == 0;
  }
  int posicion = 0;
  while(1){
    if(rutaDesmembrada[posicion+1]!=NULL){
      if(rutaDesmembrada[posicion+2] == NULL){
        string_append(&fathersName, rutaDesmembrada[posicion]);
        strDirectorio* directory = list_find(tablaDirectorios, (void*)isMyFather);
        if(directory == NULL){
        	free(fathersName);
        	return -2;
        }
        free(fathersName);
        return directory->index;
      }
    }else if(rutaDesmembrada[posicion+1]==NULL){
    	free(fathersName);
    	return -1;
    }
    posicion++;
  }
}



bool almacenarArchivo(char* pathArchivo, char* pathDirectorio, char* tipo){
	FILE* archivoALeer=fopen(pathArchivo,"r+");

	//SI NO SE PUEDE ABRIR EL ARCHIVO
	if(archivoALeer==NULL){
		log_error(loggerFileSystem,"Error al tratar de abrir el archivo.");
		return false;
	}

	//SI existe el directorio

	//Si tiene algo almacenado
	uint32_t tamanioDelArchivo = sacarTamanioArchivo(archivoALeer);
	if(tamanioDelArchivo==0){
		log_error(loggerFileSystem,"El archivo que se desea guardar no contiene nada.");
		fclose(archivoALeer);
		return false;
	}

	//VERIFICO SI EL TIPO DE DATO ES EL CORRECTO
	if(strcmp(tipo,"B")!=0 && strcmp(tipo,"T")!=0){
		log_error(loggerFileSystem,"El tipo de archivo ingresado es incorrecto");
		fclose(archivoALeer);
		return false;
	}


	//GENERO LA INSTANCIO DEL ARCHIVO PARA ALMACENARLO
	strArchivo* entradaArchivoAGuardar=malloc(sizeof(strArchivo));
	entradaArchivoAGuardar->tamanio=sacarTamanioArchivo(archivoALeer);
	entradaArchivoAGuardar->bloques=list_create();
	entradaArchivoAGuardar->nombre=string_new();
	entradaArchivoAGuardar->tipo=string_new();

		//OBTENGO EL NOMBRE DEL ARCHIVO
	char** rutaArchivo=string_split(pathArchivo,"/"); 									//TENGO QUE LIBERARLO
	char* nombreArchivo=obtenerNombreUltimoPath(rutaArchivo);
	string_append(&entradaArchivoAGuardar->nombre,nombreArchivo);

		//HAY QUE LIBERAR RUTA ARHICHIVO

		//OBTENGO EL INDEX DEL DIRECTORIO PARDRE
	//entradaArchivoAGuardar->directorioPadre=obtenerIdPadreDirectorio(rutaArchivo,0,-1);
	entradaArchivoAGuardar->directorioPadre=obtenerDirectorioPadre(rutaArchivo);
		//OBTENGO EL TIPO
	string_append(&entradaArchivoAGuardar->tipo,tipo);

	liberarRutaDesarmada(rutaArchivo);

	t_list* posicionesBloquesAGuardar=list_create();

	log_trace(loggerFileSystem,"Se procede a calcular la cantidad de bloques que ocupa el archivo %s",nombreArchivo);
	if(strcmp(tipo,"B")==0){
		uint32_t tamAux=0;
		while(tamanioDelArchivo>0){
			if(tamanioDelArchivo<1048576){
				tamAux+=tamanioDelArchivo;
				list_add(posicionesBloquesAGuardar,tamAux);
				tamanioDelArchivo-=tamanioDelArchivo;
			}else{
				tamAux+=1048576;
				list_add(posicionesBloquesAGuardar,tamAux);
				tamanioDelArchivo-=1048576;
			}
		}

	}
	if(strcmp(tipo,"T")==0){
		int digito;
		uint32_t ultimoBarraN=0;
		uint32_t registroAntesMega=0;
		uint32_t ultimaPosicion=0;

		fseek(archivoALeer,0,SEEK_SET);
		while(!feof(archivoALeer)){
			digito = fgetc(archivoALeer);
			if(digito=='\n'){
				ultimoBarraN = ftell(archivoALeer);
			}

			if(ftell(archivoALeer)==1048576+registroAntesMega){
				registroAntesMega=ultimoBarraN;
				list_add(posicionesBloquesAGuardar,ultimoBarraN);
			}
		}

		fseek(archivoALeer,0,SEEK_END);
		ultimaPosicion=ftell(archivoALeer);
		list_add(posicionesBloquesAGuardar,ultimaPosicion);
		fseek(archivoALeer,0,SEEK_SET);
	}

	uint32_t cantidadBloquesArchivo=list_size(posicionesBloquesAGuardar);
	if(tablaNodos->tamanioFSLibre<cantidadBloquesArchivo*2){
		log_error(loggerFileSystem,"El tamaño del archivo supera la capacidad de almacenamiento del sistema");
		//DEBO LIBERAR todo
		return false;
	}

	entradaArchivoAGuardar->disponible=true;

	log_trace(loggerFileSystem,"Se Procede almacenar el %s.",nombreArchivo);
	enviarDatosANodo(posicionesBloquesAGuardar,archivoALeer,entradaArchivoAGuardar);
	log_info(loggerFileSystem,"Se ha almacenado correctamente el archivo %s.",nombreArchivo);
	list_add(tablaArchivos,entradaArchivoAGuardar);


	//PERSISTO EL ARCHIVO
	persistirArchivo(entradaArchivoAGuardar);
	//PERSISTO MI TABLA DE NODOS
	persistirTablaNodo();
	list_destroy(posicionesBloquesAGuardar);
	fclose(archivoALeer);
	free(nombreArchivo);
	return true;

}


int crearDirectorio(char * path){

	int indexDir = list_size(tablaDirectorios);
	if(indexDir<=100){

		char ** pathDesc = string_split(path,"/");

		int idPadre = directorioInexistente(pathDesc,0,-1);

		if(idPadre == -2 || idPadre == -3 || idPadre == -4){
			return idPadre;
		} else {

			strDirectorio * directorio = malloc(sizeof(strDirectorio));
			directorio->nombre = string_new();
			string_append(&directorio->nombre,obtenerNombreUltimoPath(pathDesc));
			directorio->padre = idPadre;
			directorio->index = indexDir;

			list_add(tablaDirectorios,directorio);
			persistirTablaDirectorio();
			free(pathDesc);
			return 0;

		}

	} else {
		return -1;
	}

}

int borrarDirectorio(char * pathDir){

	if(!contieneYamafs(pathDir)){
		return -1;
	} else {

		char ** pathDesc = string_split(pathDir,"/");

		int idPadreDir = obtenerIdPadreDirectorio(pathDesc,0,-1);

		if(idPadreDir == -2){
			return idPadreDir;
		} else {

			bool existenArchivosEnDir(strArchivo * archivo){
				return archivo->directorioPadre==idPadreDir;
			}

			if(list_any_satisfy(tablaArchivos,(void *) existenArchivosEnDir)){

				return -3;

			} else {

				char * nombreDirectorio = obtenerNombreUltimoPath(pathDesc);
				bool eliminarDirectorio(strDirectorio * directorio){
					return (strcmp(directorio->nombre,nombreDirectorio)==0 && directorio->padre==idPadreDir);
				}

				strDirectorio * directorio=list_remove_by_condition(tablaDirectorios,(void*)eliminarDirectorio);

				if(directorio!=NULL){
					free(directorio->nombre);
					free(directorio);
				}

				return 0;

			}

		}

	}

}

