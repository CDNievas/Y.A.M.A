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


bool almacenarArchivo(char* pathArchivo, char* pathDirectorio, char* tipo){
	FILE* archivoALeer=fopen(pathArchivo,"r+");

	//SI NO SE PUEDE ABRIR EL ARCHIVO
	if(archivoALeer==NULL){
		printf("Error al tratar de abrir el archivo \n");
		log_warning(loggerFileSystem,"Error al tratar de abrir el archivo.");
		return false;
	}

	//SI existe el directorio
	if(!existePath(pathDirectorio)){
		printf("El directorio de yamafs no existe \n");
		log_warning(loggerFileSystem,"El directorio de yamafs no existe.");
		return false;
	}

	//Si tiene algo almacenado
	uint32_t tamanioDelArchivo = sacarTamanioArchivo(archivoALeer);
	if(tamanioDelArchivo==0){
		printf("El archivo que se desea guardar no contiene nada \n");
		log_warning(loggerFileSystem,"El archivo que se desea guardar no contiene nada.");
		fclose(archivoALeer);
		return false;
	}

	//VERIFICO SI EL TIPO DE DATO ES EL CORRECTO
	if(strcmp(tipo,"B")!=0 && strcmp(tipo,"T")!=0){
		printf("El tipo de archivo ingresado es incorrecto \n");
		log_warning(loggerFileSystem,"El tipo de archivo ingresado es incorrecto");
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
	char** rutaArchivo=string_split(pathArchivo,"/");
	char** rutaDirectorio = string_split(pathDirectorio,"/");
	//TENGO QUE LIBERARLO
	char* nombreArchivo=obtenerNombreUltimoPath(rutaArchivo);
	string_append(&entradaArchivoAGuardar->nombre,nombreArchivo);

		//HAY QUE LIBERAR RUTA ARHICHIVO

		//OBTENGO EL INDEX DEL DIRECTORIO PARDRE
	entradaArchivoAGuardar->directorioPadre=obtenerIdDirectorio(rutaDirectorio,0,-1);

		//OBTENGO EL TIPO
	string_append(&entradaArchivoAGuardar->tipo,tipo);

	liberarRutaDesarmada(rutaArchivo);
	liberarRutaDesarmada(rutaDirectorio);

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
		printf("El tamaño del archivo supera la capacidad de almacenamiento del sistema \n");
		log_warning(loggerFileSystem,"El tamaño del archivo supera la capacidad de almacenamiento del sistema");
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
			//free(pathDesc);
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

		int idDir = obtenerIdDirectorio(pathDesc,0,-1);

		if(idDir == -2){
			return idDir;
		} else {

			bool existenArchivosEnDir(strArchivo * archivo){
				return archivo->directorioPadre==idDir;
			}

			if(list_any_satisfy(tablaArchivos,(void *) existenArchivosEnDir)){

				return -3;

			} else {

				int idPadreDir = obtenerIdPadreDirectorio(pathDesc,0,-1);

				char * nombreDirectorio = obtenerNombreUltimoPath(pathDesc);
				bool eliminarDirectorio(strDirectorio * directorio){
					return (strcmp(directorio->nombre,nombreDirectorio)==0 && directorio->padre==idPadreDir);
				}

				strDirectorio * directorio=list_remove_by_condition(tablaDirectorios,(void*)eliminarDirectorio);

				if(directorio!=NULL){
					free(directorio->nombre);
					free(directorio);
				}
				persistirTablaDirectorio();

				return 0;

			}

		}

	}

}

int borrarArchivo(char * pathDir){

	if(!contieneYamafs(pathDir)){
		return -1;
	} else {

		char ** pathDesc = string_split(pathDir,"/");

		int idDirPadre = obtenerIdPadreArchivo(pathDesc,0,-1);

		if(idDirPadre == -2){
			return idDirPadre;
		} else {

			char * nombreArchivo = obtenerNombreUltimoPath(pathDesc);
			bool eliminarArchivo(strArchivo * archivo){
				return (strcmp(archivo->nombre,nombreArchivo)==0 && archivo->directorioPadre==idDirPadre);
			}

			strArchivo * archivo=list_remove_by_condition(tablaArchivos,(void*)eliminarArchivo);

			if(archivo!=NULL){

				borrarBloquesArchivos(archivo);
				free(archivo->nombre);
				free(archivo);
				//BORRAR ARCHIVO DEL BITARRAY

			}

			return 0;

		}

	}

}

int renombrarPath(char * path, char * nuevoNombre){

	char ** pathDesc = string_split(path,"/");

	int idPadreArchivo = obtenerIdPadreArchivo(pathDesc,0,-1);
	char * viejoNombre = obtenerNombreUltimoPath(pathDesc);

	strArchivo * archivo = buscaArchivo(viejoNombre,idPadreArchivo);

	if(archivo == NULL){

		int idPadreDirectorio = obtenerIdPadreDirectorio(pathDesc,0,-1);

		strDirectorio * directorio = buscaDirectorio(viejoNombre,idPadreDirectorio);

		if(directorio == NULL){

			return -1;

		} else {

			bool yaExisteDir(strDirectorio * x){
				return(x->padre==idPadreDirectorio && strcmp(x->nombre,nuevoNombre)==0);
			}

			if(list_any_satisfy(tablaDirectorios, (void *) yaExisteDir)){
				return -2;
			} else {

				free(directorio->nombre);
				directorio->nombre = string_new();
				string_append(&directorio->nombre,nuevoNombre);

				persistirTablaDirectorio();

				return 0;

			}

		}

	} else {

		bool yaExisteArchivo(strArchivo * x){
			return(x->directorioPadre==idPadreArchivo && strcmp(x->nombre,nuevoNombre)==0);
		}

		if(list_any_satisfy(tablaArchivos, (void *) yaExisteArchivo)){
			return -2;
		} else {

			char* pathArchivoEnMetadata=obtenerPathArchivo(idPadreArchivo);
			string_append(&pathArchivoEnMetadata,viejoNombre);

			char* comando=string_new();
			string_append(&comando,"rm ");
			string_append(&comando,pathArchivoEnMetadata);
			system(comando);

			bool esElPathMetadata(char* pathSeleccionado){
				return (strcmp(pathSeleccionado,pathArchivoEnMetadata)==0);
			}
			char* pathVictima=list_remove_by_condition(listaRegistroDeArchivosGuardados,(void*)esElPathMetadata);

			free(pathVictima);
			free(comando);
			free(pathArchivoEnMetadata);


			free(archivo->nombre);
			archivo->nombre = string_new();
			string_append(&archivo->nombre,nuevoNombre);

			persistirArchivo(archivo);

			return 0;

		}
	}

}


char * obtenerBloque(int socket, uint32_t nroBloque){

	int tamanioMsg = sizeof(uint32_t) + sizeof(uint32_t);
	void * msg = malloc(tamanioMsg);

	memcpy(msg,&nroBloque,sizeof(uint32_t));
	uint32_t cantBytes = 1048576;
	memcpy(msg+sizeof(uint32_t),&cantBytes,sizeof(uint32_t));
	sendRemasterizado(socket,ENV_LEER,tamanioMsg,msg);
	free(msg);
	uint32_t noti = recibirUInt(socket);
	//uint32_t tamanio = recibirUInt(socket);
	void* string = malloc(cantBytes);
	if(recv(socket, string, cantBytes, MSG_WAITALL) == -1){
		perror("Error al recibir un string.");
		exit(-1);
	}
	char* stringRecibido = string_substring_until(string, cantBytes);
	free(string);
	return stringRecibido;
}



int funcionCat(strBloqueArchivo * bloque){

	char * nombreNodo = bloque->copia1->nodo;

	bool buscaNodo(strNodo * x){
		return (strcmp(x->nombre,nombreNodo)==0);
	}


	strNodo * nodo = list_find(tablaNodos->nodos, (void *) buscaNodo);
	strNodo * nodoElegido;

	if(nodo == NULL){
		log_error(loggerFileSystem,"Volo todo a la verga");
		exit(-1);
	} else {

		if(nodo->conectado){

			nodoElegido = nodo;

		} else {

			nombreNodo = bloque->copia2->nodo;
			nodo = list_find(tablaNodos->nodos, (void *) buscaNodo);

			if(nodo == NULL){
				log_error(loggerFileSystem,"Volo todo a la verga");
				exit(-1);
			} else {

				if(nodo->conectado){

					nodoElegido = nodo;

				} else {

					return -1;

				}

			}

		}

	}

	char * stringArchivo = obtenerBloque(nodoElegido->socket,bloque->nro);
	//char* stringFinal = string_substring_until(stringArchivo, bloque->bytes);
	//free(stringArchivo);

	printf("%s", stringArchivo);
	free(stringArchivo);

	return 0;

}

void catArchivo(char *path){

	char ** pathDesc = string_split(path,"/");
	char * nombre = obtenerNombreUltimoPath(pathDesc);

	int idPadre = obtenerIdPadreArchivo(pathDesc,0,-1);

	if(idPadre == -2){
		printf("Path inexistente \n");
		log_warning(loggerFileSystem,"Path inexistente");
	} else {

		strArchivo * archivo = buscaArchivo(nombre,idPadre);

		if(archivo == NULL){
			printf("Path inexistente \n");
			log_warning(loggerFileSystem,"Path inexistente");
		} else {

			//char * cat = string_new();

			t_list * listaBloques = archivo->bloques;

			int i=0;
			int cod=0;
			while(i<list_size(listaBloques)){

				strBloqueArchivo * bloque = list_get(listaBloques,i);

				if((cod = funcionCat(bloque)) == -1){
					break;
				}

				i++;
			}

			if(cod == -1){
				//free(cat);
				printf("%s","No hay suficientes copias de bloques.");
				log_warning(loggerFileSystem,"No hay suficientes copias de bloques para realizar cat");
			} else {
				//printf("%s",cat);
				//free(cat);
			}

		}

	}

}

void liberarBloque(char* nombreNodo, uint32_t nroBloque){
	bool esElNodoBitmap(strBitmaps* nodoBitmapSeleccionado){
		return (strcmp(nodoBitmapSeleccionado->nodo,nombreNodo)==0);
	}
	strBitmaps* nodoBitmap=list_find(listaBitmaps,(void*)esElNodoBitmap);

	if(nodoBitmap==NULL){
		log_error(loggerFileSystem,"No se encontro el %s con su bitmaps",nombreNodo);
		exit(-1);
	}

	bitarray_clean_bit(nodoBitmap->bitarray, nroBloque);

}

void liberarArchivoYPersistir(strArchivo* archivoVictima){

	char* pathArchivoEnMetadata=obtenerPathArchivo(archivoVictima->directorioPadre);
	string_append(&pathArchivoEnMetadata,archivoVictima->nombre);

	bool esElPathMetadata(char* pathSeleccionado){
		return (strcmp(pathSeleccionado,pathArchivoEnMetadata)==0);
	}
	char* pathVictima=list_remove_by_condition(listaRegistroDeArchivosGuardados,(void*)esElPathMetadata);
	free(pathVictima);

	char* comando=string_new();
	string_append(&comando,"rm ");
	string_append(&comando,pathArchivoEnMetadata);
	system(comando);
	free(comando);

	persistirRegistroArchivo();
}

int sacarCantidadbloqueLibre(strNodo* nodo){
	int cantBloquesLibre=0;
	int contador=0;

	bool esElNodoBitmap(strBitmaps* nodoBitmapSeleccionado){
		return (strcmp(nodoBitmapSeleccionado->nodo,nodo->nombre)==0);
	}
	strBitmaps* nodoBitmap=list_find(listaBitmaps,(void*)esElNodoBitmap);


	for(;contador < nodo->tamanioTotal;contador++){
		if(!bitarray_test_bit(nodoBitmap->bitarray,contador)){
			cantBloquesLibre++;
		}
	}
	return cantBloquesLibre;
}

void actualizarEstructurasNodos(){
	uint32_t cantidadDeNodoEnElSistema=list_size(tablaNodos->nodos);
	uint32_t contador=0;
	tablaNodos->tamanioFSLibre=0;
	while(contador<cantidadDeNodoEnElSistema){
		strNodo* nodoElegido=list_get(tablaNodos->nodos,contador);
		nodoElegido->tamanioLibre=sacarCantidadbloqueLibre(nodoElegido);
		tablaNodos->tamanioFSLibre+=nodoElegido->tamanioLibre;
		nodoElegido->porcentajeOscioso=(nodoElegido->tamanioLibre*100)/nodoElegido->tamanioTotal;
		contador++;
	}
	persistirTablaNodo();
}

void borrarBloquesArchivos(strArchivo* archivoVictima){


	uint32_t cantidadDeBloques=list_size(archivoVictima->bloques);
	uint32_t contador=0;

	while(contador<cantidadDeBloques){
		strBloqueArchivo* bloqueArchivo=list_get(archivoVictima->bloques,contador);

		liberarBloque(bloqueArchivo->copia1->nodo,bloqueArchivo->copia1->nroBloque);
		//tablaNodos->tamanioFSLibre++;

		liberarBloque(bloqueArchivo->copia2->nodo,bloqueArchivo->copia2->nroBloque);
		//tablaNodos->tamanioFSLibre++;

		contador++;
	}

	bool destruirBloques(strBloqueArchivo* bloqueSeleccionado){
		free(bloqueSeleccionado->copia1->nodo);
		free(bloqueSeleccionado->copia2->nodo);
		free(bloqueSeleccionado->copia1);
		free(bloqueSeleccionado->copia2);
		free(bloqueSeleccionado);
	}
	list_destroy_and_destroy_elements(archivoVictima->bloques,(void*)destruirBloques);

	liberarArchivoYPersistir(archivoVictima);
	actualizarEstructurasNodos();
}
