/*
 * funcionesConsola.c
 *
 *  Created on: 7/12/2017
 *      Author: utnso
 */

#include "funcionesConsola.h"

bool buscarArchivo(char * path){

	char ** pathDesc = string_split(path,"/");

	int idPadreArchivo = obtenerIdPadreArchivo(pathDesc,0,-1);
	char * nombreArchivo = obtenerNombreUltimoPath(pathDesc);

	strArchivo * archivo = buscaArchivo(nombreArchivo,idPadreArchivo);

	if(archivo == NULL){
		return true;
	} else {

		return false;
	}

}


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

	/*int i = 0;
	while(pathDesc[i] != NULL){
		if(strcmp(pathDesc[i],"yamafs:") == 0){
			free(pathDesc);
			return true;
		} else {
			i++;
		}
	}
	free(pathDesc);*/
	return strcmp(pathDesc[0],"yamafs:") == 0;
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

	//OBTENGO EL NOMBRE DEL ARCHIVO
	char** rutaArchivo=string_split(pathArchivo,"/");
	char** rutaDirectorio = string_split(pathDirectorio,"/");
	//TENGO QUE LIBERARLO
	char* nombreArchivo=obtenerNombreUltimoPath(rutaArchivo);
	string_append(&pathDirectorio,nombreArchivo);


	//Si existe el archivo en ese directorio
	if(!buscarArchivo(pathDirectorio)){
		printf("El archivo que se desea alamacenar ya existe en yamafs. \n");
		log_warning(loggerFileSystem,"El archivo que se desea alamacenar ya existe en yamafs.");
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


	string_append(&entradaArchivoAGuardar->nombre,nombreArchivo);

		//HAY QUE LIBERAR RUTA ARHICHIVO

		//OBTENGO EL INDEX DEL DIRECTORIO PARDRE
	entradaArchivoAGuardar->directorioPadre=obtenerIdDirectorio(rutaDirectorio,0,-1);

		//OBTENGO EL TIPO
	string_append(&entradaArchivoAGuardar->tipo,tipo);

	liberarRutaDesarmada(rutaArchivo);
	liberarRutaDesarmada(rutaDirectorio);

	t_list* posicionesBloquesAGuardar=list_create();

	printf("Se procede a calcular la cantidad de bloques que ocupa el archivo %s\n",nombreArchivo);
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

	printf("Se Procede almacenar el %s. \n",nombreArchivo);
	log_trace(loggerFileSystem,"Se Procede almacenar el %s.",nombreArchivo);
	enviarDatosANodo(posicionesBloquesAGuardar,archivoALeer,entradaArchivoAGuardar);
	log_info(loggerFileSystem,"Se ha almacenado correctamente el archivo %s. \n",nombreArchivo);
	list_add(tablaArchivos,entradaArchivoAGuardar);

	mostrarEstadoDelSistemaNodos();

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
	if(indexDir<100){

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

		if(idDir==0){
			return idDir;
		}else{
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
	
	if(FD_ISSET(socket,&socketClientesAuxiliares)){
		FD_CLR(socket,&socketClientesAuxiliares);
	}
	
	int tamanioMsg = sizeof(uint32_t);
	void * msg = malloc(tamanioMsg);
	memcpy(msg,&nroBloque,sizeof(uint32_t));
	
	sendRemasterizado(socket,ENV_LEER,tamanioMsg,msg);
	free(msg);
	uint32_t noti = recibirUInt(socket);
	void* string = malloc(SIZEBLOQUE);
	if(recv(socket, string, SIZEBLOQUE, MSG_WAITALL) == -1){
		perror("Error al recibir un string.");
		exit(-1);
	}
	
	char* stringRecibido = string_substring_until(string, SIZEBLOQUE);
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
	strCopiaArchivo* copiaElegida;

	if(nodo == NULL){
		log_error(loggerFileSystem,"Volo todo a la verga");
		exit(-1);
	} else {

		if(nodo->conectado){

			nodoElegido = nodo;
			copiaElegida=bloque->copia1;

		} else {

			nombreNodo = bloque->copia2->nodo;
			nodo = list_find(tablaNodos->nodos, (void *) buscaNodo);

			if(nodo == NULL){
				log_error(loggerFileSystem,"Volo todo a la verga");
				exit(-1);
			} else {

				if(nodo->conectado){

					nodoElegido = nodo;
					copiaElegida=bloque->copia2;

				} else {

					return -1;

				}

			}

		}

	}

	char * stringArchivo = obtenerBloque(nodoElegido->socket,copiaElegida->nroBloque);
	char * stringFinal = string_substring_until(stringArchivo, bloque->bytes);
	free(stringArchivo);

	printf("%s", stringFinal);
	free(stringFinal);

	return 0;

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

int sacarCantidadBloquesLibres(strNodo* nodo){
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
		nodoElegido->tamanioLibre=sacarCantidadBloquesLibres(nodoElegido);
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

	void destruirBloques(strBloqueArchivo* bloqueSeleccionado){
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


void crearDirectorios(char* raiz, char* cantDirectorios){
	uint32_t contador=0;
	int nDirectorios=atoi(cantDirectorios);
	while(contador<nDirectorios){
		char* directorioBase=string_from_format("%s%d",raiz,(contador+1));
		int cod = crearDirectorio(directorioBase);

		if(cod == -1){
			printf("Se ha alcanzado el limite de directorios posibles \n");
			log_warning(loggerFileSystem, "Se ha alcanzado el limite de directorios posibles");
			return;
		} else if(cod == -2){
			printf("Path incorrecto. Contenia archivos \n");
			log_warning(loggerFileSystem, "Path incorrecto. Contenia archivos");
			return;
		} else if(cod == -3){
			printf("Path inexistente \n");
			log_warning(loggerFileSystem, "Path inexistente");
			return;
		} else if(cod == -4){
			printf("Ya existe un directorio con ese nombre \n");
			log_warning(loggerFileSystem, "Ya existe un directorio con ese nombre");
			return;
		} else {
			printf("Se ha creado el directorio correctamente \n");
			log_info(loggerFileSystem,"Se ha creado el directorio correctamente");
		}

		contador++;
	}

}


int catCpto(strBloqueArchivo * bloque,FILE* archivoFSLocal){

	char * nombreNodo = bloque->copia1->nodo;

	bool buscaNodo(strNodo * x){
		return (strcmp(x->nombre,nombreNodo)==0);
	}

	strNodo * nodo = list_find(tablaNodos->nodos, (void *) buscaNodo);
	strNodo * nodoElegido;
	strCopiaArchivo* copiaElegida;

	if(nodo == NULL){
		log_error(loggerFileSystem,"Volo todo a la verga");
		exit(-1);
	} else {

		if(nodo->conectado){

			nodoElegido = nodo;
			copiaElegida=bloque->copia1;

		} else {

			nombreNodo = bloque->copia2->nodo;
			nodo = list_find(tablaNodos->nodos, (void *) buscaNodo);

			if(nodo == NULL){
				log_error(loggerFileSystem,"Volo todo a la verga");
				exit(-1);//ARREGLAR
			} else {

				if(nodo->conectado){

					nodoElegido = nodo;
					copiaElegida=bloque->copia2;

				} else {

					return -1;

				}

			}

		}

	}

	char * stringArchivo = obtenerBloque(nodoElegido->socket,copiaElegida->nroBloque);
	char * stringFinal = string_substring_until(stringArchivo, bloque->bytes);
	free(stringArchivo);
	
	fwrite(stringFinal,string_length(stringFinal),1,archivoFSLocal);
	fseek(archivoFSLocal,0,SEEK_END);

	free(stringFinal);

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
				printf("%s","No hay suficientes copias de bloques.\n");
				log_warning(loggerFileSystem,"No hay suficientes copias de bloques para realizar cat");
			}

		}

	}

}

int cpto(char * pathYamaFs, char* pathLocal){

	char ** pathDesc = string_split(pathYamaFs,"/");
	int idPadre = obtenerIdPadreArchivo(pathDesc,0,-1);

	if(idPadre == -2){
		printf("Path inexistente \n");
		log_warning(loggerFileSystem,"Path inexistente");
	} else {

		char * nombre = obtenerNombreUltimoPath(pathDesc);

		strArchivo * archivo = buscaArchivo(nombre,idPadre);

		if(archivo == NULL){
			return -1;
		} else {

			FILE* archivoFSLocal=fopen(pathLocal,"w+");

			t_list * listaBloques = archivo->bloques;

			int i=0;
			int cod=0;
			while(i<list_size(listaBloques)){

				strBloqueArchivo * bloque = list_get(listaBloques,i);

				if((cod = catCpto(bloque,archivoFSLocal)) == -1){
					break;
				}

				i++;
			}

			if(cod == -1){
				fclose(archivoFSLocal);
				return -2;
			}

			fclose(archivoFSLocal);
			return 0;

		}

	}

}

void funcionLs(char* pathDirectorio){
    char ** pathDesc = string_split(pathDirectorio,"/");
    uint32_t index=obtenerIdDirectorio(pathDesc,0,-1);
    int cont=0;
    bool esElIDPadre(strArchivo* archivoSeleccionado){
        return (archivoSeleccionado->directorioPadre==index);
    }
    int cantidadArchivos=list_count_satisfying(tablaArchivos,(void*)esElIDPadre);
    log_info(loggerFileSystem,"Lista de archivos archivos del directorio: %s",pathDirectorio);

    t_list* listaConArchivos=list_filter(tablaArchivos,(void*)esElIDPadre);
    if(cantidadArchivos>0){
        while(cont<cantidadArchivos){
            strArchivo* archivoElegido=list_get(listaConArchivos,cont);
            log_info(loggerFileSystem,"Nombre: %s",archivoElegido->nombre);
            cont++;
        }
    }else{
        log_info(loggerFileSystem,"No contiene archivos");
    }

    list_destroy(listaConArchivos);
}

void borrarLaEntradaViejaEnRegistroArchivosYLoMuevo(strArchivo* archPathOri, int idNuevo){
	//OBTENGO LA RUTA EN YAMA FS QUE GUARDO EN MI LISTA DE REGISTRO
	char* directorioArchivo=obtenerPathArchivo(archPathOri->directorioPadre);
	string_append(&directorioArchivo,archPathOri->nombre);
	bool esElRegistro(char* rutaArchivoSeleccionada){
		return(strcmp(rutaArchivoSeleccionada,directorioArchivo)==0);
	}
	char* rutaVieja=list_remove_by_condition(listaRegistroDeArchivosGuardados,(void*)esElRegistro);

	//RUTA NUEVA
	char* directorioNuevoArchivo=obtenerPathArchivo(idNuevo);
	string_append(&directorioNuevoArchivo,archPathOri->nombre);

	char* comando=string_new();
	string_append(&comando,"mv ");
	string_append(&comando,rutaVieja);
	string_append(&comando," ");
	string_append(&comando,directorioNuevoArchivo);
	system(comando);
	free(comando);

	comando=string_new();
	string_append(&comando,"rm -f ");
	string_append(&comando,rutaVieja);
	system(comando);
	free(comando);

	free(rutaVieja);
	free(directorioArchivo);
}

int moverPath(char * pathOri, char * pathFin){

	char ** pathFinDesc = string_split(pathFin,"/");

	int idPadrePathFin = obtenerIdPadreDirectorio(pathFinDesc,0,-1);
	char * nombrePathFin = obtenerNombreUltimoPath(pathFinDesc);

	strDirectorio * dirPathFin = buscaDirectorio(nombrePathFin,idPadrePathFin);

	if(dirPathFin == NULL){
		free(nombrePathFin);
		//liberarChar(pathFinDesc);
		return -1;
	} else {

		char ** pathOriDesc = string_split(pathOri,"/");

		int idPadrePathOri = obtenerIdPadreDirectorio(pathOriDesc,0,-1);
		char * nombrePathOri = obtenerNombreUltimoPath(pathOriDesc);

		strDirectorio * dirPathOri = buscaDirectorio(nombrePathOri,idPadrePathOri);

		if(dirPathOri == NULL){

			idPadrePathOri = obtenerIdPadreArchivo(pathOriDesc,0,-1);
			strArchivo * archPathOri = buscaArchivo(nombrePathOri,idPadrePathOri);

			if(archPathOri == NULL){
				free(nombrePathFin);
				free(nombrePathOri);
				//liberarChar(pathOriDesc);
				//liberarChar(pathFinDesc);
				return -2;
			} else {

				int idNuevo = obtenerIdDirectorio(pathFinDesc,0,-1);
				borrarLaEntradaViejaEnRegistroArchivosYLoMuevo(archPathOri,idNuevo);
				archPathOri->directorioPadre=idNuevo;
				persistirArchivo(archPathOri);
				//liberarChar(pathFinDesc);
				//liberarChar(pathOriDesc);
				free(nombrePathOri);

			}

		} else {
			if(dirPathOri->index==0){
				free(nombrePathFin);
				free(nombrePathOri);
				//liberarChar(pathOriDesc);
				//liberarChar(pathFinDesc);
				return -3;
			}
			int idNuevo = obtenerIdDirectorio(pathFinDesc,0,-1);
			dirPathOri->padre=idNuevo;
			persistirTablaDirectorio();
			//liberarChar(pathOriDesc);
			//liberarChar(pathFinDesc);
			free(nombrePathOri);
		}

		//liberarChar(pathFinDesc);

	}

	//liberarChar(pathFinDesc);
	free(nombrePathFin);

}

void mostrarContenido(char* nombreArchivo,uint32_t idPadre){
 	bool esElArchivo(strArchivo* archivoSeleccionado){
 		return (strcmp(archivoSeleccionado->nombre,nombreArchivo)==0) && archivoSeleccionado->directorioPadre==idPadre;
 	}
 	strArchivo* archivoBuscado=list_find(tablaArchivos,(void*)esElArchivo);

 	if(archivoBuscado==NULL){
 		printf("EL archivo %s que se quiere burcas no existe.\n",nombreArchivo);
 		log_error(loggerFileSystem,"EL archivo %s que se quiere burcas no existe",nombreArchivo);
 }

 	printf("Nombre: %s\n", archivoBuscado->nombre);
 	printf("Tamanio: %i\n", archivoBuscado->tamanio);
 	printf("Tipo: %s\n", archivoBuscado->tipo);
 	printf("Directorio Padre: %i\n", archivoBuscado->directorioPadre);

 	uint32_t cantidadDebloques=list_size(archivoBuscado->bloques);
 	uint32_t contador=0;
  	while(contador<cantidadDebloques){
 		strBloqueArchivo* bloqueSeleccionado=list_get(archivoBuscado->bloques,contador);
 		printf("Bloque: %i\n", bloqueSeleccionado->nro);
 		printf("Copia 0: Nodo: %s | Bloque: %i \n", bloqueSeleccionado->copia1->nodo,bloqueSeleccionado->copia1->nroBloque);
 		printf("Copia 1: Nodo: %s | Bloque: %i \n", bloqueSeleccionado->copia2->nodo,bloqueSeleccionado->copia2->nroBloque);
 		printf("Bytes: %i\n", bloqueSeleccionado->bytes);
 		contador++;
 	}


 }
