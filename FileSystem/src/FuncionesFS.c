/*
 * FuncionesFS.c
 *
 *  Created on: 25/11/2017
 *      Author: utnso
 */

#include "FuncionesFS.h"
#include "../../Biblioteca/src/Socket.h"

//------------------------------BUSCA PATHS

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

}

//------------------------------LIBERO MEMORIA

void liberarCopiasXBloque(copiasXBloque* copiaBloque){
	free(copiaBloque->bloque);
	free(copiaBloque->copia1->nodo);
	free(copiaBloque->copia2->nodo);
}

void liberarArchivo(tablaArchivos* entradaArchivo){
	list_destroy_and_destroy_elements(entradaArchivo->bloques,(void*)liberarCopiasXBloque);
	free(entradaArchivo->nombreArchivo);
	free(entradaArchivo->tipo);
}

void liberarListaNodos(char* nodo){
	free(nodo);
}

void liberarContenidoNodo(contenidoNodo* contenidoDelNodo){
	free(contenidoDelNodo->nodo);
}

void liberarBitmaps(tablaBitmapXNodos* entradaBitmap){
	bitarray_destroy(entradaBitmap->bitarray);
	free(entradaBitmap->nodo);
}

void liberarDirectorios(t_directory* entradaDirectorio){
	free(entradaDirectorio->nombre);
}

void liberarConexionNodo(datosConexionNodo* entradaConexionNodo){
	free(entradaConexionNodo->ip);
	free(entradaConexionNodo->nodo);
}

void liberarRegistroArchivos(char* nombre){
	free(nombre);
}

void killMe(int signal){
	list_destroy_and_destroy_elements(tablaGlobalArchivos, (void*)liberarArchivo);
	list_destroy_and_destroy_elements(tablaGlobalNodos->nodo,(void*)liberarListaNodos);
	list_destroy_and_destroy_elements(tablaGlobalNodos->contenidoXNodo,(void*)liberarContenidoNodo);
	list_destroy_and_destroy_elements(listaBitmap,(void*)liberarBitmaps);
	list_destroy_and_destroy_elements(listaDirectorios,(void*)liberarDirectorios);
	list_destroy_and_destroy_elements(listaConexionNodos,(void*)liberarConexionNodo);
	list_destroy_and_destroy_elements(registroArchivos,(void*)liberarRegistroArchivos);
	close(socketEscuchaFS);
	close(socketClienteChequeado);
	log_destroy(loggerFileSystem);
	free(PATH_METADATA);
	free(PATH_BITMAPS);
	free(PATH_ARCHIVOS);
	free(PATH_PADRE);
}




//------------------------------INICIALIZACION

void inicializarDirectoriosPrincipales(){

	char* comandoPathDirectorioRaiz=string_new();
	string_append(&comandoPathDirectorioRaiz,"mkdir ");
	string_append(&comandoPathDirectorioRaiz,PATH_PADRE);
	system(comandoPathDirectorioRaiz);
	free(comandoPathDirectorioRaiz);

	comandoPathDirectorioRaiz=string_new();
	string_append(&comandoPathDirectorioRaiz,"mkdir ");
	string_append(&comandoPathDirectorioRaiz,PATH_METADATA);
	system(comandoPathDirectorioRaiz);
	free(comandoPathDirectorioRaiz);

	comandoPathDirectorioRaiz=string_new();
	string_append(&comandoPathDirectorioRaiz,"mkdir ");
	string_append(&comandoPathDirectorioRaiz,PATH_ARCHIVOS);
	system(comandoPathDirectorioRaiz);
	free(comandoPathDirectorioRaiz);

	comandoPathDirectorioRaiz=string_new();
	string_append(&comandoPathDirectorioRaiz,"mkdir ");
	string_append(&comandoPathDirectorioRaiz,PATH_BITMAPS);
	system(comandoPathDirectorioRaiz);
	free(comandoPathDirectorioRaiz);

	t_directory* directorioPadre = malloc(sizeof(t_directory));
	directorioPadre->nombre=string_new();
	directorioPadre->index=0;
	directorioPadre->nombre="yamafs:";
	directorioPadre->padre=-1;
	list_add(listaDirectorios,directorioPadre);
}


//------------------------FUNCIONES DIRECTORIOS

void liberarComandoDesarmado(char** comandoDesarmado){
	int i = 0;
	while(comandoDesarmado[i]!=NULL){
		free(comandoDesarmado[i]);
		i++;
	}
	free(comandoDesarmado);
}

void liberarDirectorio(t_directory* directorio){
  free(directorio->nombre);
  free(directorio);
}


char* obtenerNombreDirectorio(char** rutaDesmembrada){
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
}

int obtenerDirectorioPadre(char** rutaDesmembrada){
  char* fathersName = string_new();
  bool isMyFather(t_directory* directory){
    return strcmp(fathersName, directory->nombre) == 0;
  }
  int posicion = 0;
  while(1){
    if(rutaDesmembrada[posicion+1]!=NULL){
      if(rutaDesmembrada[posicion+2] == NULL){
        string_append(&fathersName, rutaDesmembrada[posicion]);
        t_directory* directory = list_find(listaDirectorios, (void*)isMyFather);
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

t_directory* createDirectory(){
  t_directory* newDirectory = malloc(sizeof(t_directory));
  return newDirectory;
}

//MOVER DIRECTORIO

void moveDirectory(char* oldPath, char* newPath){
  char** rutaDesmembradaVieja = string_split(oldPath, "/");
  char** rutaDesmembradaNueva = string_split(newPath, "/");
  char* nombreDirectorio = obtenerNombreDirectorio(rutaDesmembradaVieja);
  int indexPadreNuevo = obtenerDirectorioPadre(rutaDesmembradaNueva);
  if(indexPadreNuevo == -1 || indexPadreNuevo==-2){
	liberarComandoDesarmado(rutaDesmembradaNueva);
    liberarComandoDesarmado(rutaDesmembradaVieja);
    free(nombreDirectorio);
    log_error(loggerFileSystem,"Error al encontrar el directorio padre del path final");
  }
  bool esDirectorio(t_directory* directorio){
    return strcmp(directorio->nombre, nombreDirectorio) == 0;
  }

  t_directory* directorioAModificar = list_find(listaDirectorios, (void*)esDirectorio);
  if(directorioAModificar == NULL){
	  liberarComandoDesarmado(rutaDesmembradaNueva);
    liberarComandoDesarmado(rutaDesmembradaVieja);
    free(nombreDirectorio);
    log_error(loggerFileSystem,"No se ha encontrar el directorio en el sistema");
  }
  directorioAModificar->padre = indexPadreNuevo;
  persistirDirectorio();
  liberarComandoDesarmado(rutaDesmembradaNueva);
  liberarComandoDesarmado(rutaDesmembradaVieja);
  free(nombreDirectorio);
}

//EXISTE DIRECTORIO

bool existeDirectory(char* ruta){
  char** rutaDesmembrada = string_split(ruta, "/");
  char* nombreDirectory = obtenerNombreDirectorio(rutaDesmembrada);
  bool esDirectorio(t_directory* directory){
    return strcmp(directory->nombre, nombreDirectory)==0;
  }
  bool yaExiste = list_any_satisfy(listaDirectorios, (void*)esDirectorio);
  //AVERIGUAR SI HAY QUE VERIFICAR TAMBIEN EL PADRE
  free(nombreDirectory);
  liberarComandoDesarmado(rutaDesmembrada);
  return yaExiste;
}

//CREAR DIRECTORIO

int crearDirectorio(char* ruta){
  if(list_size(listaDirectorios)<=100){
      uint32_t indexDir = list_size(listaDirectorios);
      t_directory* newDirectory = createDirectory();
      char** rutaDesmembrada = string_split(ruta, "/");
      newDirectory->nombre = obtenerNombreDirectorio(rutaDesmembrada);
      newDirectory->padre = obtenerDirectorioPadre(rutaDesmembrada);
      newDirectory->index = indexDir;
      if(newDirectory->padre == -2){
    	  liberarComandoDesarmado(rutaDesmembrada);
        liberarDirectorio(newDirectory);
        log_info(loggerFileSystem,"Error de directorio padre de la ruta elegida");
        return 0;
      }
      list_add(listaDirectorios, newDirectory);
      liberarComandoDesarmado(rutaDesmembrada);
      persistirDirectorio();
      log_info(loggerFileSystem,"Se creo correctamente el directorio");
      return 1;
  }else{
	  log_error(loggerFileSystem,"No se pudo crear el directorio, se llego al limite");
	  return -1;
  }
}

//RENOMBRAR DIRECTORIO
void renameDirectory(char* oldName, char* newName){
  char** rutaDesmembradaVieja = string_split(oldName, "/");
  char** rutaDesmembradaNueva = string_split(newName, "/");
  char* viejoNombre = obtenerNombreDirectorio(rutaDesmembradaVieja);
  char* nuevoNombre = obtenerNombreDirectorio(rutaDesmembradaNueva);
  bool encontrarPorNombre(t_directory* directorio){
    return strcmp(viejoNombre, directorio->nombre);
  }
  t_directory* directoryToChange = list_find(listaDirectorios, (void*)encontrarPorNombre);
  if(directoryToChange != NULL){
    free(directoryToChange->nombre);
    directoryToChange->nombre = string_new();
    string_append(&directoryToChange->nombre, nuevoNombre);
    liberarComandoDesarmado(rutaDesmembradaNueva);
    liberarComandoDesarmado(rutaDesmembradaVieja);

    persistirDirectorio();

    free(viejoNombre);
    free(nuevoNombre);
    log_info(loggerFileSystem,"Se ha renombrado exitosamente el directori/archivo.");
  }else{
	liberarComandoDesarmado(rutaDesmembradaNueva);
    liberarComandoDesarmado(rutaDesmembradaVieja);
    free(viejoNombre);
    free(nuevoNombre);
    log_error(loggerFileSystem,"El directorio que se pide renombrar no existe.");
  }
}

//OBTENER INDEX DE LA TABLA DIRECTORIO

int obtenerIndexDirectorio(char* nombre){
  bool esDirectorio(t_directory* direct){
    return strcmp(nombre, direct->nombre);
  }
  t_directory* directorio = list_find(listaDirectorios,(void*) esDirectorio);
  if(directorio != NULL){
    return directorio->index;
  }
  return -2;
}

//BORRAR DIRECTORIO

int deleteDirectory(char* directoryToDelete){
  char** rutaDesmembrada = string_split(directoryToDelete, "/");
  char* directoryName = obtenerNombreDirectorio(rutaDesmembrada);
  int indexToDelete = obtenerIndexDirectorio(directoryName);
  if(indexToDelete == -2){
    free(directoryName);
    liberarComandoDesarmado(rutaDesmembrada);

    return 0; //NO EXISTE DIRECTORIO
  }
  bool esDirectorio(t_directory* directorio){
    return strcmp(directorio->nombre, directoryName);
  }
  bool tieneHijos(t_directory* directorio){
    return directorio->padre == indexToDelete;
  }
  if(!list_any_satisfy(listaDirectorios, (void*)tieneHijos)){
    t_directory* directoryToRemove = list_remove_by_condition(listaDirectorios, (void*)esDirectorio);

    persistirDirectorio();

    liberarDirectorio(directoryToRemove);
    liberarComandoDesarmado(rutaDesmembrada);
    free(directoryName);
    return 1;
  }else{
    free(directoryName);
    liberarComandoDesarmado(rutaDesmembrada);

    return -1; //EL DIRECTORIO TIENE SUBDIRECTORIOS
  }
}
//------------------------------------------------------------------------------
//--------------------------------------FUNCION ALMACENAR
int sacarTamanioArchivo(FILE* archivo) {
	fseek(archivo, 0, SEEK_END);
	int tamanioArchivo = ftell(archivo);
	return tamanioArchivo;
}

int asignarBloqueNodo(contenidoNodo* nodo){
	int posicion=0;
	bool esNodo(tablaBitmapXNodos* nodoConBitmap){
		return(string_equals_ignore_case(nodoConBitmap->nodo,nodo->nodo));
	}
	tablaBitmapXNodos* nodoConbitarray = list_find(listaBitmap,(void*)esNodo);
	for(;posicion<nodo->total;posicion++){
		if (!bitarray_test_bit(nodoConbitarray->bitarray, posicion)) {
			bitarray_set_bit(nodoConbitarray->bitarray,posicion);
			return posicion;
		}
	}
	//persistirBitmap(nodoConbitarray); NOD DEBERIA PORQUE POR EL MMAP YA LO HACE
	return 0;
}

void asignarEnviarANodo(void* contenidoAEnviar,uint32_t tamanio,copiasXBloque* copiaBloque){

	void* mensaje=malloc(sizeof(uint32_t)*3+tamanio);
	uint32_t posicionActual=0;
	contenidoNodo* nodo0;
	contenidoNodo* nodo1;

	bool ordenarPorPorcentajeOcioso(contenidoNodo* nodoSeleccionado1, contenidoNodo* nodoSeleccionado2){
		return(nodoSeleccionado1->porcentajeOcioso > nodoSeleccionado2->porcentajeOcioso);
	}
	list_sort(tablaGlobalNodos->contenidoXNodo,(void*)ordenarPorPorcentajeOcioso);

	nodo0=list_get(tablaGlobalNodos->contenidoXNodo,0);

	nodo0->libre--;
	nodo0->porcentajeOcioso=sacarPorcentajeOcioso(nodo0->libre,nodo0->total);
	tablaGlobalNodos->libres--;

	nodo1=list_get(tablaGlobalNodos->contenidoXNodo,1);
	nodo1->libre--;
	nodo1->porcentajeOcioso=sacarPorcentajeOcioso(nodo1->libre,nodo1->total);
	tablaGlobalNodos->libres--;

	persistirTablaNodo();

	uint32_t bloqueAsignado=asignarBloqueNodo(nodo0);

	copiaBloque->copia1=malloc(sizeof(copia));

	copiaBloque->copia1->nodo=nodo0->nodo;
	copiaBloque->copia1->bloque=bloqueAsignado;


	memcpy(mensaje,&bloqueAsignado,sizeof(uint32_t));
	posicionActual+=sizeof(uint32_t);

	memcpy(mensaje+posicionActual,&tamanio,sizeof(uint32_t));
	posicionActual+=sizeof(uint32_t);

	memcpy(mensaje+posicionActual,&tamanio,sizeof(uint32_t));
	posicionActual+=sizeof(uint32_t);

	memcpy(mensaje+posicionActual,contenidoAEnviar,tamanio);
	posicionActual+=tamanio;

	sendRemasterizado(nodo0->socket,ENV_ESCRIBIR,posicionActual,mensaje);


	if(recvDeNotificacion(nodo0->socket)==ESC_INCORRECTA){
		//CACHER ERROR
	}

	mensaje=realloc(mensaje,sizeof(uint32_t)*3+tamanio);

	bloqueAsignado=asignarBloqueNodo(nodo1);
	posicionActual = 0;
	memcpy(mensaje,&bloqueAsignado,sizeof(uint32_t));
	posicionActual+=sizeof(uint32_t);

	memcpy(mensaje+posicionActual,&tamanio,sizeof(uint32_t));
	posicionActual+=sizeof(uint32_t);

	memcpy(mensaje+posicionActual,&tamanio,sizeof(uint32_t));
	posicionActual+=sizeof(uint32_t);

	memcpy(mensaje+posicionActual,contenidoAEnviar,tamanio);
	posicionActual+=tamanio;


	sendRemasterizado(nodo1->socket,ENV_ESCRIBIR,posicionActual,mensaje);

	copiaBloque->copia2=malloc(sizeof(copia));

	copiaBloque->copia2->nodo=nodo1->nodo;
	copiaBloque->copia2->bloque=bloqueAsignado;


	if(recvDeNotificacion(nodo1->socket)==ESC_INCORRECTA){
		//CACHER ERROR
	}

	free(mensaje);

}

void enviarDatosANodo(t_list* posiciones,FILE* archivo, tablaArchivos* archivoAGuardar) {
	uint32_t posicionActual = 0;
	void enviarNodoPorPosicion(int posicion) {
		copiasXBloque* copia=malloc(sizeof(copiasXBloque));
		copia->disponible=1;
		if (posicionActual == 0) {
			void* contenido = malloc(posicion);
			fread(contenido, posicion, 1, archivo);
			copia->bloque = string_new();
			char* numeroBloque = string_itoa(posicionActual);
			string_append(&copia->bloque, numeroBloque);
			copia->bytes=posicion;
			asignarEnviarANodo(contenido, posicion,copia);
			free(contenido);
			free(numeroBloque);
		} else {
			uint32_t posicionAnterior = (int) list_get(posiciones,posicionActual-1);
			char* numeroBloque = string_itoa(posicionActual);
			copia->bloque = string_new();
			string_append(&copia->bloque, numeroBloque);
			copia->bytes=posicion- posicionAnterior;
			void* contenido = malloc(posicion - posicionAnterior);
			fread(contenido, posicion - posicionAnterior, 1,archivo);
			asignarEnviarANodo(contenido,posicion - posicionAnterior,copia);
			free(contenido);
			free(numeroBloque);
		}
		posicionActual++;
		list_add(archivoAGuardar->bloques,copia);
		list_add(tablaGlobalArchivos, archivoAGuardar);
	}
	list_iterate(posiciones,(void*) enviarNodoPorPosicion);
}

bool almacenarArchivo(char* pathArchivo, char* pathDirectorio,char* tipo) {

	// CATCHEAR ERROR ARCHIVO
	FILE* archivo = fopen(pathArchivo, "r+");

	if(archivo == NULL){
		log_error(loggerFileSystem,"Error al tratar de abrir el archivo en almacenar archivo.");
		return false;

	}else{

		if(!existeDirectory(pathDirectorio)){
			log_error(loggerFileSystem,"No existe el directorio ingresado");
			return false;
		}else{

			uint32_t tamanio = sacarTamanioArchivo(archivo);
			t_list* posicionBloquesAGuardar=list_create();

			tablaArchivos* archivoAGuardar=malloc(sizeof(tablaArchivos));
			archivoAGuardar->bloques=list_create();
			archivoAGuardar->tamanio=tamanio;

			//OBTENGO NOMBRE DEL ARCHIVO
			char** rutaArchivo = string_split(pathArchivo,"/");
			archivoAGuardar->nombreArchivo=obtenerNombreDirectorio(rutaArchivo);
			string_append(&pathDirectorio, archivoAGuardar->nombreArchivo);

			char** rutaDirectorio = string_split(pathDirectorio,"/");

			log_info(loggerFileSystem, "Se procede a almacenar el archivo %s en %s.", archivoAGuardar->nombreArchivo, pathDirectorio);
			archivoAGuardar->tipo=tipo;
			archivoAGuardar->directorioPadre=obtenerDirectorioPadre(rutaDirectorio);
			liberarComandoDesarmado(rutaArchivo);
			liberarComandoDesarmado(rutaDirectorio);
			uint32_t tamAux = 0;
			if(tablaGlobalNodos->libres*1024*1024>=(tamanio*2)){
				if(tamanio!=0){
					if (strcmp(tipo,"B")==0) {
							while (tamanio > 0) {
								if (tamanio < 1048576) {
									tamAux += tamanio;
									list_add(posicionBloquesAGuardar,tamAux);
									tamanio-=tamanio;
								} else {
									tamAux += 1048576;
									list_add(posicionBloquesAGuardar,tamAux);
									tamanio -= 1048576;
								}
							}
						} else {
							int digito;
							uint32_t ultimoBarraN = 0;
							uint32_t registroAntesMega = 0;
							uint32_t ultimaPosicion = 0;
							fseek(archivo, 0, SEEK_SET);
							while (!feof(archivo)) {
								digito = fgetc(archivo);

								if (digito == '\n') {
									ultimoBarraN = ftell(archivo);
								}

								if (ftell(archivo) == 1048576 + registroAntesMega) {
									registroAntesMega = ultimoBarraN;
									list_add(posicionBloquesAGuardar,ultimoBarraN);
								}
							}

							fseek(archivo, 0, SEEK_END);
							ultimaPosicion = ftell(archivo);
							list_add(posicionBloquesAGuardar, ultimaPosicion);
							fseek(archivo, 0, SEEK_SET);
						}
						archivoAGuardar->disponible=1;
						enviarDatosANodo(posicionBloquesAGuardar,archivo,archivoAGuardar);
						persistirTablaArchivo(archivoAGuardar);

						char* comandoCopiarArchivo = string_new();
						string_append(&comandoCopiarArchivo, "cp -a ");
						string_append(&comandoCopiarArchivo, pathArchivo);
						string_append(&comandoCopiarArchivo, " ");
						string_append(&comandoCopiarArchivo, pathDirectorio);
						system(comandoCopiarArchivo);

						free(comandoCopiarArchivo);
						log_info(loggerFileSystem,"Se ha almacenado correctamente el archivo.");
						return true;
				}else{
					log_error(loggerFileSystem,"El archivo se encuentra vacio");
					return false;
				}

			}else{
				log_error(loggerFileSystem,"El tamaño del archivo supera la capacidad de almacenamiento del sistema");
				return false;
			}
			list_destroy(posicionBloquesAGuardar);
		}

	}
}

//-------------------------------------------------------------------------------------
//------------------------------------------------LEER
//void leerArchivo(char* ruta,char* nombreArchivo){
//	int cont=0;
//	bool esArchivo(tablaArchivos* archivo){
//		return(string_equals_ignore_case(archivo->nombreArchivo,nombreArchivo));
//	}
//	tablaArchivos* entradaArchivo = list_find(tablaGlobalArchivos,(void*)esArchivo);
//
//	void* contenidoArchivo=malloc(entradaArchivo->tamanio);
//
//	while(cont<list_size(entradaArchivo->bloques)){
//		copiasXBloque* copiaBloque=(copiasXBloque*)list_get(entradaArchivo->bloques,cont);
//		bool esNodo(contenidoNodo* nodoElegido){
//			return(strcmp(nodoElegido->nodo,copiaBloque->copia1->nodo)==0);
//		}
//		contenidoNodo* nodoSeleccionado=list_find(tablaGlobalNodos->contenidoXNodo,(void*)esNodo);
//		//sendRemasterizado(nodoSeleccionado->socket,ENV_LEER,copiaBloque->)
//
//	}
//
//}




