/*
 * principalesFS.c
 *
 *  Created on: 3/12/2017
 *      Author: utnso
 */

#include "principalesFS.h"

void 	destuirMetadata(){
	char* comando=string_new();
	string_append(&comando,"rm -r -f ");
	string_append(&comando,PATH_METADATA);
	system(comando);
	free(comando);
}

void limpiarEStructurasAdministrativas(){
	liberarListaRegistroArchivos();
	liberarTablaDirectorios();
	liberarTablaArchivos();
}

void inicializarDirectoriosPrincipales(){

	char* comandoPathDirectorioRaiz=string_new();
	string_append(&comandoPathDirectorioRaiz,"mkdir ");
	string_append(&comandoPathDirectorioRaiz,PATH_METADATA);
	system(comandoPathDirectorioRaiz);
	free(comandoPathDirectorioRaiz);

	comandoPathDirectorioRaiz=string_new();
	string_append(&comandoPathDirectorioRaiz,"mkdir ");
	string_append(&comandoPathDirectorioRaiz,PATH_METADATA);
	string_append(&comandoPathDirectorioRaiz,"/archivos");
	system(comandoPathDirectorioRaiz);
	free(comandoPathDirectorioRaiz);

	comandoPathDirectorioRaiz=string_new();
	string_append(&comandoPathDirectorioRaiz,"mkdir ");
	string_append(&comandoPathDirectorioRaiz,PATH_METADATA);
	string_append(&comandoPathDirectorioRaiz,"/bitmaps");
	system(comandoPathDirectorioRaiz);
	free(comandoPathDirectorioRaiz);
}


char * obtenerPathTablaNodo(){
	char * path = string_new();
	string_append(&path, PATH_METADATA);
	string_append(&path, "/nodos.bin");
	return path;
}

char * obtenerPathDirectorio(){
	char * path = string_new();
	string_append(&path, PATH_METADATA);
	string_append(&path, "/directorios.dat");
	return path;
}

char * obtenerPathArchivo(uint directorioPadre){
	char * path = string_new();
	string_append(&path, PATH_METADATA);
	string_append(&path, "/archivos/");
	char* directorioPapa=string_itoa(directorioPadre);
	string_append(&path, directorioPapa);
	string_append(&path, "/");
	free(directorioPapa);
	return path;
}

t_bitarray * crearBitmap(int cantBloques){

	int tamanioBitarray=cantBloques/8;
	if(cantBloques % 8 != 0){
	  tamanioBitarray++;
	 }

	char* bits=malloc(tamanioBitarray);

	t_bitarray * bitarray = bitarray_create_with_mode(bits,tamanioBitarray,MSB_FIRST);

	int cont=0;
	for(; cont < tamanioBitarray*8; cont++){
		bitarray_clean_bit(bitarray, cont);
	}

	return bitarray;
}

char * obtenerPathBitmap(char * nombreNodo){
	char * path = string_new();
	string_append(&path, PATH_METADATA);
	string_append(&path, "/bitmaps/");
	string_append(&path, nombreNodo);
	string_append(&path, ".dat");
	return path;
}

void liberarListaRegistroArchivos(){
	void destruirElemento(char* rutaArchivo ){
		free(rutaArchivo);
	}

	if(list_is_empty(listaRegistroDeArchivosGuardados)){
		list_destroy(listaRegistroDeArchivosGuardados);
	} else {
		list_destroy_and_destroy_elements(listaRegistroDeArchivosGuardados, (void *) destruirElemento);
	}

}

void liberarlistaConexionNodos(){

	void destruirElemento(strConexiones * datosConexionNodo){
		free(datosConexionNodo->ip);
		free(datosConexionNodo->nodo);
		free(datosConexionNodo);
	}

	if(list_is_empty(listaConexionesNodos)){
		list_destroy(listaConexionesNodos);
	} else {
		list_destroy_and_destroy_elements(listaConexionesNodos, (void *) destruirElemento);
	}

}


void handlerSIGINT(){
	log_warning(loggerFileSystem, "SIGINT enviado, cerrando el proceso");
	liberarMemoria();
}

void liberarMemoria(){
	pthread_cancel(hiloConsolaFS);
	liberarListaRegistroArchivos();
	liberarlistaConexionNodos();
	liberarTablaDirectorios();
	liberarTablaNodos();
	liberarTablaArchivos();
	liberarBitmaps();
	//list_destroy_and_destroy_elements(socketsDatanode,(void *)free);
	free(commandChar);
	free(PATH_METADATA);
	log_destroy(loggerFileSystem);
	exit(0);
}


void liberarTablaDirectorios(){

	void destruirElemento(strDirectorio * directorio){
		free(directorio->nombre);
		free(directorio);
	}

	if(list_is_empty(tablaDirectorios)){
		list_destroy(tablaDirectorios);
	} else {
		list_destroy_and_destroy_elements(tablaDirectorios, (void *) destruirElemento);
	}

}

void liberarTablaNodos(){

	void destruirElemento(strNodo * nodo){
		free(nodo->nombre);
		free(nodo);
	}

	if(list_is_empty(tablaNodos->nodos)){
		list_destroy(tablaNodos->nodos);
	} else {
		list_destroy_and_destroy_elements(tablaNodos->nodos, (void *) destruirElemento);
	}

//	void destruirNombres(char * nombreNodo){
//		free(nombreNodo);
//	}
//
//	if(list_is_empty(tablaNodos->listaNodos)){
//		list_destroy(tablaNodos->listaNodos);
//	} else {
//		list_destroy_and_destroy_elements(tablaNodos->listaNodos, (void *) destruirNombres);
//	}
	list_destroy(tablaNodos->listaNodos);
	//free(tablaNodos);

}

void liberarTablaArchivos(){


	void destruirElemento(strArchivo * archivo){

		free(archivo->nombre);
		free(archivo->tipo);

		void destruirElementoPrima(strBloqueArchivo * bloqueArchivo){
			free(bloqueArchivo->copia1->nodo);
			free(bloqueArchivo->copia2->nodo);
			free(bloqueArchivo->copia1);
			free(bloqueArchivo->copia2);
			free(bloqueArchivo);
		}

		list_destroy_and_destroy_elements(archivo->bloques, (void *) destruirElementoPrima);
		free(archivo);

	}

	list_destroy_and_destroy_elements(tablaArchivos,(void *) destruirElemento);

}

void liberarBitmaps(){
	void destruirElemento(strBitmaps * bitmapNodo){
		free(bitmapNodo->nodo);
		free(bitmapNodo->bitarray->bitarray);//AVERIGUAR
		bitarray_destroy(bitmapNodo->bitarray);
		free(bitmapNodo);
	}

	if(list_is_empty(listaBitmaps)){
		list_destroy(listaBitmaps);
	} else {
		list_destroy_and_destroy_elements(listaBitmaps, (void *) destruirElemento);
	}
}

void iniciarTablaDeDirectorios(){
	strDirectorio* directorioPrincipal=malloc(sizeof(strDirectorio));
	directorioPrincipal->nombre=string_new();
	directorioPrincipal->index=0;
	string_append(&directorioPrincipal->nombre,"yamafs:");
	directorioPrincipal->padre=-1;
	list_add(tablaDirectorios,directorioPrincipal);
	persistirTablaDirectorio();
}

void chequearParametrosFS(int cantPar, char * par){

	if(cantPar < 2 || cantPar > 3){
		printf("Error con la cantidad de parametros\n");
		log_error(loggerFileSystem,"Cantidad de parametros incorrecta al iniciar el proceso");
		liberarMemoria();
		exit(-1);
	} else {

		if(cantPar == 3){

			if(strcmp(par,"--clean")==0){

				char* comandoReseteo=string_new();
				string_append(&comandoReseteo,"rm -r ");
				string_append(&comandoReseteo,PATH_METADATA);
				system(comandoReseteo);
				free(comandoReseteo);

			} else {

				printf("Flag inexistente\n");
				log_error(loggerFileSystem,"Flag inexistente");
				liberarMemoria();
				exit(-1);

			}

		}

	}

}


void cargarConfigFS(t_config* configuracionFS) {

	if (!config_has_property(configuracionFS, "PUERTO_ESCUCHA")) {
		printf("No se encuentra el parametro PUERTO_ESCUCHA en el archivo de configuracion");
		log_error(loggerFileSystem,"No se encuentra el parametro PUERTO_ESCUCHA en el archivo de configuracion");
		liberarMemoria();
		exit(-1);
	}

	PUERTO_ESCUCHA = config_get_int_value(configuracionFS,"PUERTO_ESCUCHA");


	if (!config_has_property(configuracionFS, "PATH_METADATA")) {
		printf("No se encuentra el parametro PATH_METADATA en el archivo de configuracion");
		log_error(loggerFileSystem,"No se encuentra el parametro PATH_METADATA en el archivo de configuracion");
		config_destroy(configuracionFS);
		liberarMemoria();
		exit(-1);
	}

	PATH_METADATA = string_new();
	string_append(&PATH_METADATA, config_get_string_value(configuracionFS, "PATH_METADATA"));

	config_destroy(configuracionFS);

}


void limpiarFS(){
	log_warning(loggerFileSystem,"Se ha cortado al proceso de forma abrupta");
	liberarMemoria();
}


int iniciarServidor(int puerto){
	struct sockaddr_in mySocket;
	int socketListener = socket(AF_INET, SOCK_STREAM, 0);
	verificarErrorSocket(socketListener);
	verificarErrorSetsockopt(socketListener);
	mySocket.sin_family = AF_INET;
	mySocket.sin_port = htons(puerto);
	mySocket.sin_addr.s_addr = INADDR_ANY;
	memset(&(mySocket.sin_zero), '\0', 8);
	verificarErrorBind(socketListener, mySocket);
	verificarErrorListen(socketListener);
	return socketListener;
}

void iniciarEstructuras(){

	//Inicio mutex
    pthread_mutex_init(&mutex, NULL);

	// Tabla de directorios
	tablaDirectorios = list_create();


	// Tabla de nodos
	tablaNodos = (strTablaNodos*)malloc(sizeof(strTablaNodos));
	tablaNodos->tamanioFSLibre = 0;
	tablaNodos->tamanioFSTotal = 0;
	tablaNodos->listaNodos=list_create();
	tablaNodos->nodos = list_create();

	// Tabla de archivos
	tablaArchivos = list_create();

	//Registro de archivos guardados en el sistema
	listaRegistroDeArchivosGuardados=list_create();

	// Bitmaps
	listaBitmaps = list_create();

	//Conexiones De nodos
	listaConexionesNodos = list_create();

	commandChar = malloc(100*sizeof(char));

	seDesconectoUnNodo=false;

	sistemaFormateado = false;

	envioDeInformacionADataNode=true;

}

void atenderConexion(int socketNuevo){

	int socketAceptado = aceptarConexionDeCliente(socketNuevo);
	FD_SET(socketAceptado, &socketClientes);
	socketMaximo = calcularSocketMaximo(socketAceptado,socketMaximo);
	log_info(loggerFileSystem,"Se ha recibido una nueva conexion");

}

void atenderNotificacion(int socket){

	int nroNotificacion = recvDeNotificacion(socket);

	switch(nroNotificacion){

		case ES_DATANODE:
			sendDeNotificacion(socket, ES_FS);
			sendDeNotificacion( socket,PEDIR_INFONODO);
			break;

		case ES_WORKER:
			log_info(loggerFileSystem,"Se ha conectado un Worker");
			if(!estadoEstable){
				log_error(loggerFileSystem, "No se encuentra en un estado seguro. Cerrando conexion con Worker");
				FD_CLR(socket, &socketClientes);
				close(socket);
			} else {
				sendDeNotificacion(socket, ES_FS);
			}
			break;

		case ES_YAMA:
			log_info(loggerFileSystem,"Se ha conectado una YAMA");
			if (estadoEstable) {
				enviarListaNodos(socket);
			} else {
				log_error(loggerFileSystem, "No se encuentra en un estado seguro. Cerrando conexion con YAMA");
				FD_CLR(socket, &socketClientes);
				close(socket);
			}
			break;

		case REC_INFONODO:

			log_info(loggerFileSystem,"Registrando Datanode");
			registrarNodo(socket);
			break;

		case INFO_ARCHIVO_FS:
			enviarDatoArchivo(socket);
			break;
		case DATOS_NODO:
			enviarDatosConexionNodo(socket);
			break;

		case ALMACENADO_FINAL:
			almacenarArchivoWorker(socket);
			break;

		case 0:
			log_warning(loggerFileSystem, "El socket %d corto la conexion", socket);
			verificarSiNodo(socket);
			FD_CLR(socket, &socketClientes);
			close(socket);
			break;

		default:
			log_warning(loggerFileSystem, "La conexion recibida es erronea");
			//sacarSocket(socket);
			FD_CLR(socket, &socketClientes);
			close(socket);
			break;

	}

}

void verificarSiNodo(int socket){

	bool esElNodoDesconectado(strNodo* nodoSeleccionado){
			return (nodoSeleccionado->socket==socket);
	}

	if(estadoAnterior==true || sistemaFormateado==true){
		strNodo* nodoDesconectado=list_find(tablaNodos->nodos,(void*)esElNodoDesconectado);

		if(nodoDesconectado!=NULL){
			log_error(loggerFileSystem,"Se ha desconectado un DataNode del sistema.");
			tablaNodos->tamanioFSTotal-=nodoDesconectado->tamanioTotal;
			tablaNodos->tamanioFSLibre-=nodoDesconectado->tamanioLibre;
			nodoDesconectado->conectado=false;
			seDesconectoUnNodo=true;
			persistirTablaNodo();
		}
	}else{

		if(sistemaFormateado==false || estadoAnterior==false){

			strNodo * nodoDesconectado = list_remove_by_condition(tablaNodos->nodos,(void *)esElNodoDesconectado);

			if(nodoDesconectado != NULL){
				log_warning(loggerFileSystem,"Era un datanode, se ignora porque no se hizo format y el sistema no presenta estado anterior");
				bool borrarNodo(char * nodoSeleccionado){
					return(strcmp(nodoSeleccionado,nodoDesconectado->nombre)==0);
				}
				list_remove_by_condition(tablaNodos->listaNodos,(void *)borrarNodo);
				tablaNodos->tamanioFSTotal-=nodoDesconectado->tamanioTotal;
				tablaNodos->tamanioFSLibre-=nodoDesconectado->tamanioLibre;
			}
		}
	}
}
