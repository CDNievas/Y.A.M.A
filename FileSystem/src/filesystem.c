#include "consolaFS.h"
#include "structFS.h"

#define PARAMETROS {"PUERTO_ESCUCHA"}

int PUERTO_ESCUCHA;

t_log* loggerFileSystem;
int hayNodes = 0;
int esEstadoSeguro = 1;
tablaNodos* tablaGlobalNodos;
t_list* listaBitmap;
t_list* tablaGlobalArchivos;
bool hayEstadoAnterior;
t_list* listaConexionNodos;

//---------------------------------------------------BITMAP---------------------------------------------------------

char * obtenerPathBitmap(char * nombreNodo){

	// Path
	char * path = string_new();
	string_append(&path, "/metadata/bitmap/");
//	path= "/metadata/bitmap/";
	string_append(&path, nombreNodo);
	string_append(&path, ".dat");
	return path;

}

t_bitarray * abrirBitmap(char * nombreNodo,int cantBloques){

	char * path = obtenerPathBitmap(nombreNodo); // Path bitmap
	uint32_t archivo; // FD Archivo
	struct stat infoBitmap; // Guarda informacion del archivo
	void * mapArchivo; // Memoria del mmap
	t_bitarray * bitarray; // Bitarray

	// Abro el archivo
	if(stat(path,&infoBitmap) < 0){
		// Error al abrir el archivo
		log_error(loggerFileSystem,"Error al tratar de abrir el archivo.");
		exit(-1);
	}

	if((archivo = open(path, O_RDWR)) < 0){
		// Error al abrir el archivo
		log_error(loggerFileSystem,"Error al tratar de abrir el archivo.");
		exit(-1);
	}

	// Lo mapeo a memoria
	mapArchivo = mmap(0, infoBitmap.st_size, PROT_WRITE | PROT_READ, MAP_SHARED, archivo, 0);

	bitarray = bitarray_create_with_mode(mapArchivo,cantBloques,MSB_FIRST);

	if(close(archivo) < 0){
		log_error(loggerFileSystem,"Fallo al cerrar el archivo.");
		exit(-1);
	}

	return bitarray;

}

t_bitarray * crearBitmap(char * nombreNodo, int cantBloques){

	char * path = obtenerPathBitmap(nombreNodo); // Path bitmap

	if((truncate(path,cantBloques)) == -1){
		// Error al crear el archivo
		log_error(loggerFileSystem,"Error al tratar de abrir el archivo.");
		exit(-1);
	}

	return abrirBitmap(nombreNodo, cantBloques);

}

//Habria que implementar la copia de los archivos

void cargarFileSystem(t_config* configuracionFS) {
	if (!config_has_property(configuracionFS, "PUERTO_ESCUCHA")) {
		log_error(loggerFileSystem,
				"No se encuentra el parametro PUERTO_ESCUCHA en el archivo de configuracion");
		exit(-1);
	} else {
		PUERTO_ESCUCHA = config_get_int_value(configuracionFS,
				"PUERTO_ESCUCHA");
	}
	config_destroy(configuracionFS);
}

//--------------------------------Nodos---------------------------------------
int cantBloquesLibres(t_bitarray* bitarray) {
	int i = 0;
	int cont = 0;
	for (; i < bitarray_get_max_bit(bitarray); i++) {
		if (!bitarray_test_bit(bitarray, i)) {
			cont++;
		}
	}
	return cont;
}


void registrarNodo(int socket) {

	char * nombreNodo;
	int cantBloques;
	t_bitarray * bitarray;

	nombreNodo = (char*)recibirString(socket);
	cantBloques = recibirUInt(socket);

	// Checkeo estado anterior
	if(hayEstadoAnterior){
		bitarray = abrirBitmap(nombreNodo,cantBloques);

	} else {
		bitarray = crearBitmap(nombreNodo,cantBloques);

	// Creo el bloque de info del nodo
	contenidoNodo* bloque = malloc(sizeof(contenidoNodo));

	// Asigno nombre del nodo
	bloque->nodo = string_new();
	string_append(&bloque->nodo, nombreNodo);

	// Asigno cant de bloques
	bloque->total = cantBloques;

	// Asigno cant de bloques libres
	bloque->libre = cantBloquesLibres(bitarray);

	// Aniado a la tabla de info de nodos
	tablaGlobalNodos->tamanio += bloque->total;
	tablaGlobalNodos->libres += bloque->libre;
	list_add(tablaGlobalNodos->nodo, nombreNodo);
	list_add(tablaGlobalNodos->contenidoXNodo, bloque);

	// Aniado a la tala de bitmaps
	tablaBitmapXNodos* bitmapNodo = malloc(sizeof(tablaBitmapXNodos));
	string_append(&bitmapNodo->nodo, nombreNodo);
	bitmapNodo->bitarray = bitarray;
	list_add(listaBitmap, bitmapNodo);

	// Aniado a la tabla de conexiones de nodo
	nodoConexion* nodoConectado = malloc(sizeof(nodoConexion));
	nodoConectado->nodo = string_new();
	string_append(&(nodoConectado->nodo), nombreNodo);
	nodoConectado->soket= socket;
	list_add(listaConexionNodos, nodoConectado);
	}
}

//-----------------------------------------------FUNCION ALAMACENAR----------------------------------------------------
int sacarTamanioArchivo(FILE* archivo) {
	fseek(archivo, 0, SEEK_END);
	int tamanioArchivo = ftell(archivo);
	return tamanioArchivo;
}

//void asignarEnviarANodo(char* contenidoBloque, int tamanioArchivo,
//		FILE* archivo) {
//	char* contenido = malloc(tamanioArchivo);
//	fread(contenido, tamanioArchivo, 0, archivo);
//	int nodoAsignado = asignarNodo();
//	enviarANodo(nodoAsignado, contenidoBloque);
//}

void almacenarArchivo(void* mensaje, int socket) {
	char* path = string_new();
	char* nombreArchivo = string_new();
	path = recibirString(socket);
	nombreArchivo = recibirString(socket);
	char tipo = (char) recibirUInt(socket);

	FILE* archivo = fopen(path, "r+");
	int bloqueAsignado = 0;
	char* contenidoBloque = string_new();
	int tamanioArchivo = sacarTamanioArchivo(archivo);

	if (tipo == "B") {

		int tamanio = sacarTamanioArchivo(archivo);
		void * contenidoAEnviar = malloc(1048576);
		while (tamanio >= 0) {
			if (tamanio < 1048576) {
				fread(contenidoAEnviar, tamanio, 1, archivo);
//				asignarEnviarANodo(contenidoAEnviar, tamanio, archivo);
				break;
			} else {
				fread(contenidoAEnviar, 1048576, 1, archivo);
//				asignarEnviarANodo(contenidoAEnviar, 1048576, archivo);
				tamanio -= 1048576;
			}
		}
		free(contenidoAEnviar);

	} else {
		char digito;
		int ultimoBarraN = 0;
		int registroAntesMega = 0;
		void * contenidoAEnviar;
		int ultimaPosicion = 0;
		t_list * posiciones = list_create();

		while (!feof(archivo)) {
			digito = fgetc(archivo);

			if (digito == '\n') {
				ultimoBarraN = ftell(archivo);
			}

			if (ftell(archivo) == 1048576 + registroAntesMega) {
				registroAntesMega = ultimoBarraN;
				list_add(posiciones, &ultimoBarraN);
			}
		}

		fseek(archivo, 0, SEEK_END);
		ultimaPosicion = ftell(archivo);
		list_add(posiciones, &ultimaPosicion);
		fseek(archivo, 0, SEEK_SET);
		int posicionActual = 0;

		void partir(int * posicion) {
			if (posicionActual == 0) {
				contenidoAEnviar = malloc(*posicion);
				fread(contenidoAEnviar, *posicion, 1, archivo);
//				asignarEnviarANodo(contenidoAEnviar, *posicion, archivo);
			} else {
				int posicionAnterior = *((int*) list_get(posiciones,
						posicionActual - 1));
				contenidoAEnviar = malloc((*posicion) - posicionAnterior);
				fread(contenidoAEnviar, (*posicion) - posicionAnterior, 1,
						archivo);
//				asignarEnviarANodo(contenidoAEnviar,(*posicion) - posicionAnterior, archivo);
			}
			posicionActual++;
		}
		list_iterate(posiciones, partir);
		free(contenidoAEnviar);

	}
}

//--------------------------------YAMA----------------------------------------
//ENVIAR TABLA NODOS

int sacarTamanioMEnsaje() {
	int tamanio = 0;
	int i = 0;
	for (; i < list_size(tablaGlobalNodos->nodo); i++) {
		tamanio += sizeof(int);
		int tamanioNodo = string_length(list_get(tablaGlobalNodos->nodo, i));
		tamanio += tamanioNodo;
	}
	return tamanio;
}

void enviarListaNodos(int socket) {
	void* mensaje = malloc(sizeof(tablaGlobalNodos->nodo));
	int posicionActual = 0;
	int i = 0;
	for (; i < list_size(tablaGlobalNodos->nodo); i++) {
		int tamanioNodo = string_length(list_get(tablaGlobalNodos->nodo, i));
		memcpy(mensaje, &tamanioNodo, sizeof(int));
		posicionActual += sizeof(int);
		memcpy(mensaje + posicionActual, list_get(tablaGlobalNodos->nodo, i),
				tamanioNodo);
		posicionActual += tamanioNodo;
	}
	int tamanioMsj = sacarTamanioMEnsaje();
	sendRemasterizado(socket, ES_FS, tamanioMsj, mensaje);
}

//ENVIAR DATOS

int tamanioStruct(copiasXBloque* contenido){
	return sizeof(uint32_t)*6 + string_length(contenido->bloque) + string_length(contenido->copia1->nodo) + string_length(contenido->copia2->nodo);
}

void* serializarCopiaBloque(copiasXBloque* contenido){
	int posicionActual = 0;

	void* datosSerializados = malloc(tamanioStruct(contenido));

	// serializo bloque
	uint32_t tamanioBloque = string_length(contenido->bloque);
	memcpy(datosSerializados + posicionActual, &tamanioBloque, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(datosSerializados + posicionActual, contenido->bloque, tamanioBloque);
	posicionActual += tamanioBloque;
	//serializo copia1
	uint32_t tamanioCopia1 = string_length(contenido->copia1->nodo);
	memcpy(datosSerializados + posicionActual, &tamanioCopia1, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(datosSerializados + posicionActual, contenido->copia1->nodo, tamanioCopia1);
	posicionActual += tamanioCopia1;
	memcpy(datosSerializados + posicionActual, contenido->copia1->bloque, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	// serializo copia2
	uint32_t tamanioCopia2 = string_length(contenido->copia2->nodo);
	memcpy(datosSerializados + posicionActual, &tamanioCopia2, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(datosSerializados + posicionActual, contenido->copia2->nodo, tamanioCopia2);
	posicionActual += tamanioCopia2;
	memcpy(datosSerializados + posicionActual, contenido->copia2->bloque, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	//serializo bytes
	memcpy(datosSerializados + posicionActual, contenido->bytes, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);

	return datosSerializados;

}

void enviarTablaAYama(tablaArchivos* entradaArchivo){
	uint32_t cont=0;
	uint32_t posicionActual=0;
	copiasXBloque* copiaBloque=malloc(sizeof(copiasXBloque));

	void* mensaje= malloc((sizeof(copiasXBloque)*list_size(entradaArchivo->bloques)+sizeof(uint32_t)));

	memcpy(mensaje+posicionActual,list_size(entradaArchivo->bloques),sizeof(uint32_t)); //REVISAR
	posicionActual += sizeof(uint32_t);

	while(cont<list_size(entradaArchivo->bloques)){
		copiaBloque=(copiasXBloque*)list_get(entradaArchivo->bloques,cont);
		memcpy(mensaje,serializarCopiaBloque(copiaBloque),tamanioStruct(copiaBloque));
		posicionActual+=tamanioStruct(copiaBloque);
	}

	uint32_t tamanioMsj=sizeof(uint32_t)+(sizeof(copiasXBloque)*list_size(entradaArchivo->bloques)); //REVISAR

//	sendRemasterizado(socket, INFO_ARCHIVO_FS, tamanioMsj, mensaje);
}


void enviarDatoArchivo(int socket){
  char* archivoABuscar=recibirString(socket);
	bool esArchivo(tablaArchivos* archivo){
		return(string_equals_ignore_case(archivo->nombreArchivo,archivoABuscar));
	}
	tablaArchivos* entradaArchivo = list_find(tablaGlobalArchivos,(void*)esArchivo);

	enviarTablaAYama(entradaArchivo);
}

//--------------------------------Main----------------------------------------
int main(int argc, char **argv) {
	loggerFileSystem = log_create("FileSystem.log", "FileSystem", 1, 0);
	chequearParametros(argc, 2);
	t_config* configuracionFS = generarTConfig(argv[1], 1);
//	t_config* configuracionFS = generarTConfig("Debug/filesystem.ini", 1);
	cargarFileSystem(configuracionFS);
	int socketMaximo, socketClienteChequeado, socketAceptado;
	int socketEscuchaFS = ponerseAEscucharClientes(PUERTO_ESCUCHA, 0);
	pthread_t hiloConsolaFS;
	socketMaximo = socketEscuchaFS;
	fd_set socketClientes, socketClientesAuxiliares;
	FD_ZERO(&socketClientes);
	FD_ZERO(&socketClientesAuxiliares);
	FD_SET(socketEscuchaFS, &socketClientes);
	pthread_create(&hiloConsolaFS, NULL, (void*) consolaFS, NULL);

	tablaGlobalNodos = malloc(sizeof(tablaNodos));
	tablaGlobalNodos->nodo = list_create();
	tablaGlobalNodos->contenidoXNodo = list_create();

	listaBitmap = list_create();
	tablaGlobalArchivos = list_create();
	listaConexionNodos=list_create();

	while (1) {
		socketClientesAuxiliares = socketClientes;
		if (select(socketMaximo + 1, &socketClientesAuxiliares, NULL, NULL,
		NULL) == -1) {
			log_error(loggerFileSystem, "No se pudo llevar a cabo el select.");
			exit(-1);
		}
		log_info(loggerFileSystem,
				"Se recibio nueva actividad de los clientes");
		for (socketClienteChequeado = 0; socketClienteChequeado <= socketMaximo;
				socketClienteChequeado++) {
			if (FD_ISSET(socketClienteChequeado, &socketClientesAuxiliares)) {
				if (socketClienteChequeado == socketEscuchaFS) {
					socketAceptado = aceptarConexionDeCliente(socketEscuchaFS);
					FD_SET(socketAceptado, &socketClientes);
					socketMaximo = calcularSocketMaximo(socketAceptado,
							socketMaximo);
					log_info(loggerFileSystem,
							"Se ha recibido una nueva conexion.");
				} else {
					int notificacion = recvDeNotificacion(
							socketClienteChequeado);
					switch (notificacion) {
					case ES_DATANODE:
						sendDeNotificacion(socketClienteChequeado, ES_FS);
						break;
					case ES_MASTER:
						//Hago mas cosas
						break;
					case ES_YAMA:
						if (hayNodes && esEstadoSeguro) {
							enviarListaNodos(socketClienteChequeado);
						} else {
							FD_CLR(socketClienteChequeado, &socketClientes);
							close(socketClienteChequeado);
						}
						break;
					case REC_INFONODO:
						registrarNodo(socketClienteChequeado);
						break;
//					case INFO_ARCHIVO_FS:
//						enviarDatoArchivo(socketClienteChequeado);
//						break;
					default:
						log_error(loggerFileSystem,
								"La conexion recibida es erronea.");
						FD_CLR(socketClienteChequeado, &socketClientes);
						close(socketClienteChequeado);
					}

				}
			}
		}
	}
	return EXIT_SUCCESS;
}
