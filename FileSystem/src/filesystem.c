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

void registrarNodo(int socket) {
	char* nombreNodo = string_new();
	int tamanioNombreNodo = 0, cantidadDeBloques = 0;
	t_bitarray* bitarray;

	nombreNodo = recibirString(socket);
	cantidadDeBloques = recibirUInt(socket);
	//bitarray=recibirBitarray(socket);
	if (hayEstadoAnterior) {
		//verificarNodo(nombreNodo);
	} else {
		//CARGO LA TABLA DE NODOS
		contenidoNodo* bloque = malloc(sizeof(contenidoNodo));
		bloque->nodo = string_new();
		string_append(&bloque->nodo, nombreNodo);
		bloque->total = cantidadDeBloques;
		//bloque->libre = sacarBloquesLibre(bitarray);

		tablaGlobalNodos->tamanio += cantidadDeBloques;
		tablaGlobalNodos->libres += bloque->libre;
		list_add(tablaGlobalNodos->nodo, nombreNodo);
		list_add(tablaGlobalNodos->contenidoXNodo, bloque);

		//CARGO LA TABLA DE BITMAPS
		tablaBitmapXNodos* bitmapNodo = malloc(sizeof(tablaBitmapXNodos));
		string_append(&bitmapNodo->nodo, nombreNodo);
		bitmapNodo->bitarray = bitarray;
		list_add(listaBitmap, bitmapNodo);

		//CARGO LA TABLA DE CONEXIONES DE NODO
		nodoConexion* nodoConectado = malloc(sizeof(nodoConexion));
		nodoConectado->nodo = string_new();
		string_append(nodoConectado->nodo, nombreNodo);
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
						//registrarNodo(paqueteRecibido->mensaje, int socket);
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
						//case INFO_ARCHIVOFS:
						//enviarDatoArchivo(paqueteRecibido.mensaje, socketClienteChequeado);
						//break;
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
