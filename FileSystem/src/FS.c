/*
 * FS.c
 *
 *  Created on: 25/11/2017
 *      Author: utnso
 */

#include "BitarrayConfiguraciones.h"
#include "Estructuras.h"
#include "Consola.h"
#include "FuncionesFS.h"
#include "Nodos.h"
#include "Persistencia.h"
#include "YAMA.h"
#include "Worker.h"
#include "EstadoAnterior.h"
#include "../../Biblioteca/src/Socket.c"
#include "../../Biblioteca/src/configParser.c"

int main(int argc, char **argv) {
	signal(SIGINT, killMe);
	loggerFileSystem = log_create("FileSystem.log", "FileSystem", 1, 0);
	if(argc<2){
		log_error(loggerFileSystem,"Faltan parametro para iniciar FS");
		exit (-1);
	}
//	t_config* configuracionFS = generarTConfig("Debug/off_filesystem.ini", 5);
	t_config* configuracionFS = generarTConfig(argv[1], 5);
	cargarFileSystem(configuracionFS);
	chequearParametrosFs(argc,argv[2]);
	int socketMaximo, socketAceptado;
	socketEscuchaFS = ponerseAEscucharClientes(PUERTO_ESCUCHA, 0);
	socketMaximo = socketEscuchaFS;
	fd_set socketClientes, socketClientesAuxiliares;
	FD_ZERO(&socketClientes);
	FD_ZERO(&socketClientesAuxiliares);
	FD_SET(socketEscuchaFS, &socketClientes);
	pthread_create(&hiloConsolaFS, NULL, (void*) consolaFS, NULL);

	tablaGlobalNodos = malloc(sizeof(tablaNodos));
	tablaGlobalNodos->libres=0;
	tablaGlobalNodos->tamanio=0;
	tablaGlobalNodos->nodo = list_create();
	tablaGlobalNodos->contenidoXNodo = list_create();

	listaBitmap = list_create();
	tablaGlobalArchivos = list_create();
	listaConexionNodos=list_create();


	listaDirectorios = list_create();
	registroArchivos=list_create();

	seDesconectoUnNodo=false;
	if(hayUnEstadoAnterior()==false){
		inicializarDirectoriosPrincipales();
		esEstadoSeguro=false;
		estaFormateado=false;
	}


	while (1) {

		socketClientesAuxiliares = socketClientes;

		if (select(socketMaximo + 1, &socketClientesAuxiliares, NULL, NULL,NULL) == -1) {
			log_error(loggerFileSystem, "No se pudo llevar a cabo el select.");
			exit(-1);
		}

		for (socketClienteChequeado = 0; socketClienteChequeado <= socketMaximo;socketClienteChequeado++) {
			if (FD_ISSET(socketClienteChequeado, &socketClientesAuxiliares)) {
				if (socketClienteChequeado == socketEscuchaFS) {

					socketAceptado = aceptarConexionDeCliente(socketEscuchaFS);
					FD_SET(socketAceptado, &socketClientes);
					socketMaximo = calcularSocketMaximo(socketAceptado,socketMaximo);
					log_info(loggerFileSystem,"Se ha recibido una nueva conexion.");

				} else {

					int notificacion = recvDeNotificacion(socketClienteChequeado);

					switch (notificacion) {
						case ES_WORKER:
							sendDeNotificacion(socketClienteChequeado, ES_FS);
							break;
						case ES_DATANODE:
							sendDeNotificacion(socketClienteChequeado, ES_FS);
							break;
						case ES_YAMA:
							if (esEstadoSeguro) {
								enviarListaNodos(socketClienteChequeado);
							} else {
								log_error(loggerFileSystem, "No se encuentra en un estado seguro. Cerrando conexion con YAMA");
								FD_CLR(socketClienteChequeado, &socketClientes);
								close(socketClienteChequeado);
							}
							break;
						case REC_INFONODO:
							registrarNodo(socketClienteChequeado);
							log_info(loggerFileSystem,"Se ha registrado un nodo nuevo al sistema");
							break;
						case REC_BLOQUE:
							recibirBloque(socketClienteChequeado);
							break;
						case INFO_ARCHIVO_FS:
							enviarDatoArchivo(socketClienteChequeado);
							break;
						case DATOS_NODO:
							enviarDatosConexionNodo(socketClienteChequeado);
							break;
						case ALMACENADO_FINAL:
							almacenarArchivoWorker(socketClienteChequeado);
							break;
						case CORTO:
							log_error(loggerFileSystem, "El socket %d corto la conexion.", socketClienteChequeado);
							verificarQueNodo(socketClienteChequeado);
							FD_CLR(socketClienteChequeado, &socketClientes);
							close(socketClienteChequeado);
							break;
						default:
							log_error(loggerFileSystem,
									"La conexion recibida es erronea.");
							FD_CLR(socketClienteChequeado, &socketClientes);
							close(socketClienteChequeado);
							break;
					}

				}
			}
		}
	}

	return EXIT_SUCCESS;

}
