#include "funcionesDatanode.h"
#include "../../Biblioteca/src/genericas.c"
#include "../../Biblioteca/src/configParser.c"
#include "../../Biblioteca/src/Socket.c"

/* Funcion main pide
	- Path archivo de configuracion.ini
*/

int main(int argc, char **argv) {

	signal(SIGINT, handlerSIGINT);

	chequearParametros(argc,2);
	loggerDatanode = log_create("DataNode.log", "DataNode", 1, 0);

	// Debug Eclipse
//	t_config* configuracionDataNode = generarTConfig("src/datanode.ini", 6);

	// Run consola
	t_config* configuracionDataNode = generarTConfig(argv[1], 7);
	cargarDataNode(configuracionDataNode);

	log_info(loggerDatanode, "Se cargo correctamente DataNode cuyo nombre es %s.", NOMBRE_NODO);

	// Cargo binario
	cargarBin(mapArchivo);

	// Conexion con FS
	socketServerFS = conectarAServer(IP_FILESYSTEM, PUERTO_FILESYSTEM);
	realizarHandshakeFS(socketServerFS);

	// Envio nombre de nodo y cantidad de bloques al FS
	enviarInfoNodo(socketServerFS);

	corte = 1;
	while(corte){

		u_int32_t operacion = recvDeNotificacion(socketServerFS);

		switch (operacion){
			case REC_LEER:{

				u_int32_t nroBloque = recibirUInt(socketServerFS);
				u_int32_t cantBytes = recibirUInt(socketServerFS);

				if(cantBytes > SIZEBLOQUE){
					log_error(loggerDatanode, "Se estan tratando de leer %d bytes.",cantBytes);
					exit(-96);
				}

				//Leo el bloque
				void * bloque = leerBloque(nroBloque,cantBytes);


				if(bloque == NULL){
					// Bloque inexistente
					exit(-99);
				} else {
					// Lo envio a FS
					sendRemasterizado(socketServerFS, ENV_BLOQUE, cantBytes, bloque);
				}

				free(bloque);
				break;
			}

			case REC_ESCRIBIR:{

				u_int32_t nroBloque = recibirUInt(socketServerFS);
				u_int32_t cantBytes = recibirUInt(socketServerFS);

				if(cantBytes > SIZEBLOQUE){
					log_warning(loggerDatanode, "Se estan escribiendo %d bytes.",cantBytes);
				}

				char * bloque = recvDeBloque(socketServerFS);

				// Escribo bloque
				int resultado = escribirBloque(nroBloque,bloque,cantBytes);
				if(resultado<0){
					// Bloque inexistente
					sendDeNotificacion(socketServerFS, ESC_INCORRECTA);
					log_error(loggerDatanode, "Se trato de escribir en el bloque %d que es inexistente.",nroBloque);
					exit(-98);
				}

				sendDeNotificacion(socketServerFS, ESC_CORRECTA);
				free(bloque);
				break;

			}
				
			case DESCONECTAR_NODO:{
				corte=0;
				log_error(loggerDatanode, "FileSystem rechazo la conexion");
				close(socketServerFS);
				break;
			}
				
			case 0:{
				corte = 0;
				log_error(loggerDatanode, "Murio FileSystem. Terminando proceso");
				break;
			}

			default:
				corte = 0;
				log_warning(loggerDatanode, "Peticion recibida por FS incorrecta.");
				break;
		}

	}

	// LIBERO MEMORIA
	close(socketServerFS);
	munmap(mapArchivo,cantBloques);
	log_destroy(loggerDatanode);

	return EXIT_SUCCESS;

}
