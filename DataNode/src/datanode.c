#include "funcionesDatanode.h"
#include "../../Biblioteca/src/genericas.c"
#include "../../Biblioteca/src/configParser.c"
#include "../../Biblioteca/src/Socket.c"

/* Funcion main pide
	- Path archivo de configuracion.ini
*/

int main(int argc, char **argv) {

	chequearParametros(argc,2);
	loggerDatanode = log_create("DataNode.log", "DataNode", 1, 0);

	// Debug Eclipse
	//t_config* configuracionDataNode = generarTConfig("src/datanode.ini", 6);

	// Run consola
	t_config* configuracionDataNode = generarTConfig(argv[1], 6);
	cargarDataNode(configuracionDataNode);

	log_info(loggerDatanode, "Se cargo correctamente DataNode cuyo nombre es %s.", nombreNodo);

	// Cargo binario
	cargarBin(mapArchivo);

	// Conexion con FS
	int socketServerFS = conectarAServer(ipFilesystem, puertoFilesystem);
	realizarHandshakeFS(socketServerFS);

	// Envio nombre de nodo y cantidad de bloques al FS
	enviarInfoNodo(socketServerFS);

	corte = 1;
	while(corte){

		u_int32_t operacion = recvDeNotificacion(socketServerFS);

		switch (operacion){
			case REC_LEER:{

				u_int32_t nroBloque = recibirUInt(socketServerFS);
				void * bloque;

				// Leo el bloque
				if((bloque = leerBloque(nroBloque)) == NULL){
					// Bloque inexistente
					exit(-99);
				} else {
					// Lo envio a FS
					sendRemasterizado(socketServerFS, ENV_BLOQUE, SIZEBLOQUE, bloque);
				}

				free(bloque);
				break;
			}
			case REC_ESCRIBIR:{

				u_int32_t nroBloque = recibirUInt(socketServerFS);
				void * bloque = recvDeBloque(socketServerFS);

				// Escribo bloque
				if(escribirBloque(nroBloque,bloque) < 0){
					// Bloque inexistente
					exit(-98);
				}

				free(bloque);
				break;
			}
			default:
				log_warning(loggerDatanode, "Peticion recibida por FS incorrecta.");
				break;
		}

	}

	// LIBERO MEMORIA
	munmap(mapArchivo,cantBloques);
	log_destroy(loggerDatanode);

	return EXIT_SUCCESS;

}
