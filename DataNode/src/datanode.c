#include "funcionesDatanode.h"
#include "../../Biblioteca/src/configParser.c"
#include "../../Biblioteca/src/Socket.c"

/* Funcion main pide
	- Path archivo de configuracion.ini
*/

int main(int argc, char **argv) {
	loggerDatanode = log_create("DataNode.log", "DataNode", 1, 0);
	chequearParametros(argc,2);
	t_config* configuracionDataNode = generarTConfig(argv[1], 6);
	//t_config* configuracionDataNode = generarTConfig("src/datanode.ini", 6);
	cargarDataNode(configuracionDataNode);
	log_info(loggerDatanode, "Se cargo correctamente DataNode cuyo nombre es %s.", NOMBRE_NODO);

	cargarBin(mapArchivo);
	log_info(loggerDatanode,"Bitarray creado correctamente");

	//int socketServerFS = conectarAServer(IP_FILESYSTEM, PUERTO_FILESYSTEM);
	//realizarHandshakeFS(socketServerFS);
	/*while(1){

	}*/

	// EJEMPLO DE USO

	// Creo la estructura bloque
	void * dataInBloque = malloc(SIZEBLOQUE);

	// Cargo el bloque con informacion
	char * randomChar = malloc(1024);
	gen_random(randomChar,1024);
	memcpy(dataInBloque,randomChar,1024);

	// Escribo bloque y chequeo por si hay error
	if(escribirBloque(0,dataInBloque) < 0){
		return -1;
	}

	void * dataOutBloque;
	dataOutBloque = leerBloque(0);

	if(dataOutBloque == NULL){
		return -2;
	}

	printf("%s",dataOutBloque);

	free(dataOutBloque);
	free(randomChar);
	free(dataInBloque);

	// LIBERO MEMORIA
	free(memBitarray);
	munmap(mapArchivo,cantBloques);
	bitarray_destroy(bitarray);
	log_destroy(loggerDatanode);

	return EXIT_SUCCESS;

}
