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

	cargarBin(bitarray,bmap);
	//printf("%d \n",bitarray_get_max_bit(bitarray));

/*
	int i = 1;
	while(i <= 5){
		printf("%d \n",bitarray_test_bit(bitarray,i));
		i++;
	}
*/

	//int socketServerFS = conectarAServer(IP_FILESYSTEM, PUERTO_FILESYSTEM);
	//realizarHandshakeFS(socketServerFS);
	/*while(1){

	}*/


	munmap(bmap,infoDatabin.st_size);
	bitarray_destroy(bitarray);
	log_destroy(loggerDatanode);
	return EXIT_SUCCESS;

}
