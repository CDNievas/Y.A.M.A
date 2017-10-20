#include "structMaster.h"

#include "transformaciones.h"
#include "reduccionLocal.h"
#include "reduccionGlobal.h"

#include "../../Biblioteca/src/Socket.c"
#include "../../Biblioteca/src/configParser.c"


#define PARAMETROS {"YAMA_IP","YAMA_PUERTO"}


int main(int argc, char **argv) {
	loggerMaster = log_create("Master.log", "Master", 1, 0);
	chequearParametros(argc,5);
	t_config* configuracionMaster = generarTConfig("master.ini", 2);
	cargarMaster(configuracionMaster);
    socketYAMA = conectarAServer(YAMA_IP, YAMA_PUERTO);
    realizarHandshake(socketYAMA,ES_YAMA);

	return EXIT_SUCCESS;
}
