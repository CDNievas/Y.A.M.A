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
	scriptTransformador = argv[1];
	scriptReduccion = argv[2];
	archivoAModificar = argv[3];
	pathDondeGuardar = argv[4];
	nombresNodos = list_create();
	log_info(loggerMaster,"Se cargo exitosamente Master");
    socketYAMA = conectarAServer(YAMA_IP, YAMA_PUERTO);
    log_info(loggerMaster,"Conectando con YAMA. IP: %s. Puerto: %d",YAMA_IP,YAMA_PUERTO);
    realizarHandshake(socketYAMA,ES_YAMA);
    log_info(loggerMaster,"Handshake realizado con éxito");

    log_info(loggerMaster,"Envio archivo sobre el que se desea operar a YAMA");
    int tamanioArchAModificar = tamanioArchivoAModificar();
    void* archivoAModificarSerializado = serializarArchivoAModificar();
    sendRemasterizado(socketYAMA, TRANSFORMACION, tamanioArchAModificar, archivoAModificarSerializado);
    log_info(loggerMaster,"Envio realizado con exito");

    log_info(loggerMaster,"Iniciando proceso de transformacion");
    procesarTransformacion();

	return EXIT_SUCCESS;
}
