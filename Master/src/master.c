#include "structMaster.h"

#define PARAMETROS {"YAMA_IP","YAMA_PUERTO"}

char* YAMA_IP;
char*WORKER_IP;
int YAMA_PUERTO;
int WORKER_PUERTO;

t_log* loggerMaster;

void cargarMaster(t_config* configuracionMaster){
    if(!config_has_property(configuracionMaster, "YAMA_IP")){
        log_error(loggerMaster, "No se encuentra YAMA_IP");
        exit(-1);
    }else{
        YAMA_IP = string_new();
        string_append(&YAMA_IP, config_get_string_value(configuracionMaster, "YAMA_IP"));
    }
    if(!config_has_property(configuracionMaster, "YAMA_PUERTO")){
        log_error(loggerMaster, "No se encuentra YAMA_PUERTO en el archivo de configuracion");
        exit(-1);
    }else{
        YAMA_PUERTO = config_get_int_value(configuracionMaster, "YAMA_PUERTO");
    }
}

void realizarHandshake(int unSocket, int proceso){
    sendDeNotificacion(unSocket, ES_MASTER);
    int notificacion = recvDeNotificacion(unSocket);
    if(notificacion != proceso){
        log_error(loggerMaster, "La conexion establecida no es correcta");
        exit(-1);
    }
}


long int obtenerTamanioArchivo(FILE* unArchivo){
	fseek(unArchivo, 0, SEEK_END);
	long int tamanioArchivo = ftell(unArchivo);
	return tamanioArchivo;
}

char* leerArchivo(FILE* unArchivo, long int tamanioArchivo)
{
	fseek(unArchivo, 0, SEEK_SET);
	char* contenidoArchivo = malloc(tamanioArchivo+1);
	fread(contenidoArchivo, tamanioArchivo, 1, unArchivo);
	contenidoArchivo[tamanioArchivo] = '\0';
	return contenidoArchivo;
}

char* obtenerContenido(char* unPath){
	FILE* archivoALeer = fopen(unPath, "rb");
	long int tamanioArchivo = obtenerTamanioArchivo(archivoALeer);
	char* contenidoArchivo = leerArchivo(archivoALeer,tamanioArchivo);
	return contenidoArchivo;
}

void propagarArchivo(char* unNombreArchivo, int socketDeYama){
	char* contenidoArchivo = obtenerContenido(unNombreArchivo);
	sendRemasterizado(socketDeYama,SK_FILE_SEND,string_length(contenidoArchivo),contenidoArchivo);
	free(contenidoArchivo);
}

int main(int argc, char **argv) {
	loggerMaster = log_create("Master.log", "Master", 1, 0);
	chequearParametros(argc,5);
	t_config* configuracionMaster = generarTConfig("master.ini", 2);
	cargarMaster(configuracionMaster);
    int socketYAMA = conectarAServer(YAMA_IP, YAMA_PUERTO);
    realizarHandshake(socketYAMA,ES_YAMA);
    WORKER_IP = "127.0.0.1";
    WORKER_PUERTO = 5010;
    propagarArchivo(argv[3],socketYAMA);
    int socketWorker = conectarAServer(WORKER_IP, WORKER_PUERTO);
    realizarHandshake(socketWorker,ES_WORKER);
	return EXIT_SUCCESS;
}
