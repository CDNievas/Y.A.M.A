#include "structMaster.h"

#define PARAMETROS {"YAMA_IP","YAMA_PUERTO"}

char* YAMA_IP;
int YAMA_PUERTO;

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

void realizarHandshakeMasterYama(int socketYAMA){
    sendDeNotificacion(socketYAMA, ES_MASTER);
    int notificacion = recvDeNotificacion(socketYAMA);
    if(notificacion != ES_YAMA){
        log_error(loggerMaster, "La conexion establecida no es de YAMA");
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

char* abrirArchivo(char* unPath){
	FILE* archivoALeer = fopen(unPath, "rb");
	long int tamanioArchivo = obtenerTamanioArchivo(archivoALeer);
	char* contenidoArchivo = leerArchivo(archivoALeer,tamanioArchivo);
	return contenidoArchivo;
}

/* Funcion main pide
	- Path archivo de configuracion.ini
*/

int main(int argc, char **argv) {
	loggerMaster = log_create("Master.log", "Master", 1, 0);
	chequearParametros(argc);
	t_config* configuracionMaster = generarTConfig(argv[1], 2);
//	t_config* configuracionMaster = generarTConfig("Debug/master.ini", 2);
	cargarMaster(configuracionMaster);
    int socketYAMA = conectarAServer(YAMA_IP, YAMA_PUERTO);
    realizarHandshakeMasterYama(socketYAMA);
	return EXIT_SUCCESS;
}
