#include "structFS.h"
#include <pthread.h>


#define PARAMETROS {"PUERTO_ESCUCHA"}

int PUERTO_ESCUCHA;

t_log* loggerFileSystem;
int hayNodes = 0;
int esEstadoSeguro = 1;

//CONSOLA DE FS

void *consolaFS(){
	char* comando;
	while(1){
		comando = readline("->");
		char* comandoDosCaracteres = string_substring_until(comando, 2);
		char* comandoTresCaracteres = string_substring_until(comando, 3);
		char* comandoCuatroCaracteres = string_substring_until(comando, 4);
		char* comandoCincoCaracteres = string_substring_until(comando, 5);
		char* comandoSeisCaracteres = string_substring_until(comando, 6);
		char* comandoSieteCaracteres = string_substring_until(comando, 7);
		if(comando == NULL){
			break;
		}else if(string_equals_ignore_case(comando, "exit")){
			free(comando);
			exit(0);
		}else if(string_equals_ignore_case(comando, "format")){
			add_history(comando);
		}else if(string_equals_ignore_case(comandoDosCaracteres, "rm")){
			add_history(comandoDosCaracteres);
		}else if(string_equals_ignore_case(comandoDosCaracteres, "mv")){
			add_history(comandoDosCaracteres);
		}else if(string_equals_ignore_case(comandoDosCaracteres, "ls")){
			add_history(comandoDosCaracteres);
		}else if(string_equals_ignore_case(comandoTresCaracteres, "cat")){
			add_history(comandoTresCaracteres);
		}else if(string_equals_ignore_case(comandoTresCaracteres, "md5")){
			add_history(comandoTresCaracteres);
		}else if(string_equals_ignore_case(comandoCuatroCaracteres, "cpto")){
			add_history(comandoCuatroCaracteres);
		}else if(string_equals_ignore_case(comandoCuatroCaracteres, "info")){
			add_history(comandoCuatroCaracteres);
		}else if(string_equals_ignore_case(comandoCincoCaracteres, "mkdir")){
			add_history(comandoCincoCaracteres);
		}else if(string_equals_ignore_case(comandoSeisCaracteres, "rename")){
			add_history(comandoSeisCaracteres);
		}else if(string_equals_ignore_case(comandoSeisCaracteres, "cpfrom")){
			add_history(comandoSeisCaracteres);
		}else if(string_equals_ignore_case(comandoSieteCaracteres, "cpblock")){
			add_history(comandoSieteCaracteres);
		}else{
			//LOGGER DE ERROR PARA QUE PUEDA VOLVER A REINGRESAR EL DATO
		}
	}
}

 //Habria que implementar la copia de los archivos

void cargarFileSystem(t_config* configuracionFS){
    if(!config_has_property(configuracionFS, "PUERTO_ESCUCHA")){
    	log_error(loggerFileSystem, "No se encuentra el parametro PUERTO_ESCUCHA en el archivo de configuracion");
    	exit(-1);
    }else{
        PUERTO_ESCUCHA = config_get_int_value(configuracionFS, "PUERTO_ESCUCHA");
    }
    config_destroy(configuracionFS);
}

int main(int argc, char **argv) {
	loggerFileSystem = log_create("FileSystem.log", "FileSystem", 1, 0);
	chequearParametros(argc,2);
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
	pthread_create(&hiloConsolaFS, NULL, consolaFS, NULL);
	while(1){
		socketClientesAuxiliares = socketClientes;
		if(select(socketMaximo+1, &socketClientesAuxiliares, NULL, NULL, NULL)==-1){
			log_error(loggerFileSystem, "No se pudo llevar a cabo el select.");
			exit(-1);
		}
		log_info(loggerFileSystem, "Se recibio nueva actividad de los clientes");
		for(socketClienteChequeado = 0; socketClienteChequeado <= socketMaximo; socketClienteChequeado++){
			if(FD_ISSET(socketClienteChequeado, &socketClientesAuxiliares)){
				if(socketClienteChequeado == socketEscuchaFS){
					socketAceptado = aceptarConexionDeCliente(socketEscuchaFS);
					FD_SET(socketAceptado, &socketClientes);
					socketMaximo = calcularSocketMaximo(socketAceptado, socketMaximo);
					log_info(loggerFileSystem, "Se ha recibido una nueva conexion.");
				}else{
					int notificacion = recvDeNotificacion(socketClienteChequeado);
					switch(notificacion){
						case ES_DATANODE:
							hayNodes = 1;
							sendDeNotificacion(socketClienteChequeado, ES_FS);
						break;
						case ES_MASTER:
							//Hago mas cosas
						break;
						case ES_YAMA:
							if(hayNodes && esEstadoSeguro){
								sendDeNotificacion(socketClienteChequeado, ES_FS);
							}else{
								FD_CLR(socketClienteChequeado, &socketClientes);
								close(socketClienteChequeado);
							}
						break;
						default:
							log_error(loggerFileSystem, "La conexion recibida es erronea.");
							FD_CLR(socketClienteChequeado, &socketClientes);
							close(socketClienteChequeado);
						}

					}
				}
			}
		}
	return EXIT_SUCCESS;
}
