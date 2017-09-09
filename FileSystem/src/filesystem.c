#include "structFS.h"

#define PARAMETROS {"PUERTO_ESCUCHA"}

int PUERTO_ESCUCHA;

t_log* loggerFileSystem;
int hayNodes = 0;
int esEstadoSeguro = 1; //Habria que implementar la copia de los archivos

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
	chequearParametros(argc);
	t_config* configuracionFS = generarTConfig(argv[1], 1);
//	t_config* configuracionFS = generarTConfig("Debug/filesystem.ini", 1);
	cargarFileSystem(configuracionFS);
	int socketMaximo, socketClienteChequeado, socketAceptado;
	int socketEscuchaFS = ponerseAEscucharClientes(PUERTO_ESCUCHA, 0);
	socketMaximo = socketEscuchaFS;
	fd_set socketClientes, socketClientesAuxiliares;
	FD_ZERO(&socketClientes);
	FD_ZERO(&socketClientesAuxiliares);
	FD_SET(socketEscuchaFS, &socketClientes);
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
