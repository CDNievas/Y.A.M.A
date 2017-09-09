#include "../../Biblioteca/src/genericas.c"
#include "../../Biblioteca/src/Socket.c"
#include "../../Biblioteca/src/configParser.c"

#define PARAMETROS {"FS_IP","FS_PUERTO","RETARDO_PLANIFICACION","ALGORITMO_BALANCEO", "PUERTO_MASTERS"}
char* FS_IP;
int FS_PUERTO;
int RETARDO_PLANIFICACION;
char* ALGORITMO_BALANCEO;
int PUERTO_MASTERS;

t_log* loggerYAMA;

void cargarYAMA(t_config* configuracionYAMA){
    if(!config_has_property(configuracionYAMA, "FS_IP")){
        log_error(loggerYAMA, "No se encuentra el parametro FS_IP en el archivo de configuracion");
        exit(-1);
    }else{
        FS_IP = string_new();
        string_append(&FS_IP, config_get_string_value(configuracionYAMA, "FS_IP"));
    }
    if(!config_has_property(configuracionYAMA, "FS_PUERTO")){
        log_error(loggerYAMA, "No se encuentra el parametro FS_PUERTO en el archivo de configuracion");
        exit(-1);
    }else{
        FS_PUERTO = config_get_int_value(configuracionYAMA, "FS_PUERTO");
    }
    if(!config_has_property(configuracionYAMA, "RETARDO_PLANIFICACION")){
        log_error(loggerYAMA, "No se encuentra el parametro RETARDO_PLANIFICACION en el archivo de configuracion");
        exit(-1);
    }else{
        RETARDO_PLANIFICACION = config_get_int_value(configuracionYAMA, "RETARDO_PLANIFICACION");
    }
    if(!config_has_property(configuracionYAMA, "ALGORITMO_BALANCEO")){
    	log_error(loggerYAMA, "No se encuentra el parametro RETARDO_PLANIFICACION en el archivo de configuracion");
    	exit(-1);
    }else{
        ALGORITMO_BALANCEO = string_new();
        string_append(&ALGORITMO_BALANCEO, config_get_string_value(configuracionYAMA, "ALGORITMO_BALANCEO"));
    }
    if(!config_has_property(configuracionYAMA, "PUERTO_MASTERS")){
    	log_error(loggerYAMA, "No se encuentra el parametro PUERTO_MASTERS en el archivo de configuracion");
    	exit(-1);
    }else{
    	PUERTO_MASTERS = config_get_int_value(configuracionYAMA, "PUERTO_MASTERS");
    }
    config_destroy(configuracionYAMA);
}

void realizarHandshakeConFS(int socketFS){
	sendDeNotificacion(socketFS, ES_YAMA);
	int notificacion = recvDeNotificacion(socketFS);
	if(notificacion != ES_FS){
		log_error(loggerYAMA, "La conexion establecida no es con FileSystem");
		exit(-1);
	}
}

/* Funcion main pide
	- Path archivo de configuracion.ini
*/

int main(int argc, char **argv) {

	loggerYAMA = log_create("YAMA.log", "YAMA", 1, 0);
	chequearParametros(argc);
	t_config* configuracionYAMA = generarTConfig(argv[1], 5);
//	t_config* configuracionYAMA = generarTConfig("Debug/yama.ini", 5);
	cargarYAMA(configuracionYAMA);
	log_info(loggerYAMA, "Se cargo exitosamente YAMA.");
	int socketFS = conectarAServer(FS_IP, FS_PUERTO);
	realizarHandshakeConFS(socketFS);
	int socketEscuchaMasters = ponerseAEscucharClientes(PUERTO_MASTERS, 0);
	int socketMaximo = socketEscuchaMasters, socketClienteChequeado, socketAceptado;
	fd_set socketsMasterCPeticion, socketMastersAuxiliares;
	FD_ZERO(&socketMastersAuxiliares);
	FD_ZERO(&socketsMasterCPeticion);
	FD_SET(socketEscuchaMasters, &socketsMasterCPeticion);
	while(1){
		socketMastersAuxiliares = socketsMasterCPeticion;
		if(select(socketMaximo+1, &socketMastersAuxiliares, NULL, NULL, NULL)==-1){
			log_error(loggerYAMA, "No se pudo llevar a cabo el select.");
			exit(-1);
		}
		log_info(loggerYAMA, "Un socket realizo una peticion a YAMA.");
		for(socketClienteChequeado = 0; socketClienteChequeado <= socketMaximo; socketClienteChequeado++){
			if(FD_ISSET(socketClienteChequeado, &socketMastersAuxiliares)){
				if(socketClienteChequeado == socketEscuchaMasters){
					socketAceptado = aceptarConexionDeCliente(socketEscuchaMasters);
					FD_SET(socketAceptado, &socketsMasterCPeticion);
					socketMaximo = calcularSocketMaximo(socketAceptado, socketMaximo);
					log_info(loggerYAMA, "Se recibio una nueva conexion del socket %d.", socketAceptado);
				}else{
					int notificacion = recvDeNotificacion(socketClienteChequeado);
					if(notificacion != ES_MASTER){
						log_info(loggerYAMA, "La conexion del socket %d es erronea.", socketClienteChequeado);
						FD_CLR(socketClienteChequeado, &socketsMasterCPeticion);
						close(socketClienteChequeado);
					}else{
						sendDeNotificacion(socketClienteChequeado, ES_YAMA);
					}
				}
			}
		}
	}
	return EXIT_SUCCESS;
}
