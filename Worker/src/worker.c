#include "../../Biblioteca/src/genericas.c"
#include "../../Biblioteca/src/Socket.c"
#include "../../Biblioteca/src/configParser.c"

#define PARAMETROS {"IP_FILESYSTEM","PUERTO_FILESYSTEM","NOMBRE_NODO","PUERTO_WORKER","RUTA_DATABIN"}
#define TRANSFORMACION 332
#define REDUCCION_LOCAL 333
#define REDUCCION_GLOBAL 334
#define SIZE_SCRIPT 1024

t_log* loggerWorker;
char* IP_FILESYSTEM;
char* RUTA_DATABIN;
char* NOMBRE_NODO;
int PUERTO_FILESYSTEM;
int PUERTO_WORKER;

void cargarWorker(t_config* configuracionWorker){
    if(!config_has_property(configuracionWorker, "IP_FILESYSTEM")){
        perror("No se encuentra cargado IP_FILESYSTEM en el archivo.\n");
    }else{
        IP_FILESYSTEM = config_get_string_value(configuracionWorker, "IP_FILESYSTEM");
    }
    if(!config_has_property(configuracionWorker, "PUERTO_FILESYSTEM")){
        perror("No se encuentra cargado PUERTO_FILESYSTEM en el archivo.\n");
    }else{
        PUERTO_FILESYSTEM = config_get_int_value(configuracionWorker, "PUERTO_FILESYSTEM");
    }
    if(!config_has_property(configuracionWorker, "NOMBRE_NODO")){
        perror("No se encuentra cargado NOMBRE_NODO en el archivo.\n");
    }else{
        NOMBRE_NODO = string_new();
        string_append(&NOMBRE_NODO, config_get_string_value(configuracionWorker, "NOMBRE_NODO"));
    }
    if(!config_has_property(configuracionWorker, "PUERTO_WORKER")){
        perror("No se encuentra cargado PUERTO_WORKER en el archivo.\n");
    }else{
        PUERTO_WORKER = config_get_int_value(configuracionWorker, "PUERTO_WORKER");
    }
    if(!config_has_property(configuracionWorker, "RUTA_DATABIN")){
        perror("No se encuentra cargado RUTA_DATABIN en el archivo.\n");
    }else{
        RUTA_DATABIN = string_new();
        string_append(&RUTA_DATABIN, config_get_string_value(configuracionWorker, "RUTA_DATABIN"));
    }
    config_destroy(configuracionWorker);
}

void darPermisosAScripts(char* script){
	struct stat infoScript;

	if(chmod(script,S_IXUSR|S_IRUSR|S_IXGRP|S_IRGRP|S_IXOTH|S_IROTH|S_ISVTX)!=0){
		log_error(loggerWorker,"Error al otorgar permisos al script.");
	}
	else if(stat(script,&infoScript)!=0){
		log_error(loggerWorker,"No se pudo obtener informacion del script.");
	}
	else{
		log_info(loggerWorker,"Los permisos para el script son: %08x\n",infoScript.st_mode);
	}
}

char* crearComandoScript(char* pathScript,char* pathDestino, int nroBloque, long bytesOcupados){
	char* command = string_new();
	int bloqueAnterior = nroBloque-1;
	string_append(&command, "head -n ");
	string_append(&command,string_itoa((bloqueAnterior*1048576)+bytesOcupados));
	string_append(&command," ");
	string_append(&command,RUTA_DATABIN);
	string_append(&command," | tail -n ");
	string_append(&command,string_itoa(bytesOcupados));
	string_append(&command," | ./");
	string_append(&command,pathScript);
	string_append(&command," | ./sort > ");
	string_append(&command,pathDestino);
	free(pathScript);
	free(pathDestino);
	return command;
}

void crearProcesoHijo(int socketMaster){
	paquete* unPaquete = recvRemasterizado(socketMaster);
	int tamanioScript;
	int tamanioPathDestino;
	int nroBloque;
	int pipe_padreAHijo[2];
	int pipe_hijoAPadre[2];
	int status;
	long bytesOcupados;
	char* pathScript;
	char* pathDestino;

	pipe(pipe_padreAHijo);
	pipe(pipe_hijoAPadre);

	pid_t pid = fork();

	switch(pid){
	case -1:
		perror("No se pudo crear el proceso hijo\n");
		exit(-1);
	case 0:

		printf("Proceso hijo con pid: %d \n",pid);
		printf("Soy el hijo y mi padre tiene el pid: %d \n",getppid());

		dup2(pipe_padreAHijo[0],STDIN_FILENO);
		dup2(pipe_hijoAPadre[1],STDOUT_FILENO);

		close(pipe_padreAHijo[1]);
		close(pipe_hijoAPadre[0]);
		close(pipe_hijoAPadre[1]);
		close(pipe_padreAHijo[0]);

		switch(unPaquete->tipoMsj){
		case TRANSFORMACION:{
			memcpy(&tamanioScript,unPaquete->mensaje,sizeof(int));
			pathScript = malloc(tamanioScript);
			memcpy(pathScript,unPaquete->mensaje+sizeof(int),tamanioScript);
			memcpy(&nroBloque,unPaquete->mensaje+sizeof(int)+tamanioScript,sizeof(int));
			memcpy(&bytesOcupados,unPaquete->mensaje+sizeof(int)*2+tamanioScript,sizeof(long));
			memcpy(&tamanioPathDestino,unPaquete->mensaje+sizeof(int)*2+tamanioScript+sizeof(long),sizeof(int));
			pathDestino = malloc(tamanioPathDestino);
			memcpy(pathDestino,unPaquete->mensaje+sizeof(int)*3+tamanioScript+sizeof(double),tamanioPathDestino);

			free(unPaquete);

			darPermisosAScripts(pathScript);

			char* command = crearComandoScript(pathScript,pathDestino,nroBloque,bytesOcupados);

			system(command);

			free(command);
		}
		break;
		case REDUCCION_LOCAL:{

		}
		break;
		case REDUCCION_GLOBAL:{

		}
		break;
		default:
			log_error(loggerWorker, "Error al recibir paquete de Master");
			exit(-1);
		}

		exit(1);
	default:
		close(socketMaster);

		printf("Proceso padre con pid: %d \n",pid);

		close(pipe_padreAHijo[0]);
		close(pipe_hijoAPadre[1]);

		close(pipe_padreAHijo[1]);

		waitpid(pid,&status,0);

		close(pipe_hijoAPadre[0]);

	}
}

int main(int argc, char **argv) {
	loggerWorker = log_create("Worker.log", "Worker", 1, 0);
	chequearParametros(argc,2);
	t_config* configuracionWorker = generarTConfig(argv[1], 6);
	//t_config* configuracionWorker = generarTConfig("Debug/worker.ini", 5);
	cargarWorker(configuracionWorker);
	log_info(loggerWorker, "Se cargo correctamente Worker.");
	int socketMaximo, socketClienteChequeado, socketAceptado;
	int socketEscuchaWorker = ponerseAEscucharClientes(PUERTO_WORKER, 0);
	socketMaximo = socketEscuchaWorker;
	fd_set socketsMasters, socketMastersAuxiliares;
	FD_ZERO(&socketsMasters);
	FD_ZERO(&socketMastersAuxiliares);
	FD_SET(socketEscuchaWorker, &socketsMasters);
	while(1){
		socketMastersAuxiliares = socketsMasters;
		if(select(socketMaximo+1, &socketMastersAuxiliares, NULL, NULL, NULL)==-1){
			log_error(loggerWorker, "No se pudo llevar a cabo el select.");
			exit(-1);
		}
		log_info(loggerWorker, "Se recibio nueva actividad de los clientes");
		for(socketClienteChequeado = 0; socketClienteChequeado <= socketMaximo; socketClienteChequeado++){
			if(FD_ISSET(socketClienteChequeado, &socketMastersAuxiliares)){
				if(socketClienteChequeado == socketEscuchaWorker){
					socketAceptado = aceptarConexionDeCliente(socketEscuchaWorker);
					FD_SET(socketAceptado, &socketsMasters);
					socketMaximo = calcularSocketMaximo(socketAceptado, socketMaximo);
					log_info(loggerWorker, "Se ha recibido una nueva conexion.");
				}else{
					int notificacion = recvDeNotificacion(socketClienteChequeado);
					if(notificacion!=ES_MASTER){
						log_info(loggerWorker, "La conexion recibida es erronea.");
						FD_CLR(socketClienteChequeado, &socketsMasters);
						close(socketClienteChequeado);
					}else{
						sendDeNotificacion(socketClienteChequeado, ES_WORKER);
						crearProcesoHijo(socketClienteChequeado);
					}
				}

			}
		}
	}
	return EXIT_SUCCESS;
}
