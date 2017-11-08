#include "../../Biblioteca/src/Socket.c"
#include "../../Biblioteca/src/configParser.c"

#define PARAMETROS {"IP_FILESYSTEM","PUERTO_FILESYSTEM","NOMBRE_NODO","PUERTO_WORKER","RUTA_DATABIN"}
#define TRANSFORMACION 1
#define TRANSFORMACION_TERMINADA 2
#define ERROR_TRANSFORMACION 3
#define REDUCCION_LOCAL 8
#define REDUCCION_LOCAL_TERMINADA 4
#define ERROR_REDUCCION_LOCAL 6
#define REDUCCION_GLOBAL 9
#define REDUCCION_GLOBAL_TERMINADA 5
#define ERROR_REDUCCION_GLOBAL 7
#define ALMACENADO_FINAL 15
#define ALMACENADO_FINAL_TERMINADO 16
#define ERROR_ALMACENADO_FINAL 17

t_log* loggerWorker;
char* IP_FILESYSTEM;
char* RUTA_DATABIN;
char* NOMBRE_NODO;
int PUERTO_FILESYSTEM;
int PUERTO_WORKER;

void sigchld_handler(int s){
	while(wait(NULL) > 0);
}

void eliminarProcesosMuertos(){
	struct sigaction sa;
	sa.sa_handler = sigchld_handler;
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = SA_RESTART;
	if (sigaction(SIGCHLD, &sa, NULL) == -1) {
		log_error(loggerWorker,"Error de sigaction al eliminar procesos muertos");
		exit(-1);
	}
}

void cargarWorker(t_config* configuracionWorker){
    if(!config_has_property(configuracionWorker, "IP_FILESYSTEM")){
        log_error(loggerWorker,"No se encuentra cargado IP_FILESYSTEM en el archivo.\n");
        exit(-1);
    }else{
        IP_FILESYSTEM = config_get_string_value(configuracionWorker, "IP_FILESYSTEM");
    }
    if(!config_has_property(configuracionWorker, "PUERTO_FILESYSTEM")){
    	log_error(loggerWorker,"No se encuentra cargado PUERTO_FILESYSTEM en el archivo.\n");
        exit(-1);
    }else{
        PUERTO_FILESYSTEM = config_get_int_value(configuracionWorker, "PUERTO_FILESYSTEM");
    }
    if(!config_has_property(configuracionWorker, "NOMBRE_NODO")){
    	log_error(loggerWorker,"No se encuentra cargado NOMBRE_NODO en el archivo.\n");
        exit(-1);
    }else{
        NOMBRE_NODO = string_new();
        string_append(&NOMBRE_NODO, config_get_string_value(configuracionWorker, "NOMBRE_NODO"));
    }
    if(!config_has_property(configuracionWorker, "PUERTO_WORKER")){
    	log_error(loggerWorker,"No se encuentra cargado PUERTO_WORKER en el archivo.\n");
        exit(-1);
    }else{
        PUERTO_WORKER = config_get_int_value(configuracionWorker, "PUERTO_WORKER");
    }
    if(!config_has_property(configuracionWorker, "RUTA_DATABIN")){
    	log_error(loggerWorker,"No se encuentra cargado RUTA_DATABIN en el archivo.\n");
        exit(-1);
    }else{
        RUTA_DATABIN = string_new();
        string_append(&RUTA_DATABIN, config_get_string_value(configuracionWorker, "RUTA_DATABIN"));
    }
    config_destroy(configuracionWorker);
}

void darPermisosAScripts(char* script){
	struct stat infoScript;

	if(chmod(script,S_IXUSR|S_IRUSR|S_IXGRP|S_IRGRP|S_IXOTH|S_IROTH|S_ISVTX)!=0){
		log_error(loggerWorker,"Error al otorgar permisos al script.\n");
	}
	else if(stat(script,&infoScript)!=0){
		log_error(loggerWorker,"No se pudo obtener informacion del script.");
	}
	else{
		log_info(loggerWorker,"Los permisos para el script son: %08x\n",infoScript.st_mode);
	}
}

char* crearComandoScriptTransformador(char* nombreScript,char* pathDestino, uint32_t nroBloque, uint32_t bytesOcupados){
	char* command = string_new();
	int bloqueAnterior = nroBloque-1;
	string_append(&command, "head -n ");
	string_append(&command,string_itoa((bloqueAnterior*1048576)+bytesOcupados));
	string_append(&command," ");
	string_append(&command,RUTA_DATABIN);
	string_append(&command," | tail -n ");
	string_append(&command,string_itoa(bytesOcupados));
	string_append(&command," | ./");
	string_append(&command,nombreScript);
	string_append(&command," | sort > ");
	string_append(&command,pathDestino);
	free(pathDestino);
	log_info(loggerWorker, "Se creo correctamente el comando del script transformador");
	return command;
}

char* crearComandoScriptReductor(char* archivoApareado,char* nombreScript,char* pathDestino){
	char* command = string_new();
	string_append(&command,archivoApareado);
	string_append(&command," | ./");
	string_append(&command,nombreScript);
	string_append(&command," > ");
	string_append(&command,pathDestino);
	free(pathDestino);
	free(archivoApareado);
	return command;
}

void guardarScript(char* script,char* nombreScript){
	string_append(&nombreScript,"XXXXXX");
	int resultado = mkstemp(nombreScript);

	if(resultado==-1){
		log_error(loggerWorker,"No se pudo crear un archivo temporal para guardar el script.\n");
		exit(-1);
	}

	FILE* archivoScript = fopen(nombreScript,"w");

	if(archivoScript==NULL){
		log_error(loggerWorker,"No se pudo guardar el script.\n");
		exit(-1);
	}

	if(fputs(script,archivoScript)==EOF){
		log_error(loggerWorker,"No se pudo escribir en el archivo del script.\n");
		exit(-1);
	}

	if(fclose(archivoScript)==EOF){
		log_error(loggerWorker,"No se pudo cerrar el archivo del script.\n");
	}

	free(script);
}

void eliminarScript(char* nombreScript){
	if(remove(nombreScript)!=0){
		log_error(loggerWorker,"No se pudo eliminar el script.\n");
	}
	else{
		log_info(loggerWorker,"El script se elimino correctamente.\n");
	}
	free(nombreScript);
}

char* aparearArchivos(t_list* archivosTemporales){
	return NULL;
}

void ejecutarPrograma(char* command,int socketMaster,uint32_t casoError,uint32_t casoExito){
	uint32_t resultado = system(command);
	switch(resultado){
	case -1:{
		log_error(loggerWorker, "Error al crear el hijo para ejecutar el programa con system.");
		sendDeNotificacion(socketMaster,casoError);
	}
	break;
	case 0:{
		log_error(loggerWorker, "Shell no disponible. (Error al disponer de los comandos.");
		sendDeNotificacion(socketMaster,casoError);
	}
	break;
	default:
		log_info(loggerWorker, "Script ejecutado correctamente");
		sendDeNotificacion(socketMaster,casoExito);
	}

	free(command);
}

void crearProcesoHijo(int socketMaster){
	log_info(loggerWorker, "Se recibio un job del socket de master %d.\n",socketMaster);
	int pipe_padreAHijo[2];
	int pipe_hijoAPadre[2];

	pipe(pipe_padreAHijo);
	pipe(pipe_hijoAPadre);
	log_info(loggerWorker, "Pipes creados\n");

	pid_t pid = fork();

	switch(pid){
	case -1:{
		log_error(loggerWorker, "No se pudo crear el proceso hijo\n");
		close(socketMaster);
		exit(-1);
	}
	break;
	case 0:{
		log_info(loggerWorker,"Soy el hijo con el pid %d y mi padre tiene el pid: %d \n",getpid(),getppid());

		dup2(pipe_padreAHijo[0],STDIN_FILENO);
		dup2(pipe_hijoAPadre[1],STDOUT_FILENO);

		close(pipe_padreAHijo[1]);
		close(pipe_hijoAPadre[0]);
		close(pipe_padreAHijo[0]);
		close(pipe_hijoAPadre[1]);

		uint32_t tipoEtapa = recibirUInt(socketMaster);

		switch(tipoEtapa){
		case TRANSFORMACION:{
			char* script = recibirString(socketMaster);
			char* nombreScript = recibirString(socketMaster);
			uint32_t nroBloque = recibirUInt(socketMaster);
			uint32_t bytesOcupados = recibirUInt(socketMaster);
			char* pathDestino = recibirString(socketMaster);

			log_info(loggerWorker, "Todos los datos fueron recibidos de master para realizar la transformacion");

			guardarScript(script,nombreScript);

			darPermisosAScripts(nombreScript);

			char* command = crearComandoScriptTransformador(nombreScript,pathDestino,nroBloque,bytesOcupados);

			ejecutarPrograma(command,socketMaster,ERROR_TRANSFORMACION,TRANSFORMACION_TERMINADA);

			eliminarArchivo(nombreScript);
		}
		break;
		case REDUCCION_LOCAL:{
			char* script = recibirString(socketMaster);
			char* pathDestino = recibirString(socketMaster);
			char* nombreScript = recibirString(socketMaster);
			uint32_t cantidadTemporales = recibirUInt(socketMaster);
			uint32_t posicion;
			t_list* archivosTemporales = list_create();
			for(posicion = 0; posicion < cantidadTemporales; posicion++){
				char* unArchivoTemporal = recibirString(socketMaster);
				list_add(archivosTemporales,unArchivoTemporal);
			}

			log_info(loggerWorker, "Todos los datos fueron recibidos de master para realizar la reduccion local");

			char* archivoApareado = aparearArchivos(archivosTemporales);

			guardarScript(script,nombreScript);

			darPermisosAScripts(nombreScript);

			char* command = crearComandoScriptReductor(archivoApareado,nombreScript,pathDestino);

			ejecutarPrograma(command,socketMaster,ERROR_REDUCCION_LOCAL,REDUCCION_LOCAL_TERMINADA);

			eliminarArchivo(nombreScript);
			eliminarArchivo(archivoApareado);

		}
		break;
		case REDUCCION_GLOBAL:{
			char* script = recibirString(socketMaster);
			char* pathDestino = recibirString(socketMaster);
						char* nombreScript = recibirString(socketMaster);
						uint32_t cantidadWorkers = recibirUInt(socketMaster);
						uint32_t posicionWorker;
						t_list* listaSocketsWorkers = list_create();
						for(posicionWorker = 0; posicionWorker < cantidadWorkers; posicionWorker++){
							char* archivoTemporal = recibirString(socketMaster);
							char* ipWorker = recibirString(socketMaster);
							uint32_t puertoWorker = recibirUInt(socketMaster);
							uint32_t unSocketWorker = conectarAServer(ipWorker, puertoWorker);
							realizarHandshakeWorker(archivoTemporal,unSocketWorker);
							list_add(listaSocketsWorkers,&unSocketWorker);
						}

						log_info(loggerWorker, "Todos los datos fueron recibidos de master para realizar la reduccion global");

						char* archivoApareado = realizarApareoGlobal(listaSocketsWorkers);

						guardarScript(script,nombreScript);

						darPermisosAScripts(nombreScript);

						char* command = crearComandoScriptReductor(archivoApareado,nombreScript,pathDestino);

						ejecutarPrograma(command,socketMaster,ERROR_REDUCCION_GLOBAL,REDUCCION_GLOBAL_TERMINADA);

						eliminarArchivo(nombreScript);
						eliminarArchivo(archivoApareado);
		}
		break;
		case ALMACENADO_FINAL:{
			char* nombreArchivoReduccionGlobal = recibirString(socketMaster);
						char* nombreResultante = recibirString(socketMaster);
						char* rutaResultante = recibirString(socketMaster);

						log_info(loggerWorker, "Todos los datos fueron recibidos de master para realizar el almacenado final");

						uint32_t socketFS = conectarAServer(IP_FILESYSTEM, PUERTO_FILESYSTEM);
						realizarHandshakeFS(socketFS);

						enviarDatosAFS(socketFS,nombreArchivoReduccionGlobal,nombreResultante,rutaResultante);

						int notificacion = recvDeNotificacion(socketFS);

						if(notificacion==ALMACENADO_FINAL_TERMINADO){
							sendDeNotificacion(socketMaster,ALMACENADO_FINAL_TERMINADO);
							close(socketFS);
						} else if(notificacion==ERROR_ALMACENADO_FINAL){
							sendDeNotificacion(socketMaster,ERROR_ALMACENADO_FINAL);
							close(socketFS);
						} else{
							log_error(loggerWorker, "La conexion recibida es erronea.\n");
							sendDeNotificacion(socketMaster,ERROR_ALMACENADO_FINAL);
							close(socketFS);
						}
		}
		break;
		default:
			log_error(loggerWorker, "Error al recibir paquete de Master");
			exit(-1);
		}
		close(socketMaster);

		exit(1);
	}
	break;
	default:
		close(socketMaster);

		log_info(loggerWorker,"Soy el proceso padre con pid: %d y mi hijo tiene el pid %d \n ",getpid(),pid);

		close(pipe_padreAHijo[0]);
		close(pipe_hijoAPadre[1]);
		close(pipe_padreAHijo[1]);
		close(pipe_hijoAPadre[0]);
	}
}

int main(int argc, char **argv) {
	loggerWorker = log_create("Worker.log", "Worker", 1, 0);
	chequearParametros(argc,2);
	t_config* configuracionWorker = generarTConfig(argv[1], 6);
	//t_config* configuracionWorker = generarTConfig("Debug/worker.ini", 5);
	cargarWorker(configuracionWorker);
	log_info(loggerWorker, "Se cargo correctamente Worker.\n");
	int socketAceptado, socketEscuchaWorker;
	socketEscuchaWorker = ponerseAEscucharClientes(PUERTO_WORKER, 0);
	eliminarProcesosMuertos();
	log_info(loggerWorker, "Procesos muertos eliminados del sistema.\n");
	while(1){
		socketAceptado = aceptarConexionDeCliente(socketEscuchaWorker);
		log_info(loggerWorker, "Se ha recibido una nueva conexion.\n");
		int notificacion = recvDeNotificacion(socketAceptado);
		switch(notificacion){
		case ES_MASTER:{
			log_info(loggerWorker, "Se recibio una conexion de master.\n");
			sendDeNotificacion(socketAceptado, ES_WORKER);
			crearProcesoHijo(socketAceptado);
		}
		break;
		case ES_WORKER:{
			log_info(loggerWorker, "Se recibio una conexion de otro worker.\n");
			sendDeNotificacion(socketAceptado, ES_OTRO_WORKER);
			char* nombreArchivoTemporal = recibirString(socketAceptado);
			enviarDatosAWorkerDesignado(socketAceptado,nombreArchivoTemporal);
		}
		break;
		default:
			log_error(loggerWorker, "La conexion recibida es erronea.\n");
			close(socketAceptado);
		}
	}
	close(socketEscuchaWorker);
	return EXIT_SUCCESS;
}
