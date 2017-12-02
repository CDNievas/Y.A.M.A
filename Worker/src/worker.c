#include "../../Biblioteca/src/Socket.c"
#include "../../Biblioteca/src/configParser.c"
#include <sys/mman.h>

#define TRANSFORMACION 1
#define TRANSFORMACION_TERMINADA 2
#define ERROR_TRANSFORMACION 18
#define REDUCCION_LOCAL 8
#define REDUCCION_LOCAL_TERMINADA 4
#define ERROR_REDUCCION_LOCAL 6
#define REDUCCION_GLOBAL 9
#define REDUCCION_GLOBAL_TERMINADA 5
#define ERROR_REDUCCION_GLOBAL 7
#define ALMACENADO_FINAL 15
#define ALMACENADO_FINAL_TERMINADO 17
#define ERROR_ALMACENADO_FINAL 16
#define APAREO_GLOBAL 18

t_log* loggerWorker;
char* IP_FILESYSTEM;
char* RUTA_DATABIN;
char* NOMBRE_NODO;
int PUERTO_FILESYSTEM;
int PUERTO_WORKER;
void* dataBinBloque;
size_t dataBinTamanio;
bool dataBinLiberado;

typedef struct{
	int socketParaRecibir;
	char* bloqueLeido;
	char* nombreNodo;
}infoApareoArchivo;

void sigchld_handler(int s){
	while(wait(NULL) > 0);
}

void eliminarProcesosMuertos(){
	struct sigaction sa;
	sa.sa_handler = sigchld_handler;
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = SA_RESTART;
	if (sigaction(SIGCHLD, &sa, NULL) == -1) {
		log_error(loggerWorker,"Error de sigaction al eliminar procesos muertos.\n");
		exit(-1);
	}
}

void cargarWorker(t_config* configuracionWorker){
    if(!config_has_property(configuracionWorker, "IP_FILESYSTEM")){
        log_error(loggerWorker,"No se encuentra cargado IP_FILESYSTEM en el archivo.\n");
        exit(-1);
    }else{
    	IP_FILESYSTEM = string_new();
    	string_append(&IP_FILESYSTEM, config_get_string_value(configuracionWorker, "IP_FILESYSTEM"));
//        IP_FILESYSTEM = config_get_string_value(configuracionWorker, "IP_FILESYSTEM");
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

char* leerLinea(FILE* unArchivo){
	char* lineaLeida = string_new();
	bool entro = false;

	while(!feof(unArchivo))
	{
		entro = true;
		char cadenaLeida[2] = " ";
		cadenaLeida[0] = fgetc(unArchivo);

		if ((cadenaLeida[0] != '\n') && (cadenaLeida[0] != EOF))
		{
			string_append(&lineaLeida,cadenaLeida);
		}
		else{
			break;
		}
	}

	if(!entro){
		string_append(&lineaLeida," \0");
	}

	log_info(loggerWorker, "Se obtuvo la siguiente linea del archivo.\n");

	return lineaLeida;
}

void darPermisosAScripts(char* script, int casoError, int socketMaster){
	struct stat infoScript;

	char* comandoAEjecutar = string_new();
	string_append(&comandoAEjecutar,"chmod 777 ");
	string_append(&comandoAEjecutar,script);

	int resultado = system(comandoAEjecutar);

	if(!WIFEXITED(resultado)){
		log_error(loggerWorker,"Error al otorgar permisos al script.\n");

		if(WIFSIGNALED(resultado)){
			log_error(loggerWorker, "La llamada al sistema termino con la senial %d\n",WTERMSIG(resultado));
		}

		sendDeNotificacion(socketMaster,casoError);
	}
	else{
		log_info(loggerWorker, "System para permisos ejecutado correctamente con el valor de retorno: %d\n",WEXITSTATUS(resultado));
	}

	if(stat(script,&infoScript)!=0){
		log_error(loggerWorker,"No se pudo obtener informacion del script.\n");
	}
	else{
		log_info(loggerWorker,"Los permisos para el script son: %08x\n",infoScript.st_mode);
	}

	free(comandoAEjecutar);
}

void crearArchivoTemporal(char* nombreScript,int casoError,int socketMaster){
	string_append(&nombreScript,"XXXXXX");
	log_info(loggerWorker,"nombre: %s \n",nombreScript);
	log_info(loggerWorker,"nombre: %d \n",strlen(nombreScript));
	int resultado = mkstemp(nombreScript);
	log_info(loggerWorker,"nombrePosmkstemp: %s \n",nombreScript);
	log_info(loggerWorker,"nombrePosmkstemp: %d \n",strlen(nombreScript));

	if(resultado==-1){
		log_error(loggerWorker,"No se pudo crear un archivo temporal para guardar el script.\n");
		sendDeNotificacion(socketMaster,casoError);
		exit(-1);
	}
	else{
		log_info(loggerWorker, "Se creo correctamente un archivo temporal con nombre: %s.\n",nombreScript);
	}
}

long int obtenerTamanioArchivo(FILE* unArchivo){
	int retornoSeek = fseek(unArchivo, 0, SEEK_END);

	if(retornoSeek!=0){
		log_error(loggerWorker,"Error de fseek.\n");
		exit(-1);
	}

	long int tamanioArchivo = ftell(unArchivo);

	if(tamanioArchivo==-1){
		log_error(loggerWorker,"Error de ftell.\n");
		exit(-1);
	}

	return tamanioArchivo;
}

char* leerArchivo(FILE* unArchivo, long int tamanioArchivo)
{
	int retornoSeek = fseek(unArchivo, 0, SEEK_SET);

	if(retornoSeek!=0){
		log_error(loggerWorker,"Error de fseek.\n");
		exit(-1);
	}

	char* contenidoArchivo = malloc(tamanioArchivo+1);

	if(contenidoArchivo==NULL){
		log_error(loggerWorker,"Error al asignar memoria para leer el archivo.\n");
		exit(-10);
	}

	fread(contenidoArchivo, tamanioArchivo, 1, unArchivo);
	contenidoArchivo[tamanioArchivo] = '\0';
	return contenidoArchivo;
}

char* obtenerContenido(char* unPath){
	FILE* archivoALeer = fopen(unPath, "r");

	if(archivoALeer==NULL){
		log_error(loggerWorker,"No se pudo abrir el archivo: %s.\n",unPath);
		exit(-1);
	}

	log_info(loggerWorker,"Se pudo abrir el archivo: %s.\n",unPath);

	long int tamanioArchivo = obtenerTamanioArchivo(archivoALeer);
	log_info(loggerWorker, "Se pudo obtener el tamanio del archivo: %s.\n",unPath);
	char* contenidoArchivo = leerArchivo(archivoALeer,tamanioArchivo);
	log_info(loggerWorker, "Se pudo obtener el contenido del archivo: %s.\n",unPath);
	return contenidoArchivo;
}

void eliminarArchivo(char* nombreScript){
	if(remove(nombreScript)!=0){
		log_error(loggerWorker,"No se pudo eliminar el script.\n");
	}
	else{
		log_info(loggerWorker,"El script se elimino correctamente.\n");
	}
	free(nombreScript);
}

void* dataBinMapear() {
	int descriptorArchivo = open(RUTA_DATABIN, O_CLOEXEC | O_RDWR);
	if (descriptorArchivo<0) {
		log_error(loggerWorker,"No se pudo abrir el data.bin \n");
		exit(-1);
	}
	struct stat estadoArchivo;
	if (fstat(descriptorArchivo, &estadoArchivo) == -1) {
		log_error(loggerWorker,"Fallo el fstat \n");
		exit(-1);
	}
	dataBinTamanio = estadoArchivo.st_size;
	void* puntero = mmap(0, dataBinTamanio, PROT_WRITE | PROT_READ | PROT_EXEC, MAP_SHARED, descriptorArchivo, 0);
	if (puntero == MAP_FAILED) {
		log_error(loggerWorker,"Fallo el mmap \n");
		exit(-1);
	}
	close(descriptorArchivo);
	return puntero;
}

char* obtenerBloque(uint32_t nroBloque,uint32_t bytesOcupados,int socketMaster,int casoError){
	char* comandoAEjecutar = string_new();
	char* nombreBloque = string_new();
	char* numeroBloque = string_itoa(nroBloque);
	char* numeroPID = string_itoa((int)getpid());
	string_append(&nombreBloque,"temporalBloque");
	string_append(&nombreBloque,numeroBloque);
	string_append(&nombreBloque,numeroPID);
	FILE* archivoScript = fopen(nombreBloque,"w");

	if(archivoScript==NULL){
		log_error(loggerWorker,"No se pudo abrir el archivo donde se guardara el bloque.\n");
		sendDeNotificacion(socketMaster,casoError);
		exit(-1);
	}
	else{
		log_info(loggerWorker, "Se pudo abrir el archivo donde se guardara el bloque: %s.\n",nombreBloque);
	}
	void* pedazoDataBin = malloc(bytesOcupados);
	memcpy(pedazoDataBin,dataBinBloque+(1048576*nroBloque),bytesOcupados);

	if(fwrite(pedazoDataBin,sizeof(char),bytesOcupados,archivoScript)!=bytesOcupados){
		log_error(loggerWorker,"No se pudo escribir en el archivo donde se guardara el bloque.\n");
		sendDeNotificacion(socketMaster,casoError);
		exit(-1);
	}
	else{
		log_info(loggerWorker, "Se pudo escribir en el archivo donde se guarda el bloque: %s.\n",nombreBloque);
	}

	if(fclose(archivoScript)==EOF){
		log_error(loggerWorker,"No se pudo cerrar el archivo donde se guarda el bloque.\n");
		sendDeNotificacion(socketMaster,casoError);
		exit(-1);
	}
	else{
		log_info(loggerWorker, "Se pudo cerrar el archivo donde se guarda el bloque: %s.\n",nombreBloque);
	}

	int resultado = system(comandoAEjecutar);

	if(!WIFEXITED(resultado)){
		log_error(loggerWorker,"Error al guardar el bloque a transformar del databin.\n");

		if(WIFSIGNALED(resultado)){
			log_error(loggerWorker, "La llamada al sistema termino con la senial %d\n",WTERMSIG(resultado));
		}

		sendDeNotificacion(socketMaster,ERROR_TRANSFORMACION);
	}
	else{
		log_info(loggerWorker, "System para obtener bloque ejecutado correctamente con el valor de retorno: %d\n",WEXITSTATUS(resultado));
	}

	free(comandoAEjecutar);
	free(numeroBloque);
	free(numeroPID);
	free(pedazoDataBin);
	//free(dataBinBloque);
	return nombreBloque;
}

char* crearComandoScriptTransformador(char* nombreScript,char* pathDestino, uint32_t nroBloque, uint32_t bytesOcupados,int socketMaster,char* bloque){
	char* command = string_new();
	string_append(&command,"cat ");
	string_append(&command,bloque);
	string_append(&command," | ./");
	string_append(&command,nombreScript);
	string_append(&command," | sort > ");
	string_append(&command,pathDestino);
	free(pathDestino);
	log_info(loggerWorker, "Se creo correctamente el comando del script transformador\n");
	return command;
}

char* crearComandoScriptReductor(char* archivoApareado,char* nombreScript,char* pathDestino){
	char* command = string_new();
	string_append(&command,"cat ");
	string_append(&command,archivoApareado);
	string_append(&command," | ./");
	string_append(&command,nombreScript);
	string_append(&command," > ");
	string_append(&command,pathDestino);
	free(pathDestino);
	printf("Comando script reductor: %s",command);
	log_info(loggerWorker, "Se creo correctamente el comando del script reductor\n");
	return command;
}

char* obtenerParteScript(char* rutaCompleta,uint32_t valor){
	uint32_t posicion;
	uint32_t posicionMaxima = 0;
	char* parteScript;

	for(posicion=0;rutaCompleta[posicion]!='\0';posicion++){
		if(rutaCompleta[posicion]=='.'){
			posicionMaxima = posicion;
		}
	}
	if(valor==1){
		parteScript = string_substring_from(rutaCompleta,posicionMaxima);
	}
	else{
		parteScript = string_substring_until(rutaCompleta,posicionMaxima);
	}

	return parteScript;
}

void guardarScript(char* script,char* nombreScript,int casoError,int socketMaster){
	FILE* archivoScript = fopen(nombreScript,"w");

	if(archivoScript==NULL){
		log_error(loggerWorker,"No se pudo abrir el archivo donde se guardara el script.\n");
		sendDeNotificacion(socketMaster,casoError);
		exit(-1);
	}
	else{
		log_info(loggerWorker, "Se pudo abrir el archivo donde se guardara el script: %s.\n",nombreScript);
	}

	if(fputs(script,archivoScript)==EOF){
		log_error(loggerWorker,"No se pudo escribir en el archivo del script.\n");
		sendDeNotificacion(socketMaster,casoError);
		exit(-1);
	}
	else{
		log_info(loggerWorker, "Se pudo escribir en el archivo donde se guarda el script: %s.\n",nombreScript);
	}

	if(fclose(archivoScript)==EOF){
		log_error(loggerWorker,"No se pudo cerrar el archivo del script.\n");
		sendDeNotificacion(socketMaster,casoError);
		exit(-1);
	}
	else{
		log_info(loggerWorker, "Se pudo cerrar el archivo donde se guarda el script: %s.\n",nombreScript);
	}

	free(script);
}

char* realizarApareoGlobal(t_list* listaInfoApareo, char* temporalEncargado, int casoError, int socketMaster){
	int posicion;
	char* archivoApareado = string_new();
	string_append(&archivoApareado,"archivoApareoGlobal");
	//crearArchivoTemporal(archivoApareado,casoError,socketMaster);

	FILE* archivoGlobalApareado = fopen(archivoApareado,"w");
	FILE* miTemporal = fopen(temporalEncargado,"r");
	free(temporalEncargado);

	if(archivoGlobalApareado==NULL){
		log_error(loggerWorker,"No se pudo abrir el archivo global apareado.\n");
		exit(-1);
	}

	log_info(loggerWorker, "Se creo el archivo donde se guarda lo apareado globalmente.\n");

	while(!list_is_empty(listaInfoApareo)){
		int cantidad = list_size(listaInfoApareo);

		for(posicion=0;posicion<cantidad;posicion++){
			infoApareoArchivo* unaInfoArchivo = list_remove(listaInfoApareo, 0);
			if((unaInfoArchivo->bloqueLeido)==NULL){
				if(strcmp(unaInfoArchivo->nombreNodo,NOMBRE_NODO)!=0){
					char* unPedacitoArchivo = recibirString(unaInfoArchivo->socketParaRecibir);

					if(strcmp(unPedacitoArchivo," \0")!=0){
						unaInfoArchivo->bloqueLeido = string_new();
						string_append(&(unaInfoArchivo->bloqueLeido),unPedacitoArchivo);
						list_add(listaInfoApareo,unaInfoArchivo);
					}
					else{
						free(unaInfoArchivo->bloqueLeido);
						free(unaInfoArchivo->nombreNodo);
						close(unaInfoArchivo->socketParaRecibir);
						free(unaInfoArchivo);
					}

					free(unPedacitoArchivo);
				}
				else{
					char* unPedacitoArchivo = leerLinea(miTemporal);

					if(strcmp(unPedacitoArchivo," \0")!=0){
						unaInfoArchivo->bloqueLeido = string_new();
						string_append(&(unaInfoArchivo->bloqueLeido),unPedacitoArchivo);
						list_add(listaInfoApareo,unaInfoArchivo);
					}
					else{
						free(unaInfoArchivo->bloqueLeido);
						free(unaInfoArchivo->nombreNodo);
						free(unaInfoArchivo);
					}

					free(unPedacitoArchivo);
				}
			}
			else{
				list_add(listaInfoApareo,unaInfoArchivo);
			}
		}

		log_info(loggerWorker, "Se recibio un set de stream de los workers.\n");

		cantidad = list_size(listaInfoApareo);
		char* menorString = NULL;
		infoApareoArchivo* infoMenorElejido = NULL;

		for(posicion=0;posicion<cantidad;posicion++){
			infoApareoArchivo* unaInfoArchivoConseguido = list_get(listaInfoApareo, posicion);
			if(menorString!=NULL){
				if(strcmp(unaInfoArchivoConseguido->bloqueLeido,menorString)<0){
					//log_info(loggerWorker, "El string %s es menor alfabeticamente que %s.\n",unaInfoArchivoConseguido->bloqueLeido,menorString);
					free(menorString);
					menorString = string_new();
					string_append(&menorString,unaInfoArchivoConseguido->bloqueLeido);
					infoMenorElejido = unaInfoArchivoConseguido;
				}
				else{
					//log_info(loggerWorker, "El string %s es menor alfabeticamente que %s.\n",menorString,unaInfoArchivoConseguido->bloqueLeido);
				}
			}
			else{
				menorString = string_new();
				string_append(&menorString,unaInfoArchivoConseguido->bloqueLeido);
				infoMenorElejido = unaInfoArchivoConseguido;
			}
		}

		if(infoMenorElejido!=NULL){
			free(infoMenorElejido->bloqueLeido);
			infoMenorElejido->bloqueLeido = NULL;

			string_append(&menorString,"\n");

			//log_info(loggerWorker, "El string menor alfabeticamente es %s.\n",menorString);

			if(fputs(menorString,archivoGlobalApareado)==EOF){
				log_error(loggerWorker,"No se pudo escribir en el archivo global apareado.\n");
				exit(-1);
			}

			log_info(loggerWorker, "El string: %s se escribio correctamente en el archivo global apareado.\n",menorString);

			free(menorString);
		}
	}


	if(fclose(archivoGlobalApareado)==EOF){
		log_error(loggerWorker,"No se pudo cerrar el archivo global apareado.\n");
	}

	log_info(loggerWorker, "Se cerro correctamente el archivo global apareado.\n");

	if(fclose(miTemporal)==EOF){
		log_error(loggerWorker,"No se pudo cerrar mi archivo de reduccion local.\n");
	}

	log_info(loggerWorker, "Se cerro correctamente mi archivo de reduccion local.\n");

	list_destroy(listaInfoApareo);

	return archivoApareado;
}

void ejecutarPrograma(char* command,int socketMaster,uint32_t casoError,uint32_t casoExito){
	int resultado = system(command);

	if(!WIFEXITED(resultado)){
		log_error(loggerWorker, "Error al ejecutar el script con system.\n");

		if(WIFSIGNALED(resultado)){
			log_error(loggerWorker, "La llamada al sistema termino con la senial %d\n",WTERMSIG(resultado));
		}

		sendDeNotificacion(socketMaster,casoError);
	}
	else{
		log_info(loggerWorker, "Script ejecutado correctamente con el valor de retorno: %d\n",WEXITSTATUS(resultado));
		sendDeNotificacion(socketMaster,casoExito);
	}

	free(command);
}

void realizarHandshakeWorker(char* unArchivoTemporal, int unSocketWorker){

	uint32_t tamanioArchivoTemporal = string_length(unArchivoTemporal);
	void* datosAEnviar = malloc(tamanioArchivoTemporal+sizeof(uint32_t));

	if(datosAEnviar==NULL){
		log_error(loggerWorker,"Error al asignar memoria para realizar handshake con otro worker.\n");
		exit(-10);
	}

	memcpy(datosAEnviar,&tamanioArchivoTemporal,sizeof(uint32_t));
	memcpy(datosAEnviar+sizeof(uint32_t),unArchivoTemporal,tamanioArchivoTemporal);

	log_info(loggerWorker, "Datos serializados para ser enviados al otro worker.\n");

	sendRemasterizado(unSocketWorker,ES_WORKER,tamanioArchivoTemporal+sizeof(uint32_t),datosAEnviar);

	if(recvDeNotificacion(unSocketWorker) != ES_OTRO_WORKER){
			log_error(loggerWorker, "La conexion efectuada no es con otro worker.\n");
			close(unSocketWorker);
			exit(-1);
	}

	log_info(loggerWorker, "Se conecto con otro worker.\n");

	free(unArchivoTemporal);
	free(datosAEnviar);
}

void realizarHandshakeFS(int socketFS){
	sendDeNotificacion(socketFS, ES_WORKER);

	if(recibirUInt(socketFS) != ES_FS){
		log_error(loggerWorker, "La conexion efectuada no es con FileSystem.\n");
		close(socketFS);
		exit(-1);
	}
	log_info(loggerWorker, "Se conecto con el FileSystem.\n");
}

void enviarDatosAFS(int socketFS,char* nombreArchivoReduccionGlobal,char* nombreResultante,char* rutaResultante){

	char* contenidoArchivoReduccionGlobal = obtenerContenido(nombreArchivoReduccionGlobal);

	uint32_t tamanioContenidoArchivoReduccionGlobal= string_length(contenidoArchivoReduccionGlobal);
	uint32_t tamanionombreResultante= string_length(nombreResultante);
	uint32_t tamaniorutaResultante= string_length(rutaResultante);
	uint32_t tamanioTotalAEnviar = tamanioContenidoArchivoReduccionGlobal+tamaniorutaResultante+tamanionombreResultante+(sizeof(uint32_t)*4);
	uint32_t tipoTexto = 22;

	void* datosAEnviar = malloc(tamanioTotalAEnviar);

	if(datosAEnviar==NULL){
		log_error(loggerWorker,"Error al asignar memoria para enviar datos al FileSystem.\n");
		exit(-10);
	}

	memcpy(datosAEnviar,&tamanioContenidoArchivoReduccionGlobal,sizeof(uint32_t));
	memcpy(datosAEnviar+sizeof(uint32_t),contenidoArchivoReduccionGlobal,tamanioContenidoArchivoReduccionGlobal);
	memcpy(datosAEnviar+tamanioContenidoArchivoReduccionGlobal+sizeof(uint32_t),&tamanionombreResultante,sizeof(uint32_t));
	memcpy(datosAEnviar+tamanioContenidoArchivoReduccionGlobal+sizeof(uint32_t)*2,nombreResultante,tamanionombreResultante);
	memcpy(datosAEnviar+tamanioContenidoArchivoReduccionGlobal+tamanionombreResultante+sizeof(uint32_t)*2,&tamaniorutaResultante,sizeof(uint32_t));
	memcpy(datosAEnviar+tamanioContenidoArchivoReduccionGlobal+tamanionombreResultante+sizeof(uint32_t)*3,rutaResultante,tamaniorutaResultante);
	memcpy(datosAEnviar+tamanioContenidoArchivoReduccionGlobal+tamanionombreResultante+tamaniorutaResultante+sizeof(uint32_t)*3,&tipoTexto,sizeof(uint32_t));

	log_info(loggerWorker, "Datos serializados correctamente para ser enviados al FileSystem\n");
	sendRemasterizado(socketFS,ALMACENADO_FINAL,tamanioTotalAEnviar,datosAEnviar);
}



void enviarDatosAWorkerDesignado(int socketAceptado,char* nombreArchivoTemporal){
	FILE* archivoTemporal = fopen(nombreArchivoTemporal,"r");
	while(!feof(archivoTemporal)){
		char* streamAEnviar = leerLinea(archivoTemporal);
		uint32_t tamanioLinea = strlen(streamAEnviar);
		void* datosAEnviar = malloc(sizeof(uint32_t)+tamanioLinea);
		memcpy(datosAEnviar,&tamanioLinea,sizeof(uint32_t));
		memcpy(datosAEnviar+sizeof(uint32_t),streamAEnviar,tamanioLinea);
		if(send(socketAceptado, datosAEnviar, tamanioLinea+sizeof(uint32_t), 0) == -1){
			perror("Error al enviar mensaje.");
			exit(-1);
		}
		free(datosAEnviar);
		free(streamAEnviar);
	}

	char* terminador = string_new();
	string_append(&terminador," /0");
	uint32_t tamanioTerminador = strlen(terminador);
	void* datoAEnviar = malloc(sizeof(uint32_t)+tamanioTerminador);
	memcpy(datoAEnviar,&tamanioTerminador,sizeof(uint32_t));
	memcpy(datoAEnviar+sizeof(uint32_t),terminador,tamanioTerminador);

	if(send(socketAceptado, datoAEnviar, tamanioTerminador+sizeof(uint32_t), 0) == -1){
		perror("Error al enviar mensaje.");
		exit(-1);
	}

	free(terminador);
	free(datoAEnviar);

	log_info(loggerWorker, "Todos los datos del archivo temporal reducido del worker fueron enviados\n");

	if(fclose(archivoTemporal)==EOF){
		log_error(loggerWorker,"No se pudo cerrar el archivo global apareado.\n");
	}
}

char* aparearArchivos(t_list* archivosTemporales,int socketMaster, int casoError){
	char* nombreArchivoApareado = string_new();
	string_append(&nombreArchivoApareado,"archivoApareadoTemporal");
	//crearArchivoTemporal(nombreArchivoApareado,casoError,socketMaster);
	char* numeroPID = string_itoa((int)getpid());
	string_append(&nombreArchivoApareado,numeroPID);
	char* comandoOrdenacionArchivos = string_new();
	string_append(&comandoOrdenacionArchivos,"sort -m ");
	int posicion;
	int cantidad = list_size(archivosTemporales);

	for(posicion=0;posicion<cantidad;posicion++){
		char* unArchivoTemporal = list_remove(archivosTemporales,0);
		string_append(&comandoOrdenacionArchivos,unArchivoTemporal);
		string_append(&comandoOrdenacionArchivos," ");
		free(unArchivoTemporal);
	}

	string_append(&comandoOrdenacionArchivos,"| cat > ");
	string_append(&comandoOrdenacionArchivos,nombreArchivoApareado);

	log_info(loggerWorker,"Comando para realizar apareo local de archivos fue correctamente creado.\n");

	int resultado = system(comandoOrdenacionArchivos);

	if(!WIFEXITED(resultado)){
		log_error(loggerWorker, "Error al ejecutar el comando para aparear archivos con system.\n");

		if(WIFSIGNALED(resultado)){
			log_error(loggerWorker, "La llamada al sistema termino con la senial %d\n",WTERMSIG(resultado));
		}

		sendDeNotificacion(socketMaster,casoError);

	}
	else{
		log_info(loggerWorker, "System para aparear archivos ejecutado correctamente con el valor de retorno: %d\n",WEXITSTATUS(resultado));
	}

	free(comandoOrdenacionArchivos);
	free(numeroPID);

	list_destroy(archivosTemporales);

	return nombreArchivoApareado;
}

char* obtenerNombreScriptTransformador(char* nombreScript,uint32_t nroBloque){
	char* nombreScriptSinExtension = obtenerParteScript(nombreScript,0);
	char* numeroBloque = string_itoa(nroBloque);
	char* numeroPID = string_itoa((int)getpid());
	string_append(&nombreScriptSinExtension,numeroBloque);
	string_append(&nombreScriptSinExtension,numeroPID);
	char* extensionScript = obtenerParteScript(nombreScript,1);
	string_append(&nombreScriptSinExtension,extensionScript);
	free(nombreScript);
	free(extensionScript);
	free(numeroBloque);
	free(numeroPID);
	return nombreScriptSinExtension;
}

char* obtenerNombreScriptReductor(char* nombreScript,char* archivoApareado){
	char* nombreScriptSinExtension = obtenerParteScript(nombreScript,0);
	char* numeroPID = string_itoa((int)getpid());
	string_append(&nombreScriptSinExtension,numeroPID);
	string_append(&nombreScriptSinExtension,archivoApareado);
	char* extensionScript = obtenerParteScript(nombreScript,1);
	string_append(&nombreScriptSinExtension,extensionScript);
	free(nombreScript);
	free(extensionScript);
	free(numeroPID);
	return nombreScriptSinExtension;
}

void crearProcesoHijo(int socketMaster, int socketEscuchaWorker){
	log_info(loggerWorker, "Se recibio un job del socket de master %d.\n",socketMaster);

	if(!fork())
	{
		close(socketEscuchaWorker);

		log_info(loggerWorker,"Soy el hijo con el pid %d y mi padre tiene el pid: %d \n",getpid(),getppid());

		uint32_t tipoEtapa = recibirUInt(socketMaster);

		switch(tipoEtapa){
		case TRANSFORMACION:{
			uint32_t nroBloque = recibirUInt(socketMaster);
			uint32_t bytesOcupados = recibirUInt(socketMaster);
			char* script = recibirString(socketMaster);
			char* nombreScript = recibirString(socketMaster);
			char* pathDestino = recibirString(socketMaster);

			log_info(loggerWorker, "Todos los datos fueron recibidos de master para realizar la transformacion");

			char* nombreScriptRemasterizado = obtenerNombreScriptTransformador(nombreScript,nroBloque);

			guardarScript(script,nombreScriptRemasterizado,ERROR_TRANSFORMACION,socketMaster);

			darPermisosAScripts(nombreScriptRemasterizado,ERROR_TRANSFORMACION,socketMaster);

			char* bloque = obtenerBloque(nroBloque,bytesOcupados,socketMaster,ERROR_TRANSFORMACION);

			char* command = crearComandoScriptTransformador(nombreScriptRemasterizado,pathDestino,nroBloque,bytesOcupados,socketMaster,bloque);

			ejecutarPrograma(command,socketMaster,ERROR_TRANSFORMACION,TRANSFORMACION_TERMINADA);

			eliminarArchivo(nombreScriptRemasterizado);

			eliminarArchivo(bloque);

			break;
		}
		case REDUCCION_LOCAL:{
			char* script = recibirString(socketMaster);
			char* nombreScript = recibirString(socketMaster);
			char* pathDestino = recibirString(socketMaster);
			uint32_t cantidadTemporales = recibirUInt(socketMaster);
			uint32_t posicion;
			t_list* archivosTemporales = list_create();
			for(posicion = 0; posicion < cantidadTemporales; posicion++){
				char* unArchivoTemporal = recibirString(socketMaster);
				list_add(archivosTemporales,unArchivoTemporal);
			}

			log_info(loggerWorker, "Todos los datos fueron recibidos de master para realizar la reduccion local");

			char* archivoApareado = aparearArchivos(archivosTemporales,socketMaster,ERROR_REDUCCION_LOCAL);

			char* nombreScriptRemasterizado = obtenerNombreScriptReductor(nombreScript,archivoApareado);

			guardarScript(script,nombreScriptRemasterizado,ERROR_REDUCCION_LOCAL,socketMaster);

			darPermisosAScripts(nombreScriptRemasterizado,ERROR_REDUCCION_LOCAL,socketMaster);

			char* command = crearComandoScriptReductor(archivoApareado,nombreScriptRemasterizado,pathDestino);

			ejecutarPrograma(command,socketMaster,ERROR_REDUCCION_LOCAL,REDUCCION_LOCAL_TERMINADA);

			eliminarArchivo(nombreScriptRemasterizado);

			eliminarArchivo(archivoApareado);

			break;
		}
		case REDUCCION_GLOBAL:{
			if(!dataBinLiberado){
				munmap(dataBinBloque,dataBinTamanio);
				dataBinLiberado = true;
			}
			char* script = recibirString(socketMaster);
			char* nombreScript = recibirString(socketMaster);
			char* pathDestino = recibirString(socketMaster);
			char* temporalEncargado = recibirString(socketMaster);
			uint32_t cantidadWorkers = recibirUInt(socketMaster);
			uint32_t posicionWorker;
			t_list* listaInfoApareo = list_create();
			infoApareoArchivo* infoEncargado = malloc(sizeof(infoApareoArchivo));
			infoEncargado->nombreNodo = string_new();
			infoEncargado->bloqueLeido = NULL;
			string_append(&(infoEncargado->nombreNodo),NOMBRE_NODO);
			list_add(listaInfoApareo,infoEncargado);

			for(posicionWorker = 0; posicionWorker < (cantidadWorkers-1); posicionWorker++){
				char* archivoTemporal = recibirString(socketMaster);
				char* ipWorker = recibirString(socketMaster);
				char* nombreNodo = recibirString(socketMaster);
				uint32_t puertoWorker = recibirUInt(socketMaster);
				infoApareoArchivo* unaInfoArchivo = malloc(sizeof(infoApareoArchivo));
				if(unaInfoArchivo==NULL){
					log_error(loggerWorker,"Error al asignar memoria en almacenamiento global.\n");
					exit(-10);
				}
				unaInfoArchivo->nombreNodo = string_new();
				unaInfoArchivo->bloqueLeido = NULL;
				string_append(&(unaInfoArchivo->nombreNodo),nombreNodo);
				int unSocketWorker = conectarAServer(ipWorker, puertoWorker);
				unaInfoArchivo->socketParaRecibir = unSocketWorker;
				list_add(listaInfoApareo,unaInfoArchivo);
				log_info(loggerWorker,"Se ha conectado con otro worker. IP: %s - PUERTO: %d \n",ipWorker,puertoWorker);
				realizarHandshakeWorker(archivoTemporal,unSocketWorker);
				free(ipWorker);
				free(nombreNodo);
			}

			log_info(loggerWorker, "Todos los datos fueron recibidos de master para realizar la reduccion global");

			char* archivoApareado = realizarApareoGlobal(listaInfoApareo,temporalEncargado,ERROR_REDUCCION_GLOBAL,socketMaster);

			char* nombreScriptRemasterizado = obtenerNombreScriptReductor(nombreScript,archivoApareado);

			guardarScript(script,nombreScriptRemasterizado,ERROR_REDUCCION_GLOBAL,socketMaster);

			darPermisosAScripts(nombreScriptRemasterizado,ERROR_REDUCCION_GLOBAL,socketMaster);

			char* command = crearComandoScriptReductor(archivoApareado,nombreScriptRemasterizado,pathDestino);

			ejecutarPrograma(command,socketMaster,ERROR_REDUCCION_GLOBAL,REDUCCION_GLOBAL_TERMINADA);

			eliminarArchivo(nombreScriptRemasterizado);

			eliminarArchivo(archivoApareado);

			sendDeNotificacion(socketMaster,REDUCCION_GLOBAL_TERMINADA);

			break;
		}
		case ALMACENADO_FINAL:{
			char* nombreArchivoReduccionGlobal = recibirString(socketMaster);
			char* nombreResultante = recibirString(socketMaster);
			char* rutaResultante = recibirString(socketMaster);
			log_debug(loggerWorker,"%s",nombreArchivoReduccionGlobal);
			log_debug(loggerWorker,"%s",nombreResultante);
			log_debug(loggerWorker,"%s",rutaResultante);
			log_info(loggerWorker, "Todos los datos fueron recibidos de master para realizar el almacenado final");

			log_debug(loggerWorker,"%s",IP_FILESYSTEM);
			log_debug(loggerWorker,"%d",PUERTO_FILESYSTEM);
			int socketFS = conectarAServer(IP_FILESYSTEM, PUERTO_FILESYSTEM);
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

			break;
		}
		default:{
			log_error(loggerWorker, "Error al recibir mensaje de Master\n");
			exit(-1);
		}
		}

		close(socketMaster);
		exit(0);
	}

	close(socketMaster);

	log_info(loggerWorker,"Soy el proceso padre con pid: %d \n ",getpid());
}

void laMardita(int signal){
	log_info(loggerWorker, "Se recibio la senial SIGINT, muriendo con estilo... \n");
	if(!dataBinLiberado){
		munmap(dataBinBloque,dataBinTamanio);
	}
	free(IP_FILESYSTEM);
	free(RUTA_DATABIN);
	free(NOMBRE_NODO);
	log_info(loggerWorker, "¡¡Adios logger!! \n");
	log_destroy(loggerWorker);
	exit(0);
}

int main(int argc, char **argv) {
	dataBinLiberado = true;
	signal(SIGINT, laMardita);
	loggerWorker = log_create("Worker.log", "Worker", 1, 0);
	chequearParametros(argc,2);
	t_config* configuracionWorker = generarTConfig(argv[1], 5);
	//t_config* configuracionWorker = generarTConfig("Debug/off_worker1.ini", 5);
	cargarWorker(configuracionWorker);
	log_info(loggerWorker, "Se cargo correctamente Worker.\n");
	int socketAceptado, socketEscuchaWorker;
	socketEscuchaWorker = ponerseAEscucharClientes(PUERTO_WORKER, 0);
	eliminarProcesosMuertos();
	log_info(loggerWorker, "Procesos hijos muertos eliminados del sistema.\n");
	dataBinBloque = dataBinMapear();
	dataBinLiberado = false;
	while(1){
		socketAceptado = aceptarConexionDeCliente(socketEscuchaWorker);
		log_info(loggerWorker, "Se ha recibido una nueva conexion.\n");
		int notificacion = recvDeNotificacion(socketAceptado);
		switch(notificacion){
		case ES_MASTER:{
			log_info(loggerWorker, "Se recibio una conexion de master.\n");
			sendDeNotificacion(socketAceptado, ES_WORKER);
			crearProcesoHijo(socketAceptado,socketEscuchaWorker);
			break;
		}
		case ES_WORKER:{
			if(!dataBinLiberado){
				munmap(dataBinBloque,dataBinTamanio);
				dataBinLiberado = true;
			}
			log_info(loggerWorker, "Se recibio una conexion de otro worker.\n");
			char* nombreArchivoTemporal = recibirString(socketAceptado);
			sendDeNotificacion(socketAceptado, ES_OTRO_WORKER);
			enviarDatosAWorkerDesignado(socketAceptado,nombreArchivoTemporal);
			close(socketAceptado);
			break;
		}
		default:{
			log_error(loggerWorker, "La conexion recibida es erronea.\n");
			close(socketAceptado);
			exit(-1);
		}
		}
	}
	close(socketEscuchaWorker);
	return EXIT_SUCCESS;
}
