#include "../../Biblioteca/src/Socket.c"
#include "../../Biblioteca/src/configParser.c"
#include <sys/mman.h>

#define CORTO 0
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
int contadorRandom;
int* numerosParalelos;
int PUERTO_WORKER;
void* dataBinBloque;
size_t dataBinTamanio;

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
		free(IP_FILESYSTEM);
		free(RUTA_DATABIN);
		free(NOMBRE_NODO);
		munmap(dataBinBloque,dataBinTamanio);
		log_info(loggerWorker, "¡¡Adios logger!! \n");
		log_destroy(loggerWorker);
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

void darPermisosAScripts(char* script, int casoError, int socketMaster){
	char* comandoAEjecutar = string_new();
	string_append(&comandoAEjecutar,"chmod 0777 ");
	string_append(&comandoAEjecutar,script);

	int resultado = system(comandoAEjecutar);

	free(comandoAEjecutar);

	if(!WIFEXITED(resultado)){
		log_error(loggerWorker,"Error al otorgar permisos al script.\n");

		if(WIFSIGNALED(resultado)){
			log_error(loggerWorker, "La llamada al sistema termino con la senial %d\n",WTERMSIG(resultado));
		}
	}
	else{
		log_info(loggerWorker, "System para permisos ejecutado correctamente con el valor de retorno: %d\n",WEXITSTATUS(resultado));
	}
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
		munmap(dataBinBloque,dataBinTamanio);
		free(numerosParalelos);
		free(IP_FILESYSTEM);
		free(RUTA_DATABIN);
		free(NOMBRE_NODO);
		log_info(loggerWorker, "¡¡Adios logger!! \n");
		log_destroy(loggerWorker);
		exit(-1);
	}

	long int tamanioArchivo = ftell(unArchivo);

	if(tamanioArchivo==-1){
		log_error(loggerWorker,"Error de ftell.\n");
		munmap(dataBinBloque,dataBinTamanio);
		free(numerosParalelos);
		free(IP_FILESYSTEM);
		free(RUTA_DATABIN);
		free(NOMBRE_NODO);
		log_info(loggerWorker, "¡¡Adios logger!! \n");
		log_destroy(loggerWorker);
		exit(-1);
	}

	return tamanioArchivo;
}

char* leerArchivo(FILE* unArchivo, long int tamanioArchivo)
{
	int retornoSeek = fseek(unArchivo, 0, SEEK_SET);

	if(retornoSeek!=0){
		log_error(loggerWorker,"Error de fseek.\n");
		munmap(dataBinBloque,dataBinTamanio);
		free(numerosParalelos);
		free(IP_FILESYSTEM);
		free(RUTA_DATABIN);
		free(NOMBRE_NODO);
		log_info(loggerWorker, "¡¡Adios logger!! \n");
		log_destroy(loggerWorker);
		exit(-1);
	}

	char* contenidoArchivo = malloc(tamanioArchivo+1);

	if(contenidoArchivo==NULL){
		log_error(loggerWorker,"Error al asignar memoria para leer el archivo.\n");
		munmap(dataBinBloque,dataBinTamanio);
		free(IP_FILESYSTEM);
		free(numerosParalelos);
		free(RUTA_DATABIN);
		free(NOMBRE_NODO);
		log_info(loggerWorker, "¡¡Adios logger!! \n");
		log_destroy(loggerWorker);
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
		munmap(dataBinBloque,dataBinTamanio);
		free(numerosParalelos);
		free(IP_FILESYSTEM);
		free(RUTA_DATABIN);
		free(NOMBRE_NODO);
		log_info(loggerWorker, "¡¡Adios logger!! \n");
		log_destroy(loggerWorker);
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
		log_error(loggerWorker,"No se pudo eliminar el archivo.\n");
	}
	else{
		log_info(loggerWorker,"El archivo se elimino correctamente.\n");
	}
	free(nombreScript);
}

void* dataBinMapear() {
	//int descriptorArchivo = open(RUTA_DATABIN, O_RDONLY);
	int descriptorArchivo = open(RUTA_DATABIN, O_CLOEXEC | O_RDONLY);
	if (descriptorArchivo==-1) {
		log_error(loggerWorker,"No se pudo abrir el data.bin \n");
		free(IP_FILESYSTEM);
		free(RUTA_DATABIN);
		free(NOMBRE_NODO);
		free(numerosParalelos);
		log_info(loggerWorker, "¡¡Adios logger!! \n");
		log_destroy(loggerWorker);
		exit(-1);
	}
	log_info(loggerWorker, "Se pudo abrir el data.bin \n");
	struct stat estadoArchivo;
	if (fstat(descriptorArchivo, &estadoArchivo) == -1) {
		log_error(loggerWorker,"Fallo el fstat \n");
		free(IP_FILESYSTEM);
		free(RUTA_DATABIN);
		free(NOMBRE_NODO);
		free(numerosParalelos);
		log_info(loggerWorker, "¡¡Adios logger!! \n");
		log_destroy(loggerWorker);
		exit(-1);
	}
	dataBinTamanio = estadoArchivo.st_size;
	void* puntero = mmap(0, dataBinTamanio, PROT_READ, MAP_PRIVATE, descriptorArchivo, 0);
	if (puntero == MAP_FAILED) {
		log_error(loggerWorker,"Fallo el mmap \n");
		free(IP_FILESYSTEM);
		free(RUTA_DATABIN);
		free(NOMBRE_NODO);
		free(numerosParalelos);
		log_info(loggerWorker, "¡¡Adios logger!! \n");
		log_destroy(loggerWorker);
		exit(-1);
	}
	log_info(loggerWorker, "Se realizo correctamente el mmap \n");
	if(close(descriptorArchivo)==-1){
		log_error(loggerWorker,"Fallo al cerrar el databin \n");
		free(IP_FILESYSTEM);
		free(RUTA_DATABIN);
		free(NOMBRE_NODO);
		munmap(dataBinBloque,dataBinTamanio);
		free(numerosParalelos);
		log_info(loggerWorker, "¡¡Adios logger!! \n");
		log_destroy(loggerWorker);
		exit(-1);
	}
	log_info(loggerWorker, "Se cerro correctamente el data.bin \n");

	return puntero;
}

char* obtenerBloque(uint32_t nroBloque,uint32_t bytesOcupados,int socketMaster,int casoError){
	char* nombreBloque = string_new();
	string_append(&nombreBloque,"temporalBloque");
	char* numeroBloque = string_itoa(nroBloque);
	string_append(&nombreBloque,numeroBloque);
	free(numeroBloque);
	char* numeroPID = string_itoa((int)getpid());
	string_append(&nombreBloque,numeroPID);
	free(numeroPID);
	char* numeroRandom = string_itoa(contadorRandom);
	contadorRandom++;
	string_append(&nombreBloque,numeroRandom);
	free(numeroRandom);
	FILE* archivoScript = fopen(nombreBloque,"w");
	if(archivoScript==NULL){
		log_error(loggerWorker,"No se pudo abrir el archivo donde se guardara el bloque.\n");
		sendDeNotificacion(socketMaster,casoError);
		munmap(dataBinBloque,dataBinTamanio);
		free(numerosParalelos);
		free(nombreBloque);
		free(IP_FILESYSTEM);
		free(RUTA_DATABIN);
		free(NOMBRE_NODO);
		close(socketMaster);
		log_info(loggerWorker, "¡¡Adios logger!! \n");
		log_destroy(loggerWorker);
		exit(-1);
	}
	else{
		log_info(loggerWorker, "Se pudo abrir el archivo donde se guardara el bloque: %s.\n",nombreBloque);
	}

	if(fwrite(dataBinBloque+(1048576*nroBloque),sizeof(char),bytesOcupados,archivoScript)!=bytesOcupados){
		log_error(loggerWorker,"No se pudo escribir en el archivo donde se guardara el bloque.\n");
		sendDeNotificacion(socketMaster,casoError);
		munmap(dataBinBloque,dataBinTamanio);
		free(IP_FILESYSTEM);
		free(RUTA_DATABIN);
		free(nombreBloque);
		free(NOMBRE_NODO);
		free(numerosParalelos);
		close(socketMaster);
		log_info(loggerWorker, "¡¡Adios logger!! \n");
		log_destroy(loggerWorker);
		exit(-1);
	}
	else{
		log_info(loggerWorker, "Se pudo escribir en el archivo donde se guarda el bloque: %s.\n",nombreBloque);
	}

	if(fclose(archivoScript)==EOF){
		log_error(loggerWorker,"No se pudo cerrar el archivo donde se guarda el bloque.\n");
		sendDeNotificacion(socketMaster,casoError);
		munmap(dataBinBloque,dataBinTamanio);
		free(IP_FILESYSTEM);
		free(RUTA_DATABIN);
		free(nombreBloque);
		free(NOMBRE_NODO);
		free(numerosParalelos);
		close(socketMaster);
		log_info(loggerWorker, "¡¡Adios logger!! \n");
		log_destroy(loggerWorker);
		exit(-1);
	}
	else{
		log_info(loggerWorker, "Se pudo cerrar el archivo donde se guarda el bloque: %s.\n",nombreBloque);
	}

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
	log_debug(loggerWorker, "Se creo correctamente el comando del script transformador\n");
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
	log_debug(loggerWorker, "Se creo correctamente el comando del script reductor\n");
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
		log_error(loggerWorker,"No se pudo abrir el archivo donde se guardara el contenido.\n");
		sendDeNotificacion(socketMaster,casoError);
		munmap(dataBinBloque,dataBinTamanio);
		free(IP_FILESYSTEM);
		free(RUTA_DATABIN);
		free(NOMBRE_NODO);
		free(numerosParalelos);
		free(script);
		free(nombreScript);
		close(socketMaster);
		log_info(loggerWorker, "¡¡Adios logger!! \n");
		log_destroy(loggerWorker);
		exit(-1);
	}
	else{
		log_info(loggerWorker, "Se pudo abrir el archivo donde se guardara el contenido: %s.\n",nombreScript);
	}

	if(fputs(script,archivoScript)==EOF){
		log_error(loggerWorker,"No se pudo escribir en el archivo donde se guardara el contenido.\n");
		sendDeNotificacion(socketMaster,casoError);
		munmap(dataBinBloque,dataBinTamanio);
		free(IP_FILESYSTEM);
		free(RUTA_DATABIN);
		free(NOMBRE_NODO);
		free(numerosParalelos);
		free(script);
		free(nombreScript);
		close(socketMaster);
		log_info(loggerWorker, "¡¡Adios logger!! \n");
		log_destroy(loggerWorker);
		exit(-1);
	}
	else{
		free(script);
		log_info(loggerWorker, "Se pudo escribir en el archivo donde se guardara el contenido: %s.\n",nombreScript);
	}

	if(fclose(archivoScript)==EOF){
		log_error(loggerWorker,"No se pudo cerrar el archivo donde se guardara el contenido.\n");
		sendDeNotificacion(socketMaster,casoError);
		munmap(dataBinBloque,dataBinTamanio);
		free(IP_FILESYSTEM);
		free(RUTA_DATABIN);
		free(NOMBRE_NODO);
		free(numerosParalelos);
		free(nombreScript);
		close(socketMaster);
		log_info(loggerWorker, "¡¡Adios logger!! \n");
		log_destroy(loggerWorker);
		exit(-1);
	}
	else{
		log_info(loggerWorker, "Se pudo cerrar el archivo donde se guardara el contenido: %s.\n",nombreScript);
	}

}

char* obtenerNombreTemporal(uint32_t posicion){
	char* nombreTemporal = string_new();
	string_append(&nombreTemporal,"temporalGlobal");
	char* numeroPID = string_itoa((int)getpid());
	string_append(&nombreTemporal,numeroPID);
	free(numeroPID);
	char* posicionTexto = string_itoa(posicion);
	string_append(&nombreTemporal,posicionTexto);
	free(posicionTexto);
	char* numeroRandom = string_itoa(contadorRandom);
	contadorRandom++;
	string_append(&nombreTemporal,numeroRandom);
	free(numeroRandom);
	return nombreTemporal;
}

void eliminarArchivosLista(t_list* listaInfoApareo){
	uint32_t tamanioLista = list_size(listaInfoApareo);
	uint32_t posicion;
	for(posicion=0;posicion<tamanioLista;posicion++){
		char* nombreTemporalGlobal = list_remove(listaInfoApareo,0);
		eliminarArchivo(nombreTemporalGlobal);
	}
	list_destroy(listaInfoApareo);
}

char* realizarApareoGlobal(t_list* listaInfoApareo, char* temporalEncargado, int casoError, int socketMaster){
	uint32_t tamanioLista = list_size(listaInfoApareo);
	uint32_t posicion;
	char* comandoOrdenacionArchivos = string_new();
	string_append(&comandoOrdenacionArchivos,"sort -m ");
	string_append(&comandoOrdenacionArchivos,temporalEncargado);
	free(temporalEncargado);

	for(posicion=0;posicion<tamanioLista;posicion++){
		int unSocketWorker = list_remove(listaInfoApareo,0);
		int notificacion = recvDeNotificacion(unSocketWorker);

		if(notificacion==ES_OTRO_WORKER){
			log_debug(loggerWorker, "Se conecto con otro worker.\n");
			char* contenidoTemporal = recibirStringModificado(unSocketWorker);
			char* nombreTemporal = obtenerNombreTemporal(posicion);
			guardarScript(contenidoTemporal,nombreTemporal,ERROR_REDUCCION_GLOBAL,socketMaster);
			string_append(&comandoOrdenacionArchivos," ");
			string_append(&comandoOrdenacionArchivos,nombreTemporal);
			close(unSocketWorker);
			list_add(listaInfoApareo,nombreTemporal);
		}
		else{
			log_error(loggerWorker, "La conexion con el otro worker es erronea.\n");
			sendDeNotificacion(socketMaster,casoError);
			free(IP_FILESYSTEM);
			free(RUTA_DATABIN);
			free(NOMBRE_NODO);
			list_destroy(listaInfoApareo);
			munmap(dataBinBloque,dataBinTamanio);
			free(numerosParalelos);
			close(unSocketWorker);
			close(socketMaster);
			log_info(loggerWorker, "¡¡Adios logger!! \n");
			log_destroy(loggerWorker);
			exit(-1);
		}
	}

	string_append(&comandoOrdenacionArchivos," | cat > ");

	char* archivoApareado = string_new();
	string_append(&archivoApareado,"archivoApareoGlobal");
	char* numeroPID = string_itoa((int)getpid());
	string_append(&archivoApareado,numeroPID);
	free(numeroPID);
	char* numeroRandom = string_itoa(contadorRandom);
	contadorRandom++;
	string_append(&archivoApareado,numeroRandom);
	free(numeroRandom);
	string_append(&comandoOrdenacionArchivos,archivoApareado);

	log_info(loggerWorker,"Comando para realizar apareo global de archivos fue correctamente creado.\n");

	int resultado = system(comandoOrdenacionArchivos);

	free(comandoOrdenacionArchivos);

	if(!WIFEXITED(resultado)){
		log_error(loggerWorker, "Error al ejecutar el comando para aparear archivos con system.\n");

		if(WIFSIGNALED(resultado)){
			log_error(loggerWorker, "La llamada al sistema termino con la senial %d\n",WTERMSIG(resultado));
		}

		sendDeNotificacion(socketMaster,casoError);
		eliminarArchivosLista(listaInfoApareo);
		free(IP_FILESYSTEM);
		free(RUTA_DATABIN);
		free(NOMBRE_NODO);
		munmap(dataBinBloque,dataBinTamanio);
		free(numerosParalelos);
		close(socketMaster);
		log_info(loggerWorker, "¡¡Adios logger!! \n");
		log_destroy(loggerWorker);
		exit(-1);
	}
	else{
		eliminarArchivosLista(listaInfoApareo);
		log_info(loggerWorker, "System para aparear archivos ejecutado correctamente con el valor de retorno: %d\n",WEXITSTATUS(resultado));
	}

	return archivoApareado;
}

void ejecutarPrograma(char* command,int socketMaster,uint32_t casoError,uint32_t casoExito,char* unArchivo,char* otroArchivo){
	int resultado = system(command);

	free(command);

	if(!WIFEXITED(resultado)){
		log_error(loggerWorker, "Error al ejecutar el script con system.\n");

		if(WIFSIGNALED(resultado)){
			log_error(loggerWorker, "La llamada al sistema termino con la senial %d\n",WTERMSIG(resultado));
		}

		sendDeNotificacion(socketMaster,casoError);

		munmap(dataBinBloque,dataBinTamanio);

		eliminarArchivo(unArchivo);
		eliminarArchivo(otroArchivo);
		free(IP_FILESYSTEM);
		free(RUTA_DATABIN);
		free(NOMBRE_NODO);
		free(numerosParalelos);
		log_info(loggerWorker, "¡¡Adios logger!! \n");
		log_destroy(loggerWorker);
		close(socketMaster);
		exit(-1);
	}
	else{
		log_info(loggerWorker, "Script ejecutado correctamente con el valor de retorno: %d\n",WEXITSTATUS(resultado));
		sendDeNotificacion(socketMaster,casoExito);
	}

}

void realizarHandshakeWorker(char* unArchivoTemporal, int unSocketWorker){

	uint32_t tamanioArchivoTemporal = string_length(unArchivoTemporal);
	void* datosAEnviar = malloc(tamanioArchivoTemporal+sizeof(uint32_t));

	memcpy(datosAEnviar,&tamanioArchivoTemporal,sizeof(uint32_t));
	memcpy(datosAEnviar+sizeof(uint32_t),unArchivoTemporal,tamanioArchivoTemporal);

	free(unArchivoTemporal);

	log_debug(loggerWorker, "Datos serializados para ser enviados al otro worker.\n");

	sendRemasterizado(unSocketWorker,ES_WORKER,tamanioArchivoTemporal+sizeof(uint32_t),datosAEnviar);

	free(datosAEnviar);
}

void realizarHandshakeFS(int socketFS, int socketAceptado){
	sendDeNotificacion(socketFS, ES_WORKER);
	int notificacion = recvDeNotificacion(socketFS);

	if(notificacion != ES_FS){
		log_error(loggerWorker, "La conexion efectuada no es con FileSystem.\n");
		sendDeNotificacion(socketAceptado,ERROR_ALMACENADO_FINAL);
		munmap(dataBinBloque,dataBinTamanio);
		free(numerosParalelos);
		close(socketAceptado);
		close(socketFS);
		free(IP_FILESYSTEM);
		free(RUTA_DATABIN);
		free(NOMBRE_NODO);
		log_info(loggerWorker, "¡¡Adios logger!! \n");
		log_destroy(loggerWorker);
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

	memcpy(datosAEnviar,&tamanioContenidoArchivoReduccionGlobal,sizeof(uint32_t));
	memcpy(datosAEnviar+sizeof(uint32_t),contenidoArchivoReduccionGlobal,tamanioContenidoArchivoReduccionGlobal);
	free(contenidoArchivoReduccionGlobal);
	memcpy(datosAEnviar+tamanioContenidoArchivoReduccionGlobal+sizeof(uint32_t),&tamanionombreResultante,sizeof(uint32_t));
	memcpy(datosAEnviar+tamanioContenidoArchivoReduccionGlobal+sizeof(uint32_t)*2,nombreResultante,tamanionombreResultante);
	memcpy(datosAEnviar+tamanioContenidoArchivoReduccionGlobal+tamanionombreResultante+sizeof(uint32_t)*2,&tamaniorutaResultante,sizeof(uint32_t));
	memcpy(datosAEnviar+tamanioContenidoArchivoReduccionGlobal+tamanionombreResultante+sizeof(uint32_t)*3,rutaResultante,tamaniorutaResultante);
	memcpy(datosAEnviar+tamanioContenidoArchivoReduccionGlobal+tamanionombreResultante+tamaniorutaResultante+sizeof(uint32_t)*3,&tipoTexto,sizeof(uint32_t));

	log_info(loggerWorker, "Datos serializados correctamente para ser enviados al FileSystem\n");
	sendRemasterizado(socketFS,ALMACENADO_FINAL,tamanioTotalAEnviar,datosAEnviar);

	free(datosAEnviar);
}

char* aparearArchivos(t_list* archivosTemporales,int socketMaster, int casoError){
	int posicion;
	int cantidad = list_size(archivosTemporales);

	char* comandoOrdenacionArchivos = string_new();
	string_append(&comandoOrdenacionArchivos,"sort -m ");

	for(posicion=0;posicion<cantidad;posicion++){
		char* unArchivoTemporal = list_remove(archivosTemporales,0);
		string_append(&comandoOrdenacionArchivos,unArchivoTemporal);
		string_append(&comandoOrdenacionArchivos," ");
		free(unArchivoTemporal);
	}

	list_destroy(archivosTemporales);

	string_append(&comandoOrdenacionArchivos,"| cat > ");

	char* nombreArchivoApareado = string_new();
	string_append(&nombreArchivoApareado,"archivoApareadoTemporal");
	char* numeroPID = string_itoa((int)getpid());
	string_append(&nombreArchivoApareado,numeroPID);
	free(numeroPID);
	char* numeroRandom = string_itoa(contadorRandom);
	contadorRandom++;
	string_append(&nombreArchivoApareado,numeroRandom);
	free(numeroRandom);

	string_append(&comandoOrdenacionArchivos,nombreArchivoApareado);

	log_debug(loggerWorker,"Comando para realizar apareo local de archivos fue correctamente creado.\n");

	int resultado = system(comandoOrdenacionArchivos);

	free(comandoOrdenacionArchivos);

	if(!WIFEXITED(resultado)){
		log_error(loggerWorker, "Error al ejecutar el comando para aparear archivos con system.\n");

		if(WIFSIGNALED(resultado)){
			log_error(loggerWorker, "La llamada al sistema termino con la senial %d\n",WTERMSIG(resultado));
		}

		sendDeNotificacion(socketMaster,casoError);
		free(IP_FILESYSTEM);
		free(RUTA_DATABIN);
		free(NOMBRE_NODO);
		free(numerosParalelos);
		free(nombreArchivoApareado);
		log_info(loggerWorker, "¡¡Adios logger!! \n");
		log_destroy(loggerWorker);
		close(socketMaster);
		exit(-1);

	}
	else{
		log_debug(loggerWorker, "System para aparear archivos ejecutado correctamente con el valor de retorno: %d\n",WEXITSTATUS(resultado));
	}

	return nombreArchivoApareado;
}

char* obtenerNombreScriptTransformador(char* nombreScript,uint32_t nroBloque){
	char* nombreScriptSinExtension = obtenerParteScript(nombreScript,0);
	char* numeroBloque = string_itoa(nroBloque);
	string_append(&nombreScriptSinExtension,numeroBloque);
	free(numeroBloque);
	char* numeroPID = string_itoa((int)getpid());
	string_append(&nombreScriptSinExtension,numeroPID);
	free(numeroPID);
	char* numeroRandom = string_itoa(contadorRandom);
	contadorRandom++;
	string_append(&nombreScriptSinExtension,numeroRandom);
	free(numeroRandom);
	char* extensionScript = obtenerParteScript(nombreScript,1);
	string_append(&nombreScriptSinExtension,extensionScript);
	free(nombreScript);
	free(extensionScript);
	return nombreScriptSinExtension;
}

char* obtenerNombreScriptReductor(char* nombreScript,char* archivoApareado){
	char* nombreScriptSinExtension = obtenerParteScript(nombreScript,0);
	char* numeroPID = string_itoa((int)getpid());
	string_append(&nombreScriptSinExtension,numeroPID);
	free(numeroPID);
	char* numeroRandom = string_itoa(contadorRandom);
	contadorRandom++;
	string_append(&nombreScriptSinExtension,numeroRandom);
	free(numeroRandom);
	string_append(&nombreScriptSinExtension,archivoApareado);
	char* extensionScript = obtenerParteScript(nombreScript,1);
	string_append(&nombreScriptSinExtension,extensionScript);
	free(nombreScript);
	free(extensionScript);
	return nombreScriptSinExtension;
}

void crearProcesoHijo(int socketAceptado, int socketEscuchaWorker){

	if(!fork())
	{
		(*numerosParalelos)++;
		close(socketEscuchaWorker);

		log_info(loggerWorker,"Soy el hijo con el pid %d y mi padre tiene el pid: %d \n",getpid(),getppid());

		int notificacion = recvDeNotificacion(socketAceptado);

		switch(notificacion){
		case ES_MASTER:{
			log_debug(loggerWorker, "Se recibio una conexion de master.\n");
			sendDeNotificacion(socketAceptado, ES_WORKER);

			int tipoEtapa = recvDeNotificacion(socketAceptado);

			log_info(loggerWorker, "Se recibio un job del socket de master %d.\n",socketAceptado);

			switch(tipoEtapa){
			case TRANSFORMACION:{
				uint32_t nroBloque = recibirUInt(socketAceptado);
				uint32_t bytesOcupados = recibirUInt(socketAceptado);
				char* script = recibirString(socketAceptado);
				char* nombreScript = recibirString(socketAceptado);
				char* pathDestino = recibirString(socketAceptado);

				log_debug(loggerWorker, "Todos los datos fueron recibidos de master para realizar la transformacion");

				char* nombreScriptRemasterizado = obtenerNombreScriptTransformador(nombreScript,nroBloque);

				guardarScript(script,nombreScriptRemasterizado,ERROR_TRANSFORMACION,socketAceptado);

				darPermisosAScripts(nombreScriptRemasterizado,ERROR_TRANSFORMACION,socketAceptado);

				char* bloque = obtenerBloque(nroBloque,bytesOcupados,socketAceptado,ERROR_TRANSFORMACION);

				char* command = crearComandoScriptTransformador(nombreScriptRemasterizado,pathDestino,nroBloque,bytesOcupados,socketAceptado,bloque);

				ejecutarPrograma(command,socketAceptado,ERROR_TRANSFORMACION,TRANSFORMACION_TERMINADA,bloque,nombreScriptRemasterizado);

				eliminarArchivo(nombreScriptRemasterizado);

				eliminarArchivo(bloque);

				break;
			}
			case REDUCCION_LOCAL:{
				char* script = recibirString(socketAceptado);
				char* nombreScript = recibirString(socketAceptado);
				char* pathDestino = recibirString(socketAceptado);
				uint32_t cantidadTemporales = recibirUInt(socketAceptado);
				uint32_t posicion;
				t_list* archivosTemporales = list_create();
				for(posicion = 0; posicion < cantidadTemporales; posicion++){
					char* unArchivoTemporal = recibirString(socketAceptado);
					list_add(archivosTemporales,unArchivoTemporal);
				}

				log_debug(loggerWorker, "Todos los datos fueron recibidos de master para realizar la reduccion local");

				char* archivoApareado = aparearArchivos(archivosTemporales,socketAceptado,ERROR_REDUCCION_LOCAL);

				char* nombreScriptRemasterizado = obtenerNombreScriptReductor(nombreScript,archivoApareado);

				guardarScript(script,nombreScriptRemasterizado,ERROR_REDUCCION_LOCAL,socketAceptado);

				darPermisosAScripts(nombreScriptRemasterizado,ERROR_REDUCCION_LOCAL,socketAceptado);

				char* command = crearComandoScriptReductor(archivoApareado,nombreScriptRemasterizado,pathDestino);

				ejecutarPrograma(command,socketAceptado,ERROR_REDUCCION_LOCAL,REDUCCION_LOCAL_TERMINADA,nombreScriptRemasterizado,archivoApareado);

				eliminarArchivo(nombreScriptRemasterizado);

				eliminarArchivo(archivoApareado);

				break;
			}
			case REDUCCION_GLOBAL:{
				char* script = recibirString(socketAceptado);
				char* nombreScript = recibirString(socketAceptado);
				char* pathDestino = recibirString(socketAceptado);
				char* temporalEncargado = recibirString(socketAceptado);
				uint32_t cantidadWorkers = recibirUInt(socketAceptado);
				uint32_t posicionWorker;
				t_list* listaSocketsApareo = list_create();

				for(posicionWorker = 0; posicionWorker < (cantidadWorkers-1); posicionWorker++){
					char* archivoTemporal = recibirString(socketAceptado);
					char* ipWorker = recibirString(socketAceptado);
					uint32_t puertoWorker = recibirUInt(socketAceptado);
					int unSocketWorker = conectarWorker(ipWorker, puertoWorker);
					if(unSocketWorker!=-1){
						log_info(loggerWorker,"Se ha conectado con otro worker. IP: %s - PUERTO: %d \n",ipWorker,puertoWorker);
						free(ipWorker);
						realizarHandshakeWorker(archivoTemporal,unSocketWorker);
						list_add(listaSocketsApareo, unSocketWorker);
					}
					else{
						log_error(loggerWorker,"Error al conectarse al worker. IP: %s - PUERTO: %d \n",ipWorker,puertoWorker);
						free(ipWorker);
						sendDeNotificacion(socketAceptado,ERROR_REDUCCION_GLOBAL);
						free(IP_FILESYSTEM);
						free(RUTA_DATABIN);
						free(NOMBRE_NODO);
						munmap(dataBinBloque,dataBinTamanio);
						free(numerosParalelos);
						log_info(loggerWorker, "¡¡Adios logger!! \n");
						log_destroy(loggerWorker);
						exit(-1);
					}
				}

				log_debug(loggerWorker, "Todos los datos fueron recibidos de master para realizar la reduccion global");

				char* archivoApareado = realizarApareoGlobal(listaSocketsApareo,temporalEncargado,ERROR_REDUCCION_GLOBAL,socketAceptado);

				char* nombreScriptRemasterizado = obtenerNombreScriptReductor(nombreScript,archivoApareado);

				guardarScript(script,nombreScriptRemasterizado,ERROR_REDUCCION_GLOBAL,socketAceptado);

				darPermisosAScripts(nombreScriptRemasterizado,ERROR_REDUCCION_GLOBAL,socketAceptado);

				char* command = crearComandoScriptReductor(archivoApareado,nombreScriptRemasterizado,pathDestino);

				ejecutarPrograma(command,socketAceptado,ERROR_REDUCCION_GLOBAL,REDUCCION_GLOBAL_TERMINADA,nombreScriptRemasterizado,archivoApareado);

				eliminarArchivo(nombreScriptRemasterizado);

				eliminarArchivo(archivoApareado);

				break;
			}
			case ALMACENADO_FINAL:{
				char* nombreArchivoReduccionGlobal = recibirString(socketAceptado);
				char* nombreResultante = recibirString(socketAceptado);
				char* rutaResultante = recibirString(socketAceptado);
				log_info(loggerWorker, "Todos los datos fueron recibidos de master para realizar el almacenado final");

				int socketFS = conectarWorker(IP_FILESYSTEM, PUERTO_FILESYSTEM);
				if(socketFS!=-1){
					realizarHandshakeFS(socketFS,socketAceptado);
					enviarDatosAFS(socketFS,nombreArchivoReduccionGlobal,nombreResultante,rutaResultante);

					int notificacion = recvDeNotificacion(socketFS);

					if(notificacion==ALMACENADO_FINAL_TERMINADO){
						log_debug(loggerWorker,"Se almaceno correctamente en el filesystem. \n");
						sendDeNotificacion(socketAceptado,ALMACENADO_FINAL_TERMINADO);
					} else{
						log_error(loggerWorker, "Hubo un error en el FileSystem al almacenar el archivo.\n");
						sendDeNotificacion(socketAceptado,ERROR_ALMACENADO_FINAL);
					}

					close(socketFS);
				}
				else{
					log_error(loggerWorker, "No se pudo conectar con el FileSystem.\n");
					sendDeNotificacion(socketAceptado,ERROR_ALMACENADO_FINAL);
					close(socketAceptado);
					munmap(dataBinBloque,dataBinTamanio);
					free(numerosParalelos);
					free(IP_FILESYSTEM);
					free(RUTA_DATABIN);
					free(NOMBRE_NODO);
					log_info(loggerWorker, "¡¡Adios logger!! \n");
					log_destroy(loggerWorker);
					exit(-1);
				}

				break;
			}
			default:{
				log_error(loggerWorker, "Error al recibir el tipo de etapa de Master\n");
				munmap(dataBinBloque,dataBinTamanio);
				free(numerosParalelos);
				close(socketAceptado);
				free(IP_FILESYSTEM);
				free(RUTA_DATABIN);
				free(NOMBRE_NODO);
				log_info(loggerWorker, "¡¡Adios logger!! \n");
				log_destroy(loggerWorker);
				exit(-1);
			}
			}

			break;
		}
		case ES_WORKER:{
			log_info(loggerWorker, "Se recibio una conexion de otro worker.\n");
			char* nombreArchivoTemporal = recibirString(socketAceptado);
			char* contenidoTemporal = obtenerContenido(nombreArchivoTemporal);
			free(nombreArchivoTemporal);
			uint32_t tamanioTemporal = string_length(contenidoTemporal);
			void* datosAEnviar = malloc(sizeof(uint32_t)+tamanioTemporal);
			memcpy(datosAEnviar,&tamanioTemporal,sizeof(uint32_t));
			memcpy(datosAEnviar+sizeof(uint32_t),contenidoTemporal,tamanioTemporal);

			free(contenidoTemporal);

			sendRemasterizado(socketAceptado,ES_OTRO_WORKER,tamanioTemporal+sizeof(uint32_t),datosAEnviar);

			free(datosAEnviar);

			log_info(loggerWorker, "El archivo temporal reducido del worker fue exitosamente enviado al worker encargado\n");

			break;
		}
		case CORTO:{
			log_debug(loggerWorker,"Corto un master.\n");
			break;
		}
		default:{
			log_error(loggerWorker, "La conexion recibida es erronea.\n");
			close(socketAceptado);
			munmap(dataBinBloque,dataBinTamanio);
			free(numerosParalelos);
			free(IP_FILESYSTEM);
			free(RUTA_DATABIN);
			free(NOMBRE_NODO);
			log_info(loggerWorker, "¡¡Adios logger!! \n");
			log_destroy(loggerWorker);
			exit(-1);
		}
		}
		close(socketAceptado);
		(*numerosParalelos)--;
		exit(0);
	}

	close(socketAceptado);

}

void laMardita(int signal){
	log_info(loggerWorker, "Se recibio la senial SIGINT, muriendo con estilo... \n");
	munmap(dataBinBloque,dataBinTamanio);
	free(IP_FILESYSTEM);
	free(RUTA_DATABIN);
	free(NOMBRE_NODO);
	free(numerosParalelos);
	log_info(loggerWorker, "¡¡Adios logger!! \n");
	log_destroy(loggerWorker);
	exit(0);
}

void generarCarpetaTemporal(){
	char* comandoReseteo=string_new();
	string_append(&comandoReseteo,"rm -r ");
	string_append(&comandoReseteo,"../../../tmp");
	system(comandoReseteo);
	log_info(loggerWorker,"Se borro correctamente la carpeta temporal anterior. \n");
	free(comandoReseteo);
	mkdir("../../../tmp",0777);
	log_info(loggerWorker,"Se creo una nueva carpeta para almacenar los temporales. \n");
}

int main(int argc, char **argv) {
	loggerWorker = log_create("Worker.log", "Worker", 1, 0);
	numerosParalelos = malloc(sizeof(int));
	*numerosParalelos = 0;
	signal(SIGINT, laMardita);
	chequearParametros(argc,2);
	t_config* configuracionWorker = generarTConfig(argv[1], 5);
	//t_config* configuracionWorker = generarTConfig("Debug/off_worker1.ini", 5);
	cargarWorker(configuracionWorker);
	log_info(loggerWorker, "Se cargo correctamente Worker.\n");
	int socketAceptado, socketEscuchaWorker;
	socketEscuchaWorker = ponerseAEscucharClientes(PUERTO_WORKER, 0);
	//eliminarProcesosMuertos();
	//log_debug(loggerWorker, "Se empezo a ejecutar correctamente el sigaction con el sigchild handler para eliminar procesos zombies del sitema.\n");
	dataBinBloque = dataBinMapear();
	contadorRandom = 0;
	generarCarpetaTemporal();
	while(1){
		socketAceptado = aceptarConexionDeCliente(socketEscuchaWorker);
		log_info(loggerWorker, "Se ha recibido una nueva conexion.\n");
		while((*numerosParalelos)>1)
		{

		}
		crearProcesoHijo(socketAceptado,socketEscuchaWorker);
	}
	close(socketEscuchaWorker);
	return EXIT_SUCCESS;
}
