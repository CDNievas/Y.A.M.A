#include "../../Biblioteca/src/Socket.c"
#include "../../Biblioteca/src/configParser.c"

#define TRANSFORMACION 1
#define TRANSFORMACION_TERMINADA 2
#define REPLANIFICAR 3
#define REDUCCION_LOCAL_TERMINADA 4
#define REDUCCION_GLOBAL_TERMINADA 5
#define ERROR_REDUCCION_LOCAL 6
#define ERROR_REDUCCION_GLOBAL 7
#define REDUCCION_LOCAL 8
#define REDUCCION_GLOBAL 9
#define ABORTAR 10
#define EN_PROCESO 11
#define DATOS_NODO 12 //CON ESTO LE PIDO A FS LOS DATOS DE CONEXION DEL NODO
#define INFO_ARCHIVO_FS 13 //CON ESTO LE PIDO A FS LA INFO DEL ARCHIVO
#define FINALIZADO 14
#define ALMACENAMIENTO_FINAL 15
#define CORTO 0
#define ALMACENADO_FINAL_TERMINADO 16
#define ERROR_ALMACENADO_FINAL 17
#define ERROR_TRANSFORMACION 18

char* YAMA_IP;
char* WORKER_IP;
int YAMA_PUERTO;
int WORKER_PUERTO;
t_log* loggerMaster;
t_list* listaInfoNodos;
t_list* listaHilosTransformacion;
uint32_t cantidadNodos;
pthread_mutex_t mutexNodos;

typedef struct{
	char* nombreNodo;
	char* ipNodo;
	uint32_t puertoNodo;
}conexionNodo;

typedef struct{
	char* scriptTransformacion;
	int socketYAMA;
}informacionGeneral;

typedef struct{
	conexionNodo conexion;
	uint32_t nroBloque;
	uint32_t bytesOcupados;
	char* nombreTemporal;
	informacionGeneral infoGeneral;
}datosTransformacion;

typedef struct{
	char* nombreNodo;
	uint32_t numeroBloque;
	pthread_t hiloManejadorNodo;
}datosHilo;

void cargarMaster(t_config* configuracionMaster){
    if(!config_has_property(configuracionMaster, "YAMA_IP")){
        log_error(loggerMaster, "No se encuentra YAMA_IP.\n");
        exit(-1);
    }else{
        YAMA_IP = string_new();
        string_append(&YAMA_IP, config_get_string_value(configuracionMaster, "YAMA_IP"));
    }
    if(!config_has_property(configuracionMaster, "YAMA_PUERTO")){
        log_error(loggerMaster, "No se encuentra YAMA_PUERTO en el archivo de configuracion.\n");
        exit(-1);
    }else{
        YAMA_PUERTO = config_get_int_value(configuracionMaster, "YAMA_PUERTO");
    }
    config_destroy(configuracionMaster);
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
	int retornoSeek = fseek(unArchivo, 0, SEEK_END);

	if(retornoSeek==0){
		log_error(loggerMaster,"Error de fseek.\n");
		exit(-1);
	}

	long int tamanioArchivo = ftell(unArchivo);

	if(tamanioArchivo==-1){
		log_error(loggerMaster,"Error de ftell.\n");
		exit(-1);
	}

	return tamanioArchivo;
}

char* leerArchivo(FILE* unArchivo, long int tamanioArchivo)
{
	int retornoSeek = fseek(unArchivo, 0, SEEK_SET);

	if(retornoSeek==0){
		log_error(loggerMaster,"Error de fseek.\n");
		exit(-1);
	}

	char* contenidoArchivo = malloc(tamanioArchivo+1);

	if(contenidoArchivo==NULL){
		log_error(loggerMaster,"Error al asignar memoria para leer el archivo.\n");
		exit(-10);
	}

	fread(contenidoArchivo, tamanioArchivo, 1, unArchivo);
	contenidoArchivo[tamanioArchivo] = '\0';
	return contenidoArchivo;
}

char* obtenerContenido(char* unPath){
	FILE* archivoALeer = fopen(unPath, "rb");

	if(archivoALeer==NULL){
		log_error(loggerMaster,"No se pudo abrir el archivo: %s.\n",unPath);
		exit(-1);
	}

	log_info(loggerMaster,"Se pudo abrir el archivo: %s.\n",unPath);

	long int tamanioArchivo = obtenerTamanioArchivo(archivoALeer);
	log_info(loggerMaster, "Se pudo obtener el tamanio del archivo: %s.\n",unPath);
	char* contenidoArchivo = leerArchivo(archivoALeer,tamanioArchivo);
	log_info(loggerMaster, "Se pudo obtener el contenido del archivo: %s.\n",unPath);
	return contenidoArchivo;
}

void enviarArchivoAYAMA(char* unArchivo,int socketYAMA){
	uint32_t tamanioArchivo = string_length(unArchivo);
	void* datosAEnviar = malloc(tamanioArchivo+sizeof(uint32_t));

	if(datosAEnviar==NULL){
		log_error(loggerMaster,"Error al asignar memoria para enviar archivo a YAMA.\n");
		exit(-10);
	}

	memcpy(datosAEnviar,&tamanioArchivo,sizeof(uint32_t));
	memcpy(datosAEnviar+sizeof(uint32_t),unArchivo,tamanioArchivo);

	log_info(loggerMaster, "Datos serializados para ser enviados a YAMA.\n");

	sendRemasterizado(socketYAMA,TRANSFORMACION,tamanioArchivo+sizeof(uint32_t),datosAEnviar);

	free(datosAEnviar);
}

void recibirSolicitudTransformacion(int socketYAMA){
    uint32_t cantidadDeNodos = recibirUInt(socketYAMA);
    uint32_t i;
    for(i=0;i<cantidadDeNodos; i++){
    	datosTransformacion* nodoActual = (datosTransformacion*) malloc(sizeof(datosTransformacion));
        nodoActual->conexion.nombreNodo = recibirString(socketYAMA);
        nodoActual->conexion.ipNodo = recibirString(socketYAMA);
        nodoActual->conexion.puertoNodo = recibirUInt(socketYAMA);
        nodoActual->nroBloque = recibirUInt(socketYAMA);
        nodoActual->bytesOcupados = recibirUInt(socketYAMA);
        nodoActual->nombreTemporal = recibirString(socketYAMA);

        pthread_mutex_lock(&mutexNodos);
        list_add(listaInfoNodos,nodoActual);
        pthread_mutex_unlock(&mutexNodos);
    }
}

int recvDeNotificacionMaster(int socketMaster){
	int notificacion;
	int resultadoRecv = recv(socketMaster, &notificacion, sizeof(int), 0);
	if(resultadoRecv ==-1){
		log_info(loggerMaster,"Error al recibir la notificacion de Worker. Este ha cortado.");
		notificacion = -1;
	}else if(resultadoRecv == 0){
		notificacion = 0;
	}
	return notificacion;
}

void eliminarHiloListaTransformacion(char* nombreNodo,uint32_t nroBloque){
	uint32_t posicion;
	for(posicion=0;posicion<list_size(listaHilosTransformacion);posicion++){
		datosHilo* unDatoHilo = list_get(listaHilosTransformacion,posicion);
		if((strcmp(unDatoHilo->nombreNodo,nombreNodo)==0) && (nroBloque==unDatoHilo->numeroBloque)){
			list_remove(listaHilosTransformacion,posicion);
			free(unDatoHilo->nombreNodo);
			free(unDatoHilo);
			break;
		}
	}
}

void eliminarHilos(char* nombreNodo,uint32_t nroBloque){
	uint32_t posicion;
	for(posicion=0;posicion<list_size(listaHilosTransformacion);posicion++){
		datosHilo* unDatoHilo = list_get(listaHilosTransformacion,posicion);
		if((strcmp(unDatoHilo->nombreNodo,nombreNodo)==0) && (nroBloque!=unDatoHilo->numeroBloque)){
			if(pthread_cancel(unDatoHilo->hiloManejadorNodo) == 0)
			{
				log_info(loggerMaster,"Se finalizo correctamente el hilo del nodo: %s y numero de bloque: %d \n",nombreNodo,nroBloque);
				pthread_join(unDatoHilo->hiloManejadorNodo, (void**) NULL);
				list_remove(listaHilosTransformacion,posicion);
				free(unDatoHilo->nombreNodo);
				free(unDatoHilo);
				posicion--;
			}else{
				log_error(loggerMaster,"No se pudo matar el hilo del nodo: %s y numero de bloque: %d \n",nombreNodo,nroBloque);
			}
		}
	}
}

void eliminarDatosTransformacion(char* nombreNodo){
	uint32_t posicion;
	for(posicion=0;posicion<list_size(listaInfoNodos);posicion++){
		datosTransformacion* unDatoTransformacion = list_get(listaInfoNodos,posicion);
		if(strcmp(unDatoTransformacion->conexion.nombreNodo,nombreNodo)==0){
			list_remove(listaInfoNodos,posicion);
			free(unDatoTransformacion->conexion.ipNodo);
			free(unDatoTransformacion->conexion.nombreNodo);
			free(unDatoTransformacion->nombreTemporal);
			free(unDatoTransformacion->infoGeneral.scriptTransformacion);
			free(unDatoTransformacion);
			posicion--;
			cantidadNodos--;
		}
	}
}

void manejadorTransformacionWorker(void* unDatoTransformacion){
	datosTransformacion* datoNodoTransformacion = (datosTransformacion*)unDatoTransformacion;

	int socketWorker = conectarAServer(datoNodoTransformacion->conexion.ipNodo, datoNodoTransformacion->conexion.puertoNodo);
	log_info(loggerMaster,"Se ha conectado con WORKER. IP: %s - PUERTO: %d \n",datoNodoTransformacion->conexion.ipNodo, datoNodoTransformacion->conexion.puertoNodo);
	realizarHandshake(socketWorker,ES_WORKER);
	log_info(loggerMaster,"Handshake con Worker realizado exitosamente.\n");

	char* codigoScript = obtenerContenido(datoNodoTransformacion->infoGeneral.scriptTransformacion);
	uint32_t tamanioScript = string_length(codigoScript);
	uint32_t tamanioNombreScript = string_length(datoNodoTransformacion->infoGeneral.scriptTransformacion);
	uint32_t tamanioPathDestino = string_length(datoNodoTransformacion->nombreTemporal);
	void* datosAEnviar = malloc(tamanioScript+tamanioNombreScript+tamanioPathDestino+(sizeof(uint32_t)*5));

	memcpy(datosAEnviar,&(datoNodoTransformacion->nroBloque),sizeof(uint32_t));
	memcpy(datosAEnviar+sizeof(uint32_t),&(datoNodoTransformacion->bytesOcupados),sizeof(uint32_t));
	memcpy(datosAEnviar+(sizeof(uint32_t)*2),&tamanioScript,sizeof(uint32_t));
	memcpy(datosAEnviar+(sizeof(uint32_t)*3),codigoScript,tamanioScript);
	memcpy(datosAEnviar+(sizeof(uint32_t)*3)+tamanioScript,&tamanioNombreScript,sizeof(uint32_t));
	memcpy(datosAEnviar+(sizeof(uint32_t)*4)+tamanioScript,datoNodoTransformacion->infoGeneral.scriptTransformacion,tamanioNombreScript);
	memcpy(datosAEnviar+(sizeof(uint32_t)*4)+tamanioScript+tamanioNombreScript,&tamanioPathDestino,sizeof(uint32_t));
	memcpy(datosAEnviar+(sizeof(uint32_t)*5)+tamanioScript+tamanioNombreScript,datoNodoTransformacion->nombreTemporal,tamanioPathDestino);

	log_info(loggerMaster, "Datos serializados para ser enviados a Worker.\n");

	sendRemasterizado(socketWorker,TRANSFORMACION,tamanioScript+tamanioNombreScript+tamanioPathDestino+(sizeof(uint32_t)*5),datosAEnviar);

	free(datosAEnviar);
	free(codigoScript);

	int resultadoTransformacion = recvDeNotificacionMaster(socketWorker);

	uint32_t tamanioNombreNodo = string_length(datoNodoTransformacion->conexion.nombreNodo);

	if(resultadoTransformacion==TRANSFORMACION_TERMINADA){
		void* datosAEnviarAYAMA = malloc(tamanioNombreNodo+(sizeof(uint32_t)*2));

		memcpy(datosAEnviarAYAMA,&tamanioNombreNodo,sizeof(uint32_t));
		memcpy(datosAEnviarAYAMA+sizeof(uint32_t),datoNodoTransformacion->conexion.nombreNodo,tamanioNombreNodo);
		memcpy(datosAEnviarAYAMA+sizeof(uint32_t)+tamanioNombreNodo,&(datoNodoTransformacion->nroBloque),sizeof(uint32_t));

		log_info(loggerMaster, "Datos de transformacion terminada para ser enviados a YAMA serializados con exito.\n");

		sendRemasterizado(datoNodoTransformacion->infoGeneral.socketYAMA,TRANSFORMACION_TERMINADA,tamanioNombreNodo+(sizeof(uint32_t)*2),datosAEnviarAYAMA);
		free(datosAEnviarAYAMA);
		free(datoNodoTransformacion->conexion.ipNodo);
		free(datoNodoTransformacion->nombreTemporal);
		free(datoNodoTransformacion->infoGeneral.scriptTransformacion);
		close(socketWorker);

		pthread_mutex_lock(&mutexNodos);
		eliminarHiloListaTransformacion(datoNodoTransformacion->conexion.nombreNodo,datoNodoTransformacion->nroBloque);
		pthread_mutex_unlock(&mutexNodos);

		free(datoNodoTransformacion->conexion.nombreNodo);
		free(datoNodoTransformacion);
		pthread_detach(pthread_self());
	}
	else{
		pthread_mutex_lock(&mutexNodos);
		eliminarDatosTransformacion(datoNodoTransformacion->conexion.nombreNodo);
		eliminarHilos(datoNodoTransformacion->conexion.nombreNodo,datoNodoTransformacion->nroBloque);
		pthread_mutex_unlock(&mutexNodos);

		void* datosAEnviarAYAMA = malloc(tamanioNombreNodo+sizeof(uint32_t));

		memcpy(datosAEnviarAYAMA,&tamanioNombreNodo,sizeof(uint32_t));
		memcpy(datosAEnviarAYAMA+sizeof(uint32_t),datoNodoTransformacion->conexion.nombreNodo,tamanioNombreNodo);

		log_error(loggerMaster, "Datos de replanificacion para ser enviados a YAMA serializados con exito.\n");

		sendRemasterizado(datoNodoTransformacion->infoGeneral.socketYAMA,REPLANIFICAR,tamanioNombreNodo+sizeof(uint32_t),datosAEnviarAYAMA);
		free(datosAEnviarAYAMA);
		free(datoNodoTransformacion->conexion.ipNodo);
		free(datoNodoTransformacion->nombreTemporal);
		free(datoNodoTransformacion->infoGeneral.scriptTransformacion);
		close(socketWorker);

		pthread_mutex_lock(&mutexNodos);
		eliminarHiloListaTransformacion(datoNodoTransformacion->conexion.nombreNodo,datoNodoTransformacion->nroBloque);
		pthread_mutex_unlock(&mutexNodos);

		free(datoNodoTransformacion->conexion.nombreNodo);
		free(datoNodoTransformacion);
		pthread_cancel(pthread_self());
	}
}

void inicializarTransformacionEnNodos(char* scriptTransformador,int socketYAMA){
	uint32_t i;
	pthread_mutex_lock(&mutexNodos);
	cantidadNodos = list_size(listaInfoNodos);
	for(i=0;i<cantidadNodos;i++){
		datosTransformacion* unDatoTransformacion = list_remove(listaInfoNodos,0);
		unDatoTransformacion->infoGeneral.scriptTransformacion = string_new();
		datosHilo* unDatoHilo = (datosHilo*) malloc(sizeof(datosHilo));
		unDatoHilo->nombreNodo = string_new();
		unDatoTransformacion->infoGeneral.socketYAMA = socketYAMA;
		unDatoHilo->numeroBloque = unDatoTransformacion->nroBloque;
		string_append(&(unDatoTransformacion->infoGeneral.scriptTransformacion ),scriptTransformador);
		string_append(&(unDatoHilo->nombreNodo),unDatoTransformacion->conexion.nombreNodo);
		pthread_t hiloManejadorWorker;
		unDatoHilo->hiloManejadorNodo = hiloManejadorWorker;
		list_add(listaHilosTransformacion,unDatoHilo);
		pthread_attr_t attr;
		pthread_attr_init(&attr);
		pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
		pthread_create(&hiloManejadorWorker, &attr, (void*)manejadorTransformacionWorker, (void*)unDatoTransformacion);
	}
	pthread_mutex_unlock(&mutexNodos);
}

void finalizarHilos(){
	uint32_t posicionTransformacion;
	pthread_mutex_lock(&mutexNodos);
	uint32_t cantidadHilosTransformacion = list_size(listaHilosTransformacion);
	for(posicionTransformacion=0;posicionTransformacion<cantidadHilosTransformacion;posicionTransformacion++){
		datosHilo* unDatoHilo = list_remove(listaHilosTransformacion,0);
		if(pthread_cancel(unDatoHilo->hiloManejadorNodo) == 0)
		{
			log_info(loggerMaster,"Se finalizo correctamente el hilo del nodo: %s y numero de bloque: %d \n",unDatoHilo->nombreNodo,unDatoHilo->numeroBloque);
			pthread_join(unDatoHilo->hiloManejadorNodo, (void**) NULL);
			free(unDatoHilo->nombreNodo);
			free(unDatoHilo);
		}else{
			log_error(loggerMaster,"No se pudo matar el hilo del nodo: %s y numero de bloque: %d \n",unDatoHilo->nombreNodo,unDatoHilo->numeroBloque);
		}
	}
	list_destroy(listaHilosTransformacion);
	pthread_mutex_unlock(&mutexNodos);
}

void liberarListas(){
	uint32_t posicionInfoNodos;
	pthread_mutex_lock(&mutexNodos);
	uint32_t cantidadInfoNodos = list_size(listaInfoNodos);
	for(posicionInfoNodos=0;posicionInfoNodos<cantidadInfoNodos;posicionInfoNodos++){
		datosTransformacion* unDatoTransformacion = list_remove(listaInfoNodos,0);
		free(unDatoTransformacion->conexion.ipNodo);
		free(unDatoTransformacion->conexion.nombreNodo);
		free(unDatoTransformacion->nombreTemporal);
		free(unDatoTransformacion->infoGeneral.scriptTransformacion);
		free(unDatoTransformacion);
	}
	list_destroy(listaInfoNodos);
	pthread_mutex_unlock(&mutexNodos);
}

int main(int argc, char **argv) {
	loggerMaster = log_create("Master.log", "Master", 1, 0);
	chequearParametros(argc,6);
	t_config* configuracionMaster = generarTConfig(argv[1], 2);
	cargarMaster(configuracionMaster);
	log_info(loggerMaster,"Se cargo exitosamente Master.\n");
    int socketYAMA = conectarAServer(YAMA_IP, YAMA_PUERTO);
    log_info(loggerMaster,"Se ha conectado con YAMA. IP: %s - PUERTO: %d \n",YAMA_IP,YAMA_PUERTO);
    realizarHandshake(socketYAMA,ES_YAMA);
    log_info(loggerMaster,"Handshake con YAMA realizado exitosamente.\n");
    enviarArchivoAYAMA(argv[4],socketYAMA);
    log_info(loggerMaster,"Envio de archivo realizado con exito.\n");
    listaInfoNodos = list_create();
    listaHilosTransformacion = list_create();
    pthread_mutex_init(&mutexNodos,NULL);
    while(1){
    	int operacion = recvDeNotificacion(socketYAMA);
    	switch(operacion){
    	case TRANSFORMACION:{
    		recibirSolicitudTransformacion(socketYAMA);
    		inicializarTransformacionEnNodos(argv[2],socketYAMA);
    		break;
    	}
    	case REDUCCION_LOCAL:{
    		break;
    	}
    	case REDUCCION_GLOBAL:{
    		break;
    	}
    	case ALMACENAMIENTO_FINAL:{
    		break;
    	}
    	case ABORTAR:{
    		log_error(loggerMaster,"Se abortara el job.\n");
    		finalizarHilos();
    		liberarListas();
    		free(YAMA_IP);
    		free(WORKER_IP);
    		close(socketYAMA);
    		exit(-1);
    		break;
    	}
    	default:
    		log_error(loggerMaster,"La conexion recibida es erronea.\n");
    		close(socketYAMA);
    	}
    }
	return EXIT_SUCCESS;
}
