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
#define DATOS_NODO 12
#define INFO_ARCHIVO_FS 13
#define FINALIZADO 14
#define ALMACENADO_FINAL 15
#define CORTO 0
#define ALMACENADO_FINAL_TERMINADO 17
#define ERROR_ALMACENADO_FINAL 16
#define NO_REDU_LOCAL 19

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

typedef struct{
	conexionNodo conexion;
	char* temporalReduccionLocal;
	informacionGeneral infoGeneral;
	t_list* archivosTemporales;
	bool noHayHiloCreado;
}infoReduccionLocal;

typedef struct{
	pthread_t hiloManejadorReduccion;
}identificadorHilo;

typedef struct{
	conexionNodo conexion;
	char* temporalReduccion;
}infoReduccionGlobal;

typedef struct{
	int hora;
	int minuto;
	int segundo;
}tiempo;

char* YAMA_IP;
int YAMA_PUERTO;
t_log* loggerMaster;
t_list* listaNodosCaidos;
t_list* listaTemporales;
t_list* listaHilosTransformacion;
t_list* listaHilosReduccion;
double transformacionesRealizadas;
double reduccionesLocalesRealizadas;
double cantidadFallos;
double paralelismoMaximoTransformaciones;
double paralelismoMaximoReducciones;
double paralelismoTempMaximoTransformaciones;
double paralelismoTempMaximoReducciones;
bool avisoCargaTemporal;
pthread_mutex_t mutexNodos;
pthread_mutex_t mutexReducciones;
pthread_mutex_t mutexTemporales;
tiempo tiempoI;
tiempo tiempoTransformacion;
tiempo tiempoReduccionLocal;
tiempo tiempoReduccionGlobal;

tiempo obtenerTiempo()
{
	char* unTiempo = temporal_get_string_time();
	tiempo t;
	int i;

	char** tiempov = string_split(unTiempo, ":");

	t.hora = atoi(tiempov[0]);
	t.minuto = atoi(tiempov[1]);
	t.segundo = atoi(tiempov[2]);

	for (i=0; i<=3; i++) {
	    free(tiempov[i]);
	}

	free(tiempov);
	free(unTiempo);

	return t;
}

tiempo get_tiempo_total(tiempo in, tiempo fin)
{
	tiempo aux;
	aux.hora = fin.hora - in.hora;
	aux.minuto = fin.minuto - in.minuto;
	aux.segundo = fin.segundo - in.segundo;
	aux.segundo=fabs(aux.segundo);

	return aux;
}

void sumarTiempos(int valor,tiempo otroTiempo){
	if(valor==0){
		tiempoTransformacion.hora += otroTiempo.hora;
		tiempoTransformacion.minuto += otroTiempo.minuto;
		tiempoTransformacion.segundo += otroTiempo.segundo;

		if((tiempoTransformacion.segundo)>=60){
			tiempoTransformacion.minuto++;
			tiempoTransformacion.segundo -=60;
		}
	}
	else{
		tiempoReduccionLocal.hora += otroTiempo.hora;
		tiempoReduccionLocal.minuto += otroTiempo.minuto;
		tiempoReduccionLocal.segundo += otroTiempo.segundo;

		if((tiempoReduccionLocal.segundo)>=60){
			tiempoReduccionLocal.minuto++;
			tiempoReduccionLocal.segundo -=60;
		}
	}
}

tiempo dividirTiempo(tiempo unTiempo,double factorDivision){
	unTiempo.hora = (unTiempo.hora)/factorDivision;
	unTiempo.minuto = ceil(((double)unTiempo.minuto)/factorDivision);
	unTiempo.segundo = ceil(((double)unTiempo.segundo)/factorDivision);
	if((unTiempo.segundo)>=60){
		unTiempo.minuto++;
		unTiempo.segundo -=60;
	}
	return unTiempo;
}

void finalizarHilos(){
	uint32_t posicionTransformacion,posicionReduccion;
	pthread_mutex_lock(&mutexNodos);
	uint32_t cantidadHilosTransformacion = list_size(listaHilosTransformacion);
	for(posicionTransformacion=0;posicionTransformacion<cantidadHilosTransformacion;posicionTransformacion++){
		datosHilo* unDatoHilo = list_remove(listaHilosTransformacion,0);
		if(pthread_cancel(unDatoHilo->hiloManejadorNodo) == 0)
		{
			log_info(loggerMaster,"Se finalizo correctamente el hilo del nodo: %s y numero de bloque: %d \n",unDatoHilo->nombreNodo,unDatoHilo->numeroBloque);
		}else{
			log_error(loggerMaster,"No se pudo matar el hilo del nodo: %s y numero de bloque: %d \n",unDatoHilo->nombreNodo,unDatoHilo->numeroBloque);
		}
		free(unDatoHilo->nombreNodo);
		free(unDatoHilo);
	}
	list_destroy(listaHilosTransformacion);
	pthread_mutex_unlock(&mutexNodos);
	pthread_mutex_lock(&mutexReducciones);
	uint32_t cantidadHilosReduccion = list_size(listaHilosReduccion);
	for(posicionReduccion=0;posicionReduccion<cantidadHilosReduccion;posicionReduccion++){
		identificadorHilo* unIdentificadorHilo = list_remove(listaHilosReduccion,0);
		if(pthread_cancel(unIdentificadorHilo->hiloManejadorReduccion) == 0)
		{
			log_info(loggerMaster,"Se finalizo correctamente un hilo de reduccion local.\n");
		}else{
			log_error(loggerMaster,"No se pudo matar el hilo de reduccion local.\n");
		}
		free(unIdentificadorHilo);
	}
	list_destroy(listaHilosReduccion);
	pthread_mutex_unlock(&mutexReducciones);
}

void liberarListas(){
	uint32_t posicionTemporales;
	pthread_mutex_lock(&mutexNodos);
	list_destroy_and_destroy_elements(listaNodosCaidos,free);
	pthread_mutex_unlock(&mutexNodos);
	pthread_mutex_lock(&mutexTemporales);
	uint32_t cantidadInfoTemporales = list_size(listaTemporales);
	for(posicionTemporales=0;posicionTemporales<cantidadInfoTemporales;posicionTemporales++){
		infoReduccionLocal* unaInfoTemporalNodo = list_remove(listaTemporales,0);
		list_destroy_and_destroy_elements(unaInfoTemporalNodo->archivosTemporales, free);
		free(unaInfoTemporalNodo->temporalReduccionLocal);
		free(unaInfoTemporalNodo->conexion.ipNodo);
		free(unaInfoTemporalNodo->conexion.nombreNodo);
		free(unaInfoTemporalNodo->infoGeneral.scriptTransformacion);
		free(unaInfoTemporalNodo);
	}
	list_destroy(listaTemporales);
	pthread_mutex_unlock(&mutexTemporales);
}

void mostrarMetricas(){
	tiempo tiempoF = obtenerTiempo();
	tiempo tiempoDuracion = get_tiempo_total(tiempoI,tiempoF);
	log_debug(loggerMaster,"El tiempo total de ejecucion del job fue %i:%i:%i \n", tiempoDuracion.hora, tiempoDuracion.minuto,tiempoDuracion.segundo);
	log_debug(loggerMaster,"La cantidad de fallos obtenidos en la realizacion del job fue de: %f \n", cantidadFallos);
	log_debug(loggerMaster,"La cantidad total de tareas de transformacion realizadas durante el job fue de: %f \n", transformacionesRealizadas);
	log_debug(loggerMaster,"La cantidad total de tareas de reduccion local realizadas durante el job fue de: %f \n", reduccionesLocalesRealizadas);
	log_debug(loggerMaster,"La cantidad maxima de tareas de transformacion ejecutadas en paralelo fue de: %f \n", paralelismoMaximoTransformaciones);
	log_debug(loggerMaster,"La cantidad maxima de tareas de reduccion ejecutadas en paralelo fue de: %f \n", paralelismoMaximoReducciones);

	if(transformacionesRealizadas!=0){
		tiempo tiempoTransformacionPromedio = dividirTiempo(tiempoTransformacion,transformacionesRealizadas);
		log_debug(loggerMaster,"El tiempo promedio de ejecucion de la etapa transformacion fue %i:%i:%i \n",tiempoTransformacionPromedio.hora, tiempoTransformacionPromedio.minuto,tiempoTransformacionPromedio.segundo);
	}

	if(reduccionesLocalesRealizadas!=0){
		tiempo tiempoReduccionLocalPromedio = dividirTiempo(tiempoReduccionLocal,reduccionesLocalesRealizadas);
		log_debug(loggerMaster,"El tiempo promedio de ejecucion de la etapa reduccion local fue %i:%i:%i \n",tiempoReduccionLocalPromedio.hora, tiempoReduccionLocalPromedio.minuto,tiempoReduccionLocalPromedio.segundo);
	}

	log_debug(loggerMaster,"El tiempo de ejecucion de la etapa reduccion global fue %i:%i:%i \n",tiempoReduccionGlobal.hora, tiempoReduccionGlobal.minuto,tiempoReduccionGlobal.segundo);
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

void calcularMaximoParalelos(){
	if(paralelismoTempMaximoTransformaciones>paralelismoMaximoTransformaciones){
		paralelismoMaximoTransformaciones = paralelismoTempMaximoTransformaciones;
	}

	if(paralelismoTempMaximoReducciones>paralelismoMaximoReducciones){
		paralelismoMaximoReducciones = paralelismoTempMaximoReducciones;
	}
}

void cargarMaster(t_config* configuracionMaster){
    if(!config_has_property(configuracionMaster, "YAMA_IP")){
        log_error(loggerMaster, "No se encuentra YAMA_IP.\n");
        cantidadFallos++;
        mostrarMetricas();
        finalizarHilos();
        liberarListas();
        pthread_mutex_destroy(&mutexNodos);
        pthread_mutex_destroy(&mutexReducciones);
        pthread_mutex_destroy(&mutexTemporales);
        log_destroy(loggerMaster);
        exit(-1);
    }else{
        YAMA_IP = string_new();
        string_append(&YAMA_IP, config_get_string_value(configuracionMaster, "YAMA_IP"));
    }
    if(!config_has_property(configuracionMaster, "YAMA_PUERTO")){
        log_error(loggerMaster, "No se encuentra YAMA_PUERTO en el archivo de configuracion.\n");
        cantidadFallos++;
        mostrarMetricas();
        finalizarHilos();
        liberarListas();
        free(YAMA_IP);
        pthread_mutex_destroy(&mutexNodos);
        pthread_mutex_destroy(&mutexReducciones);
        pthread_mutex_destroy(&mutexTemporales);
        log_destroy(loggerMaster);
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
        cantidadFallos++;
        close(unSocket);
        mostrarMetricas();
        finalizarHilos();
        liberarListas();
        free(YAMA_IP);
        pthread_mutex_destroy(&mutexNodos);
        pthread_mutex_destroy(&mutexReducciones);
        pthread_mutex_destroy(&mutexTemporales);
        log_destroy(loggerMaster);
        exit(-1);
    }
}

int realizarHandshakeWorker(int unSocket, int proceso){
    sendDeNotificacion(unSocket, ES_MASTER);
    int notificacion = recvDeNotificacion(unSocket);
    if(notificacion != proceso){
        log_error(loggerMaster, "La conexion establecida no es correcta");
        notificacion = -1;
    }
    return notificacion;
}

long int obtenerTamanioArchivo(FILE* unArchivo){
	int retornoSeek = fseek(unArchivo, 0, SEEK_END);

	if(retornoSeek!=0){
		log_error(loggerMaster,"Error de fseek.\n");
		cantidadFallos++;
		mostrarMetricas();
		finalizarHilos();
		liberarListas();
		free(YAMA_IP);
		pthread_mutex_destroy(&mutexNodos);
		pthread_mutex_destroy(&mutexReducciones);
		pthread_mutex_destroy(&mutexTemporales);
		log_destroy(loggerMaster);
		exit(-1);
	}

	long int tamanioArchivo = ftell(unArchivo);

	if(tamanioArchivo==-1){
		log_error(loggerMaster,"Error de ftell.\n");
		cantidadFallos++;
		mostrarMetricas();
		finalizarHilos();
		liberarListas();
		free(YAMA_IP);
		pthread_mutex_destroy(&mutexNodos);
		pthread_mutex_destroy(&mutexReducciones);
		pthread_mutex_destroy(&mutexTemporales);
		log_destroy(loggerMaster);
		exit(-1);
	}

	return tamanioArchivo;
}

char* leerArchivo(FILE* unArchivo, long int tamanioArchivo)
{
	int retornoSeek = fseek(unArchivo, 0, SEEK_SET);

	if(retornoSeek!=0){
		log_error(loggerMaster,"Error de fseek.\n");
		cantidadFallos++;
		mostrarMetricas();
		finalizarHilos();
		liberarListas();
		free(YAMA_IP);
		pthread_mutex_destroy(&mutexNodos);
		pthread_mutex_destroy(&mutexReducciones);
		pthread_mutex_destroy(&mutexTemporales);
		log_destroy(loggerMaster);
		exit(-1);
	}

	char* contenidoArchivo = malloc(tamanioArchivo+1);

	if(contenidoArchivo==NULL){
		log_error(loggerMaster,"Error al asignar memoria para leer el archivo.\n");
		cantidadFallos++;
		mostrarMetricas();
		finalizarHilos();
		liberarListas();
		free(YAMA_IP);
		pthread_mutex_destroy(&mutexNodos);
		pthread_mutex_destroy(&mutexReducciones);
		pthread_mutex_destroy(&mutexTemporales);
		log_destroy(loggerMaster);
		exit(-1);
	}

	fread(contenidoArchivo, tamanioArchivo, 1, unArchivo);
	contenidoArchivo[tamanioArchivo] = '\0';
	return contenidoArchivo;
}

char* obtenerContenido(char* unPath){
	FILE* archivoALeer = fopen(unPath, "rb");

	if(archivoALeer==NULL){
		log_error(loggerMaster,"No se pudo abrir el archivo: %s.\n",unPath);
		cantidadFallos++;
		mostrarMetricas();
		finalizarHilos();
		liberarListas();
		free(YAMA_IP);
		pthread_mutex_destroy(&mutexNodos);
		pthread_mutex_destroy(&mutexReducciones);
		pthread_mutex_destroy(&mutexTemporales);
		log_destroy(loggerMaster);
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
		cantidadFallos++;
		close(socketYAMA);
		mostrarMetricas();
		finalizarHilos();
		liberarListas();
		free(YAMA_IP);
		pthread_mutex_destroy(&mutexNodos);
		pthread_mutex_destroy(&mutexReducciones);
		pthread_mutex_destroy(&mutexTemporales);
		log_destroy(loggerMaster);
		exit(-1);
	}

	memcpy(datosAEnviar,&tamanioArchivo,sizeof(uint32_t));
	memcpy(datosAEnviar+sizeof(uint32_t),unArchivo,tamanioArchivo);

	log_info(loggerMaster, "Datos serializados para ser enviados a YAMA.\n");

	sendRemasterizado(socketYAMA,TRANSFORMACION,tamanioArchivo+sizeof(uint32_t),datosAEnviar);

	free(datosAEnviar);
}

bool verificarNodoCaido(char* nombreNodo){
	uint32_t posicion;
	for(posicion=0;posicion<list_size(listaNodosCaidos);posicion++){
		char* unNodoCaido = list_get(listaNodosCaidos,posicion);
		if(strcmp(unNodoCaido,nombreNodo)==0){
			return true;
		}
	}
	return false;
}

void eliminarHilos(char* nombreNodo,uint32_t nroBloque){
	int posicion;
	for(posicion=0;posicion<list_size(listaHilosTransformacion);posicion++){
		datosHilo* unDatoHilo = list_get(listaHilosTransformacion,posicion);
		if((strcmp(unDatoHilo->nombreNodo,nombreNodo)==0) && (nroBloque!=unDatoHilo->numeroBloque)){
			list_remove(listaHilosTransformacion,posicion);
			posicion--;
			free(unDatoHilo->nombreNodo);
			if(pthread_cancel(unDatoHilo->hiloManejadorNodo) == 0)
			{
				log_debug(loggerMaster,"Se finalizo correctamente el hilo. \n");
			}else{
				log_error(loggerMaster,"No se pudo finalizar el hilo. \n");
			}
			free(unDatoHilo);
		}
		else if((strcmp(unDatoHilo->nombreNodo,nombreNodo)==0) && (nroBloque==unDatoHilo->numeroBloque)){
			list_remove(listaHilosTransformacion,posicion);
			posicion--;
			free(unDatoHilo->nombreNodo);
			free(unDatoHilo);
		}
	}
}

void replanificarTransformacion(datosTransformacion* datoNodoTransformacion,uint32_t tamanioNombreNodo){
	pthread_mutex_lock(&mutexNodos);
	eliminarHilos(datoNodoTransformacion->conexion.nombreNodo,datoNodoTransformacion->nroBloque);
	list_add(listaNodosCaidos,datoNodoTransformacion->conexion.nombreNodo);
	pthread_mutex_unlock(&mutexNodos);

	cantidadFallos++;

	void* datosAEnviarAYAMA = malloc(tamanioNombreNodo+sizeof(uint32_t));
	memcpy(datosAEnviarAYAMA,&tamanioNombreNodo,sizeof(uint32_t));
	memcpy(datosAEnviarAYAMA+sizeof(uint32_t),datoNodoTransformacion->conexion.nombreNodo,tamanioNombreNodo);

	log_debug(loggerMaster, "Datos de replanificacion para ser enviados a YAMA serializados con exito.\n");

	sendRemasterizado(datoNodoTransformacion->infoGeneral.socketYAMA,REPLANIFICAR,tamanioNombreNodo+sizeof(uint32_t),datosAEnviarAYAMA);

	free(datoNodoTransformacion->conexion.ipNodo);
	free(datoNodoTransformacion->nombreTemporal);
	free(datoNodoTransformacion->infoGeneral.scriptTransformacion);
	free(datoNodoTransformacion->conexion.nombreNodo);
	free(datosAEnviarAYAMA);
	free(datoNodoTransformacion);

	pthread_detach(pthread_self());
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

void manejadorTransformacionWorker(void* unDatoTransformacion){
	datosTransformacion* datoNodoTransformacion = (datosTransformacion*)unDatoTransformacion;

	uint32_t tamanioNombreNodo = string_length(datoNodoTransformacion->conexion.nombreNodo);

	int socketWorker = conectarWorker(datoNodoTransformacion->conexion.ipNodo, datoNodoTransformacion->conexion.puertoNodo);

	if(socketWorker!=-1){
		log_info(loggerMaster,"Se ha conectado con WORKER. IP: %s - PUERTO: %d \n",datoNodoTransformacion->conexion.ipNodo, datoNodoTransformacion->conexion.puertoNodo);

		int resultadoHandshake = realizarHandshakeWorker(socketWorker,ES_WORKER);

		if(resultadoHandshake!=-1){
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

			paralelismoTempMaximoTransformaciones++;

			tiempo tiempoInicialTransformacion = obtenerTiempo();
			int retornoSend = sendRemasterizadoWorker(socketWorker,TRANSFORMACION,tamanioScript+tamanioNombreScript+tamanioPathDestino+(sizeof(uint32_t)*5),datosAEnviar);

			free(datosAEnviar);
			free(codigoScript);

			if(retornoSend==-1){
				log_error(loggerMaster,"Falla al realizar send con Worker.\n");
				replanificarTransformacion(datoNodoTransformacion,tamanioNombreNodo);
			}
			else{
				int resultadoTransformacion = recvDeNotificacionMaster(socketWorker);

				calcularMaximoParalelos();

				paralelismoTempMaximoTransformaciones--;

				tiempo tiempoFinalTransformacion = obtenerTiempo();

				tiempo tiempoDuracionTransformacion = get_tiempo_total(tiempoInicialTransformacion,tiempoFinalTransformacion);

				sumarTiempos(0,tiempoDuracionTransformacion);

				if(resultadoTransformacion==TRANSFORMACION_TERMINADA){
					transformacionesRealizadas++;
					void* datosAEnviarAYAMA = malloc(tamanioNombreNodo+(sizeof(uint32_t)*2));

					memcpy(datosAEnviarAYAMA,&tamanioNombreNodo,sizeof(uint32_t));
					memcpy(datosAEnviarAYAMA+sizeof(uint32_t),datoNodoTransformacion->conexion.nombreNodo,tamanioNombreNodo);
					memcpy(datosAEnviarAYAMA+sizeof(uint32_t)+tamanioNombreNodo,&(datoNodoTransformacion->nroBloque),sizeof(uint32_t));

					log_info(loggerMaster, "Datos de transformacion terminada para ser enviados a YAMA serializados con exito.\n");

					free(datoNodoTransformacion->conexion.ipNodo);
					free(datoNodoTransformacion->nombreTemporal);
					free(datoNodoTransformacion->infoGeneral.scriptTransformacion);

					sendRemasterizado(datoNodoTransformacion->infoGeneral.socketYAMA,TRANSFORMACION_TERMINADA,tamanioNombreNodo+(sizeof(uint32_t)*2),datosAEnviarAYAMA);

					pthread_mutex_lock(&mutexNodos);
					eliminarHiloListaTransformacion(datoNodoTransformacion->conexion.nombreNodo,datoNodoTransformacion->nroBloque);
					pthread_mutex_unlock(&mutexNodos);

					free(datosAEnviarAYAMA);
					free(datoNodoTransformacion->conexion.nombreNodo);
					free(datoNodoTransformacion);

					pthread_detach(pthread_self());
				}
				else{
					replanificarTransformacion(datoNodoTransformacion,tamanioNombreNodo);
				}
			}
		}
		else{
			log_error(loggerMaster,"Falla al realizar handshake con Worker.\n");
			replanificarTransformacion(datoNodoTransformacion,tamanioNombreNodo);
		}

		close(socketWorker);
	}
	else{
		log_error(loggerMaster,"No se pudo conectar con el WORKER. IP: %s - PUERTO: %d \n",datoNodoTransformacion->conexion.ipNodo, datoNodoTransformacion->conexion.puertoNodo);
		replanificarTransformacion(datoNodoTransformacion,tamanioNombreNodo);
	}
}

void realizarTransformacion(int socketYAMA,char* scriptTransformador){
    uint32_t cantidadDeNodos = recibirUInt(socketYAMA);
    uint32_t i;
    for(i=0;i<cantidadDeNodos; i++){
    	datosTransformacion* unDatoTransformacion = (datosTransformacion*)malloc(sizeof(datosTransformacion));
    	unDatoTransformacion->nombreTemporal = string_new();
    	unDatoTransformacion->conexion.ipNodo = string_new();
    	unDatoTransformacion->conexion.nombreNodo = string_new();
    	unDatoTransformacion->infoGeneral.scriptTransformacion = string_new();
    	char* nombreNodo = recibirString(socketYAMA);
    	char* ipNodo = recibirString(socketYAMA);
    	unDatoTransformacion->conexion.puertoNodo = recibirUInt(socketYAMA);
    	uint32_t nroBloque = recibirUInt(socketYAMA);
    	unDatoTransformacion->nroBloque = nroBloque;
    	unDatoTransformacion->infoGeneral.socketYAMA = socketYAMA;
    	unDatoTransformacion->bytesOcupados = recibirUInt(socketYAMA);
    	char* nombreTemporal = recibirString(socketYAMA);
    	string_append(&(unDatoTransformacion->conexion.nombreNodo),nombreNodo);
    	string_append(&(unDatoTransformacion->conexion.ipNodo),ipNodo);
    	string_append(&(unDatoTransformacion->nombreTemporal),nombreTemporal);
    	string_append(&(unDatoTransformacion->infoGeneral.scriptTransformacion),scriptTransformador);
    	free(ipNodo);
    	free(nombreTemporal);
    	datosHilo* unDatoHilo = (datosHilo*) malloc(sizeof(datosHilo));
    	unDatoHilo->nombreNodo = string_new();
    	string_append(&(unDatoHilo->nombreNodo),nombreNodo);
    	unDatoHilo->numeroBloque = nroBloque;
    	pthread_t hiloManejadorWorker;
    	pthread_attr_t attr;
    	pthread_attr_init(&attr);
    	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    	pthread_mutex_lock(&mutexNodos);
    	bool verificacion = verificarNodoCaido(nombreNodo);
    	if(!verificacion){
    		pthread_create(&hiloManejadorWorker, &attr, (void*)manejadorTransformacionWorker, (void*)unDatoTransformacion);
    		unDatoHilo->hiloManejadorNodo = hiloManejadorWorker;
    		list_add(listaHilosTransformacion,unDatoHilo);
    		pthread_mutex_unlock(&mutexNodos);
    	}
    	else{
    		pthread_mutex_unlock(&mutexNodos);
    		free(unDatoHilo->nombreNodo);
    		free(unDatoHilo);
    		free(unDatoTransformacion->conexion.nombreNodo);
    		free(unDatoTransformacion->conexion.ipNodo);
    		free(unDatoTransformacion->nombreTemporal);
    		free(unDatoTransformacion->infoGeneral.scriptTransformacion);
    		free(unDatoTransformacion);
    	}
    	free(nombreNodo);
    }
}

infoReduccionLocal* recibirSolicitudReduccionLocal(int socketYAMA, char* scriptReduccion){
	uint32_t cantidadTemporales = recibirUInt(socketYAMA);
	char* nombreNodo = recibirString(socketYAMA);
	char* ipNodo = recibirString(socketYAMA);
	uint32_t puertoNodo = recibirUInt(socketYAMA);
	char* temporalReduccionLocal = recibirString(socketYAMA);
	uint32_t i,posicion;

	pthread_mutex_lock(&mutexTemporales);
	for(posicion=0;posicion<list_size(listaTemporales);posicion++){
		infoReduccionLocal* unaInfoTemporalNodo = list_get(listaTemporales,posicion);
		if(strcmp(unaInfoTemporalNodo->conexion.nombreNodo,nombreNodo)==0){
			pthread_mutex_unlock(&mutexTemporales);
			for(i=0;i<cantidadTemporales;i++){
				char* unArchivoTemporal = recibirString(socketYAMA);
				list_add(unaInfoTemporalNodo->archivosTemporales,unArchivoTemporal);
			}
			free(temporalReduccionLocal);
			free(ipNodo);
			free(nombreNodo);
			return unaInfoTemporalNodo;
		}
	}
	pthread_mutex_unlock(&mutexTemporales);

	infoReduccionLocal* unaInfoTemporalNodo = (infoReduccionLocal*)malloc(sizeof(infoReduccionLocal));
	unaInfoTemporalNodo->noHayHiloCreado = true;
	unaInfoTemporalNodo->archivosTemporales = list_create();
	unaInfoTemporalNodo->temporalReduccionLocal = string_new();
	unaInfoTemporalNodo->conexion.ipNodo = string_new();
	unaInfoTemporalNodo->conexion.nombreNodo = string_new();
	unaInfoTemporalNodo->infoGeneral.scriptTransformacion = string_new();
	unaInfoTemporalNodo->infoGeneral.socketYAMA = socketYAMA;

	string_append(&(unaInfoTemporalNodo->temporalReduccionLocal),temporalReduccionLocal);
	string_append(&(unaInfoTemporalNodo->conexion.ipNodo),ipNodo);
	string_append(&(unaInfoTemporalNodo->conexion.nombreNodo),nombreNodo);
	string_append(&(unaInfoTemporalNodo->infoGeneral.scriptTransformacion),scriptReduccion);
	unaInfoTemporalNodo->conexion.puertoNodo = puertoNodo;
	for(i=0;i<cantidadTemporales;i++){
		char* unArchivoTemporal = recibirString(socketYAMA);
		list_add(unaInfoTemporalNodo->archivosTemporales,unArchivoTemporal);
	}
	pthread_mutex_lock(&mutexTemporales);
	list_add(listaTemporales,unaInfoTemporalNodo);
	pthread_mutex_unlock(&mutexTemporales);
	free(temporalReduccionLocal);
	free(ipNodo);
	free(nombreNodo);
	return unaInfoTemporalNodo;
}

uint32_t obtenerTamanioArchivoTemporales(t_list* listaArchivosTemporales,uint32_t cantidadTemporales){
	uint32_t posicion;
	uint32_t tamanio = 0;
	for(posicion=0;posicion<cantidadTemporales;posicion++){
		char* unArchivoTemporal = list_get(listaArchivosTemporales,posicion);
		tamanio += string_length(unArchivoTemporal);
		tamanio += sizeof(uint32_t);
	}
	return tamanio;
}

bool transformacionesSinFinalizarNodo(char* nombreNodo){
	uint32_t posicion;
	bool respuesta = false;
	pthread_mutex_lock(&mutexNodos);
	for(posicion=0;posicion<list_size(listaHilosTransformacion);posicion++){
		datosHilo* unDatoHilo = list_get(listaHilosTransformacion,posicion);
		if(strcmp(unDatoHilo->nombreNodo,nombreNodo)==0){
			respuesta = true;
		}
	}
	pthread_mutex_unlock(&mutexNodos);
	return respuesta;
}

void eliminarHiloListaReduccion(){
	uint32_t posicion;
	pthread_mutex_lock(&mutexReducciones);
	for(posicion=0;posicion<list_size(listaHilosReduccion);posicion++){
		identificadorHilo* unIdentificadorHilo = list_get(listaHilosReduccion,posicion);
		if(pthread_equal(pthread_self(),unIdentificadorHilo->hiloManejadorReduccion)!=0){
			list_remove(listaHilosReduccion,posicion);
			free(unIdentificadorHilo);
			break;
		}
	}
	pthread_mutex_unlock(&mutexReducciones);
}

void eliminarDatosTemporales(infoReduccionLocal* infoNodoReduccion){
	uint32_t posicion;
	pthread_mutex_lock(&mutexTemporales);
	for(posicion=0;posicion<list_size(listaTemporales);posicion++){
		infoReduccionLocal* unaInfoTemporalNodo = list_get(listaTemporales,posicion);
		if(strcmp(unaInfoTemporalNodo->conexion.nombreNodo,infoNodoReduccion->conexion.nombreNodo)==0){
			list_remove(listaTemporales,posicion);
			list_destroy_and_destroy_elements(unaInfoTemporalNodo->archivosTemporales, free);
			free(unaInfoTemporalNodo->temporalReduccionLocal);
			free(unaInfoTemporalNodo->conexion.ipNodo);
			free(unaInfoTemporalNodo->infoGeneral.scriptTransformacion);
			break;
		}
	}
	pthread_mutex_unlock(&mutexTemporales);
}

void manejadorReduccionWorker(void* unaInfoReduccionLocal){
	infoReduccionLocal* infoNodoReduccion = (infoReduccionLocal*)unaInfoReduccionLocal;

	while((transformacionesSinFinalizarNodo(infoNodoReduccion->conexion.nombreNodo)) || avisoCargaTemporal){
		log_info(loggerMaster,"Esperando a que terminen las transformaciones en el Nodo: %s ... \n",infoNodoReduccion->conexion.nombreNodo);
	}

	int socketWorker = conectarWorker(infoNodoReduccion->conexion.ipNodo, infoNodoReduccion->conexion.puertoNodo);
	if(socketWorker!=-1){
		log_info(loggerMaster,"Se ha conectado con WORKER. IP: %s - PUERTO: %d \n",infoNodoReduccion->conexion.ipNodo, infoNodoReduccion->conexion.puertoNodo);

		int resultadoHandshake = realizarHandshakeWorker(socketWorker,ES_WORKER);

		if(resultadoHandshake!=-1){
			log_info(loggerMaster,"Handshake con Worker realizado exitosamente.\n");

			char* codigoScript = obtenerContenido(infoNodoReduccion->infoGeneral.scriptTransformacion);
			uint32_t tamanioScript = string_length(codigoScript);
			uint32_t tamanioNombreScript = string_length(infoNodoReduccion->infoGeneral.scriptTransformacion);
			uint32_t tamanioPathDestino = string_length(infoNodoReduccion->temporalReduccionLocal);
			uint32_t cantidadTemporales = list_size(infoNodoReduccion->archivosTemporales);
			uint32_t tamanioAEnviar = tamanioScript+tamanioNombreScript+tamanioPathDestino+(sizeof(uint32_t)*4)+obtenerTamanioArchivoTemporales(infoNodoReduccion->archivosTemporales,cantidadTemporales);
			void* datosAEnviar = malloc(tamanioAEnviar);

			memcpy(datosAEnviar,&tamanioScript,sizeof(uint32_t));
			memcpy(datosAEnviar+sizeof(uint32_t),codigoScript,tamanioScript);
			memcpy(datosAEnviar+sizeof(uint32_t)+tamanioScript,&tamanioNombreScript,sizeof(uint32_t));
			memcpy(datosAEnviar+(sizeof(uint32_t)*2)+tamanioScript,infoNodoReduccion->infoGeneral.scriptTransformacion,tamanioNombreScript);
			memcpy(datosAEnviar+(sizeof(uint32_t)*2)+tamanioScript+tamanioNombreScript,&tamanioPathDestino,sizeof(uint32_t));
			memcpy(datosAEnviar+(sizeof(uint32_t)*3)+tamanioScript+tamanioNombreScript,infoNodoReduccion->temporalReduccionLocal,tamanioPathDestino);
			memcpy(datosAEnviar+(sizeof(uint32_t)*3)+tamanioScript+tamanioNombreScript+tamanioPathDestino,&cantidadTemporales,sizeof(uint32_t));
			uint32_t posicion, posicionActual;
			posicionActual = (sizeof(uint32_t)*4)+tamanioScript+tamanioNombreScript+tamanioPathDestino;
			for(posicion=0;posicion<cantidadTemporales;posicion++){
				char* unArchivoTemporal = list_remove(infoNodoReduccion->archivosTemporales,0);
				uint32_t tamanioArchivoTemporal = string_length(unArchivoTemporal);
				memcpy(datosAEnviar+posicionActual,&tamanioArchivoTemporal,sizeof(uint32_t));
				posicionActual += sizeof(uint32_t);
				memcpy(datosAEnviar+posicionActual,unArchivoTemporal,tamanioArchivoTemporal);
				posicionActual += tamanioArchivoTemporal;
				free(unArchivoTemporal);
			}

			log_info(loggerMaster, "Datos serializados de reduccion local listos para ser enviados a Worker.\n");

			paralelismoTempMaximoReducciones++;

			tiempo tiempoInicialReduccionLocal = obtenerTiempo();

			int retornoSend = sendRemasterizadoWorker(socketWorker,REDUCCION_LOCAL,tamanioAEnviar,datosAEnviar);

			if(retornoSend==-1){
				eliminarDatosTemporales(infoNodoReduccion);

				cantidadFallos++;

				free(datosAEnviar);
				free(codigoScript);
				close(socketWorker);

				log_error(loggerMaster, "Se enviara notificacion de error en reduccion local a YAMA.\n");

				free(infoNodoReduccion->conexion.nombreNodo);

				sendDeNotificacion(infoNodoReduccion->infoGeneral.socketYAMA,ERROR_REDUCCION_LOCAL);

				free(infoNodoReduccion);

				pthread_detach(pthread_self());
			}
			else{
				int resultadoReduccion = recvDeNotificacionMaster(socketWorker);

				calcularMaximoParalelos();

				paralelismoTempMaximoReducciones--;

				eliminarDatosTemporales(infoNodoReduccion);

				tiempo tiempoFinalReduccionLocal = obtenerTiempo();

				tiempo tiempoDuracionReduccionLocal = get_tiempo_total(tiempoInicialReduccionLocal,tiempoFinalReduccionLocal);

				sumarTiempos(1,tiempoDuracionReduccionLocal);

				uint32_t tamanioNombreNodo = string_length(infoNodoReduccion->conexion.nombreNodo);

				free(datosAEnviar);
				free(codigoScript);
				close(socketWorker);

				if(resultadoReduccion==REDUCCION_LOCAL_TERMINADA){
					reduccionesLocalesRealizadas++;

					void* datosAEnviarAYAMA = malloc(tamanioNombreNodo+sizeof(uint32_t));

					memcpy(datosAEnviarAYAMA,&tamanioNombreNodo,sizeof(uint32_t));
					memcpy(datosAEnviarAYAMA+sizeof(uint32_t),infoNodoReduccion->conexion.nombreNodo,tamanioNombreNodo);

					log_info(loggerMaster, "Datos de reduccion local terminada para ser enviados a YAMA serializados con exito.\n");

					sendRemasterizado(infoNodoReduccion->infoGeneral.socketYAMA,REDUCCION_LOCAL_TERMINADA,tamanioNombreNodo+sizeof(uint32_t),datosAEnviarAYAMA);

					eliminarHiloListaReduccion();

					free(infoNodoReduccion->conexion.nombreNodo);
					free(infoNodoReduccion);
					free(datosAEnviarAYAMA);

					pthread_detach(pthread_self());
				}
				else{
					log_error(loggerMaster, "Se enviara notificacion de error en reduccion local a YAMA.\n");

					cantidadFallos++;

					free(infoNodoReduccion->conexion.nombreNodo);

					sendDeNotificacion(infoNodoReduccion->infoGeneral.socketYAMA,ERROR_REDUCCION_LOCAL);

					free(infoNodoReduccion);

					pthread_detach(pthread_self());
				}
			}
		}
		else{
			log_error(loggerMaster,"No se pudo realizar el handshake con el worker.\n");
			cantidadFallos++;
			sendDeNotificacion(infoNodoReduccion->infoGeneral.socketYAMA,ERROR_REDUCCION_LOCAL);
			close(socketWorker);
			pthread_detach(pthread_self());
		}
	}
	else{
		log_error(loggerMaster,"No se pudo conectar con WORKER. IP: %s - PUERTO: %d \n",infoNodoReduccion->conexion.ipNodo, infoNodoReduccion->conexion.puertoNodo);
		cantidadFallos++;
		sendDeNotificacion(infoNodoReduccion->infoGeneral.socketYAMA,ERROR_REDUCCION_LOCAL);
		pthread_detach(pthread_self());
	}
}

void inicializarReduccionEnNodos(infoReduccionLocal* unaInfoReduccionLocal){
	if(unaInfoReduccionLocal->noHayHiloCreado){
		unaInfoReduccionLocal->noHayHiloCreado = false;
		avisoCargaTemporal = false;
		identificadorHilo* unIdentificadorHilo = (identificadorHilo*)malloc(sizeof(identificadorHilo));
		pthread_t hiloManejadorWorker;
		pthread_attr_t attr;
		pthread_attr_init(&attr);
		pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
		pthread_mutex_lock(&mutexReducciones);
		pthread_create(&hiloManejadorWorker, &attr, (void*)manejadorReduccionWorker, (void*)unaInfoReduccionLocal);
		unIdentificadorHilo->hiloManejadorReduccion = hiloManejadorWorker;
		list_add(listaHilosReduccion,unIdentificadorHilo);
		pthread_mutex_unlock(&mutexReducciones);
	}
	else{
		avisoCargaTemporal = false;
	}
}

bool procesosNoTerminados(){
	if((list_size(listaTemporales)!=0) || (list_size(listaHilosTransformacion)!=0) || (list_size(listaHilosReduccion)!=0)){
		return true;
	}
	else{
		return false;
	}
}

uint32_t obtenerTamanioLista(t_list* listaInfoGlobal){
	uint32_t posicion;
	uint32_t tamanioTotal = 0;
	for(posicion=0;posicion<list_size(listaInfoGlobal);posicion++){
		infoReduccionGlobal* infoEncargado = list_get(listaInfoGlobal,posicion);
		uint32_t tamanioIp = strlen(infoEncargado->conexion.ipNodo);
		uint32_t tamanioTemporal = strlen(infoEncargado->temporalReduccion);
		tamanioTotal += tamanioIp+tamanioTemporal+(sizeof(uint32_t)*3);
	}
	return tamanioTotal;
}

void destruirListaInfoGlobal(t_list* listaInfoGlobal){
	uint32_t posicion;
	uint32_t tamanioLista = list_size(listaInfoGlobal);
	for(posicion=0;posicion<tamanioLista;posicion++){
		infoReduccionGlobal* unaInfoReduxGlobal = list_remove(listaInfoGlobal,0);
		free(unaInfoReduxGlobal->conexion.ipNodo);
		free(unaInfoReduxGlobal->temporalReduccion);
		free(unaInfoReduxGlobal);
	}
	list_destroy(listaInfoGlobal);
}

void enviarDatosAWorker(t_list* listaInfoGlobal,uint32_t cantRedux,char* rutaReduxGlobal,char* scriptReductor,int socketYAMA){
	while(procesosNoTerminados()){
		log_info(loggerMaster,"Esperando a que terminen las transformaciones y reducciones locales... \n");
	}

	log_info(loggerMaster,"Transformaciones y reducciones locales terminadas, se procede a realizar la reduccion global... \n");

	infoReduccionGlobal* unaInfoReduxGlobalEncargado = list_remove(listaInfoGlobal,0);

	int socketWorker = conectarWorker(unaInfoReduxGlobalEncargado->conexion.ipNodo, unaInfoReduxGlobalEncargado->conexion.puertoNodo);

	if(socketWorker!=-1){
		log_info(loggerMaster,"Se ha conectado con el worker encargado. IP: %s - PUERTO: %d \n",unaInfoReduxGlobalEncargado->conexion.ipNodo, unaInfoReduxGlobalEncargado->conexion.puertoNodo);
		int resultadoHandshake = realizarHandshakeWorker(socketWorker,ES_WORKER);

		if(resultadoHandshake!=-1){
			log_info(loggerMaster,"Handshake con worker encargado realizado exitosamente.\n");

			char* codigoScript = obtenerContenido(scriptReductor);
			uint32_t tamanioScript = string_length(codigoScript);
			uint32_t tamanioTemporalEncargado = string_length(unaInfoReduxGlobalEncargado->temporalReduccion);
			uint32_t tamanioNombreScript = string_length(scriptReductor);
			uint32_t tamanioPathDestino = string_length(rutaReduxGlobal);
			uint32_t tamanioAEnviar = tamanioScript+tamanioNombreScript+tamanioPathDestino+tamanioTemporalEncargado+(sizeof(uint32_t)*5)+obtenerTamanioLista(listaInfoGlobal);
			void* datosAEnviar = malloc(tamanioAEnviar);
			uint32_t posicion, posicionActual;

			memcpy(datosAEnviar,&tamanioScript,sizeof(uint32_t));
			memcpy(datosAEnviar+sizeof(uint32_t),codigoScript,tamanioScript);
			memcpy(datosAEnviar+sizeof(uint32_t)+tamanioScript,&tamanioNombreScript,sizeof(uint32_t));
			memcpy(datosAEnviar+(sizeof(uint32_t)*2)+tamanioScript,scriptReductor,tamanioNombreScript);
			memcpy(datosAEnviar+(sizeof(uint32_t)*2)+tamanioScript+tamanioNombreScript,&tamanioPathDestino,sizeof(uint32_t));
			memcpy(datosAEnviar+(sizeof(uint32_t)*3)+tamanioScript+tamanioNombreScript,rutaReduxGlobal,tamanioPathDestino);
			memcpy(datosAEnviar+(sizeof(uint32_t)*3)+tamanioScript+tamanioNombreScript+tamanioPathDestino,&tamanioTemporalEncargado,sizeof(uint32_t));
			memcpy(datosAEnviar+(sizeof(uint32_t)*4)+tamanioScript+tamanioNombreScript+tamanioPathDestino,unaInfoReduxGlobalEncargado->temporalReduccion,tamanioTemporalEncargado);
			memcpy(datosAEnviar+(sizeof(uint32_t)*4)+tamanioScript+tamanioNombreScript+tamanioPathDestino+tamanioTemporalEncargado,&cantRedux,sizeof(uint32_t));
			posicionActual = (sizeof(uint32_t)*5)+tamanioScript+tamanioNombreScript+tamanioPathDestino+tamanioTemporalEncargado;

			for(posicion=0;posicion<cantRedux-1;posicion++){
				infoReduccionGlobal* unaInfoReduxGlobal = list_remove(listaInfoGlobal,0);
				uint32_t tamanioIP = strlen(unaInfoReduxGlobal->conexion.ipNodo);
				uint32_t tamanioArchivoTemporal = strlen(unaInfoReduxGlobal->temporalReduccion);
				memcpy(datosAEnviar+posicionActual,&tamanioArchivoTemporal,sizeof(uint32_t));
				posicionActual += sizeof(uint32_t);
				memcpy(datosAEnviar+posicionActual,unaInfoReduxGlobal->temporalReduccion,tamanioArchivoTemporal);
				posicionActual += tamanioArchivoTemporal;
				memcpy(datosAEnviar+posicionActual,&tamanioIP,sizeof(uint32_t));
				posicionActual += sizeof(uint32_t);
				memcpy(datosAEnviar+posicionActual,unaInfoReduxGlobal->conexion.ipNodo,tamanioIP);
				posicionActual += tamanioIP;
				memcpy(datosAEnviar+posicionActual,&(unaInfoReduxGlobal->conexion.puertoNodo),sizeof(uint32_t));
				posicionActual += sizeof(uint32_t);
				free(unaInfoReduxGlobal->conexion.ipNodo);
				free(unaInfoReduxGlobal->temporalReduccion);
				free(unaInfoReduxGlobal);
			}
			free(unaInfoReduxGlobalEncargado->conexion.ipNodo);
			free(unaInfoReduxGlobalEncargado->temporalReduccion);
			free(unaInfoReduxGlobalEncargado);

			log_info(loggerMaster, "Datos serializados de reduccion global listos para ser enviados a Worker.\n");

			free(codigoScript);
			list_destroy(listaInfoGlobal);

			tiempo tiempoInicialReduccionGlobal = obtenerTiempo();

			int retornoSend = sendRemasterizadoWorker(socketWorker,REDUCCION_GLOBAL,tamanioAEnviar,datosAEnviar);

			if(retornoSend==-1){
				log_error(loggerMaster,"Falla al enviar datos de reduccion global al worker\n");
				cantidadFallos++;
				free(datosAEnviar);
				sendDeNotificacion(socketYAMA,ERROR_REDUCCION_GLOBAL);
			}
			else{
				int resultadoReduccion = recvDeNotificacionMaster(socketWorker);

				tiempo tiempoFinalReduccionGlobal = obtenerTiempo();

				tiempoReduccionGlobal = get_tiempo_total(tiempoInicialReduccionGlobal,tiempoFinalReduccionGlobal);

				free(datosAEnviar);

				if(resultadoReduccion==REDUCCION_GLOBAL_TERMINADA){
					sendDeNotificacion(socketYAMA,REDUCCION_GLOBAL_TERMINADA);
				}
				else{
					cantidadFallos++;
					sendDeNotificacion(socketYAMA,ERROR_REDUCCION_GLOBAL);
				}
			}
		}
		else{
			log_error(loggerMaster,"Falla al realizar handshake con Worker.\n");
			cantidadFallos++;
			destruirListaInfoGlobal(listaInfoGlobal);
			sendDeNotificacion(socketYAMA,ERROR_REDUCCION_GLOBAL);
		}

		close(socketWorker);
	}
	else{
		log_error(loggerMaster,"No se pudo conectar con el WORKER. IP: %s - PUERTO: %d \n",unaInfoReduxGlobalEncargado->conexion.ipNodo, unaInfoReduxGlobalEncargado->conexion.puertoNodo);
		cantidadFallos++;
		destruirListaInfoGlobal(listaInfoGlobal);
		sendDeNotificacion(socketYAMA,ERROR_REDUCCION_GLOBAL);
	}

	free(rutaReduxGlobal);
}

void recibirSolicitudReduccionGlobal(int socketYAMA, char* scriptReduccion){
	infoReduccionGlobal* infoEncargado = (infoReduccionGlobal*)malloc(sizeof(infoReduccionGlobal));
	infoEncargado->conexion.ipNodo = string_new();
	infoEncargado->temporalReduccion = string_new();
	char* nombreNodoRecibidoEncargado = recibirString(socketYAMA);
	free(nombreNodoRecibidoEncargado);
	char* ipRecibidoEncargado = recibirString(socketYAMA);
	string_append(&(infoEncargado->conexion.ipNodo),ipRecibidoEncargado);
	free(ipRecibidoEncargado);
	infoEncargado->conexion.puertoNodo = recibirUInt(socketYAMA);
	char* temporalReduccionRecibidoEncargado = recibirString(socketYAMA);
	string_append(&(infoEncargado->temporalReduccion),temporalReduccionRecibidoEncargado);
	free(temporalReduccionRecibidoEncargado);
	t_list* listaInfoGlobal = list_create();
	list_add(listaInfoGlobal,infoEncargado);

	char* rutaReduccionGlobal = recibirString(socketYAMA);
	uint32_t cantidadReducciones = recibirUInt(socketYAMA);

	uint32_t posicion;
	for(posicion=0;posicion<cantidadReducciones;posicion++){
		infoReduccionGlobal* unaInfoReduxGlobal = (infoReduccionGlobal*)malloc(sizeof(infoReduccionGlobal));
		unaInfoReduxGlobal->conexion.ipNodo = string_new();
		unaInfoReduxGlobal->temporalReduccion = string_new();
		char* nombreNodoRecibido = recibirString(socketYAMA);
		free(nombreNodoRecibido);
		char* ipRecibido = recibirString(socketYAMA);
		string_append(&(unaInfoReduxGlobal->conexion.ipNodo),ipRecibido);
		free(ipRecibido);
		unaInfoReduxGlobal->conexion.puertoNodo = recibirUInt(socketYAMA);
		char* temporalReduccionRecibido = recibirString(socketYAMA);
		string_append(&(unaInfoReduxGlobal->temporalReduccion),temporalReduccionRecibido);
		free(temporalReduccionRecibido);
		list_add(listaInfoGlobal,unaInfoReduxGlobal);
	}
	log_info(loggerMaster,"Se recibio todos los datos de la reduccion global de YAMA. \n");
	enviarDatosAWorker(listaInfoGlobal,cantidadReducciones+1,rutaReduccionGlobal,scriptReduccion,socketYAMA);
}

char* obtenerResultante(char* rutaCompleta,uint32_t valor){
	uint32_t posicion;
	uint32_t posicionMaxima = 0;

	for(posicion=0;rutaCompleta[posicion]!='\0';posicion++){
		if(rutaCompleta[posicion]=='/'){
			posicionMaxima = posicion;
		}
	}

	if(valor==1){
		return string_substring_until(rutaCompleta,posicionMaxima+1);
	}
	else{
		return string_substring_from(rutaCompleta,posicionMaxima+1);
	}
}

void recibirSolicitudAlmacenamiento(int socketYAMA,char* rutaCompleta){
	char* ipWorker = recibirString(socketYAMA);
	uint32_t puertoWorker = recibirUInt(socketYAMA);
	char* archivoReduxGlobal = recibirString(socketYAMA);

	int socketWorker = conectarWorker(ipWorker, puertoWorker);

	if(socketWorker!=-1){
		log_info(loggerMaster,"Se ha conectado con WORKER. IP: %s - PUERTO: %d \n",ipWorker, puertoWorker);

		int resultadoHandshake = realizarHandshakeWorker(socketWorker,ES_WORKER);

		if(resultadoHandshake!=-1){
			log_info(loggerMaster,"Handshake con Worker realizado exitosamente.\n");

			char* nombreResultante = obtenerResultante(rutaCompleta,0);
			char* rutaResultante = obtenerResultante(rutaCompleta,1);
			uint32_t tamanioArchivoReduxGlobal = string_length(archivoReduxGlobal);
			uint32_t tamanioNombreResultante = string_length(nombreResultante);
			uint32_t tamanioRutaResultante = string_length(rutaResultante);

			uint32_t tamanioAEnviar = tamanioArchivoReduxGlobal+tamanioNombreResultante+tamanioRutaResultante+(sizeof(uint32_t)*3);
			void* datosAEnviar = malloc(tamanioAEnviar);

			memcpy(datosAEnviar,&tamanioArchivoReduxGlobal,sizeof(uint32_t));
			memcpy(datosAEnviar+sizeof(uint32_t),archivoReduxGlobal,tamanioArchivoReduxGlobal);
			memcpy(datosAEnviar+sizeof(uint32_t)+tamanioArchivoReduxGlobal,&tamanioNombreResultante,sizeof(uint32_t));
			memcpy(datosAEnviar+(sizeof(uint32_t)*2)+tamanioArchivoReduxGlobal,nombreResultante,tamanioNombreResultante);
			memcpy(datosAEnviar+(sizeof(uint32_t)*2)+tamanioArchivoReduxGlobal+tamanioNombreResultante,&tamanioRutaResultante,sizeof(uint32_t));
			memcpy(datosAEnviar+(sizeof(uint32_t)*3)+tamanioArchivoReduxGlobal+tamanioNombreResultante,rutaResultante,tamanioRutaResultante);

			log_info(loggerMaster, "Datos serializados de almacenamiento final listos para ser enviados a Worker.\n");

			int retornoSend = sendRemasterizadoWorker(socketWorker,ALMACENADO_FINAL,tamanioAEnviar,datosAEnviar);

			free(datosAEnviar);
			free(nombreResultante);
			free(rutaResultante);

			if(retornoSend==-1){
				log_error(loggerMaster,"Falla al enviar datos de almacenado final al worker.\n");
				cantidadFallos++;
				sendDeNotificacion(socketYAMA,ERROR_ALMACENADO_FINAL);
			}
			else{
				int resultadoReduccion = recvDeNotificacionMaster(socketWorker);

				if(resultadoReduccion==ALMACENADO_FINAL_TERMINADO){
					sendDeNotificacion(socketYAMA,ALMACENADO_FINAL_TERMINADO);
					finalizarHilos();
					liberarListas();
					free(YAMA_IP);
					close(socketYAMA);
					mostrarMetricas();
					pthread_mutex_destroy(&mutexNodos);
					pthread_mutex_destroy(&mutexReducciones);
					pthread_mutex_destroy(&mutexTemporales);
					log_destroy(loggerMaster);
					exit(0);
				}
				else{
					cantidadFallos++;
					sendDeNotificacion(socketYAMA,ERROR_ALMACENADO_FINAL);
				}
			}
		}
		else{
			log_error(loggerMaster,"Falla al realizar handshake con Worker.\n");
			cantidadFallos++;
			sendDeNotificacion(socketYAMA,ERROR_ALMACENADO_FINAL);
		}
		close(socketWorker);
	}
	else{
		log_error(loggerMaster,"No se pudo conectar con el WORKER. IP: %s - PUERTO: %d \n",ipWorker, puertoWorker);
		cantidadFallos++;
		sendDeNotificacion(socketYAMA,ERROR_ALMACENADO_FINAL);
	}

	free(ipWorker);
	free(archivoReduxGlobal);
}

void darPermisosAScripts(char* script){
	struct stat infoScript;

	char* comandoAEjecutar = string_new();
	string_append(&comandoAEjecutar,"chmod 0777 ");
	string_append(&comandoAEjecutar,script);

	int resultado = system(comandoAEjecutar);

	if(!WIFEXITED(resultado)){
		log_error(loggerMaster,"Error al otorgar permisos al script.\n");

		if(WIFSIGNALED(resultado)){
			log_error(loggerMaster, "La llamada al sistema termino con la senial %d\n",WTERMSIG(resultado));
		}

		cantidadFallos++;
	}
	else{
		log_info(loggerMaster, "Script ejecutado correctamente con el valor de retorno: %d\n",WEXITSTATUS(resultado));
	}

	if(stat(script,&infoScript)!=0){
		log_error(loggerMaster,"No se pudo obtener informacion del script.\n");
		cantidadFallos++;
	}
	else{
		log_info(loggerMaster,"Los permisos para el script son: %08x\n",infoScript.st_mode);
	}

	free(comandoAEjecutar);
}

void laVioladora(int signal){
	log_info(loggerMaster, "Se recibio la senial SIGINT, muriendo con estilo... \n");
	mostrarMetricas();
	free(YAMA_IP);
	finalizarHilos();
	liberarListas();
	log_info(loggerMaster, "¡¡Adios logger!! \n");
	log_destroy(loggerMaster);
	pthread_mutex_destroy(&mutexNodos);
	pthread_mutex_destroy(&mutexReducciones);
	pthread_mutex_destroy(&mutexTemporales);
	exit(0);
}

void inicializarVariablesGlobales(){
	loggerMaster = log_create("Master.log", "Master", 1, 0);
	tiempoI = obtenerTiempo();
	cantidadFallos = 0;
	transformacionesRealizadas = 0;
	reduccionesLocalesRealizadas = 0;
	paralelismoMaximoTransformaciones = 0;
	paralelismoMaximoReducciones = 0;
	paralelismoTempMaximoTransformaciones = 0;
	paralelismoTempMaximoReducciones = 0;
	listaNodosCaidos = list_create();
	listaHilosTransformacion = list_create();
	listaTemporales = list_create();
	listaHilosReduccion = list_create();
	pthread_mutex_init(&mutexNodos,NULL);
	pthread_mutex_init(&mutexReducciones,NULL);
	pthread_mutex_init(&mutexTemporales,NULL);
	tiempoTransformacion.hora = 0;
	tiempoTransformacion.minuto = 0;
	tiempoTransformacion.segundo = 0;
	tiempoReduccionLocal.hora = 0;
	tiempoReduccionLocal.minuto = 0;
	tiempoReduccionLocal.segundo = 0;
	tiempoReduccionGlobal.hora = 0;
	tiempoReduccionGlobal.minuto = 0;
	tiempoReduccionGlobal.segundo = 0;
}

int main(int argc, char **argv) {
	inicializarVariablesGlobales();
	signal(SIGINT, laVioladora);
	chequearParametros(argc,6);
	t_config* configuracionMaster = generarTConfig(argv[1], 2);
	//t_config* configuracionMaster = generarTConfig("Debug/master.ini", 2);
	cargarMaster(configuracionMaster);
	log_info(loggerMaster,"Se cargo exitosamente Master.\n");
    int socketYAMA = conectarAServer(YAMA_IP, YAMA_PUERTO);
    log_info(loggerMaster,"Se ha conectado con YAMA. IP: %s - PUERTO: %d \n",YAMA_IP,YAMA_PUERTO);
    realizarHandshake(socketYAMA,ES_YAMA);
    log_info(loggerMaster,"Handshake con YAMA realizado exitosamente.\n");
    enviarArchivoAYAMA(argv[4],socketYAMA);
    log_info(loggerMaster,"Envio de archivo realizado con exito.\n");
    darPermisosAScripts(argv[2]);
    darPermisosAScripts(argv[3]);
    while(1){
    	int operacion = recvDeNotificacion(socketYAMA);
    	switch(operacion){
    	case TRANSFORMACION:{
    		realizarTransformacion(socketYAMA,argv[2]);
    		break;
    	}
    	case REDUCCION_LOCAL:{
    		avisoCargaTemporal = true;
    		infoReduccionLocal* unaInfoReduccionLocal = recibirSolicitudReduccionLocal(socketYAMA,argv[3]);
    		inicializarReduccionEnNodos(unaInfoReduccionLocal);
    		break;
    	}
    	case REDUCCION_GLOBAL:{
    		log_info(loggerMaster,"Se recibio la solicitud de reduccion global de YAMA. \n");
    		recibirSolicitudReduccionGlobal(socketYAMA,argv[3]);
    		break;
    	}
    	case ALMACENADO_FINAL:{
    		recibirSolicitudAlmacenamiento(socketYAMA,argv[5]);
    		break;
    	}
    	case NO_REDU_LOCAL:{
    		log_debug(loggerMaster,"Se esta esperando a que finalicen las transformaciones en los nodos correspondientes para empezar a realizar la reduccion local. \n");
    		break;
    	}
    	case ABORTAR:{
    		log_error(loggerMaster,"Se abortara el job.\n");
    		mostrarMetricas();
    		finalizarHilos();
    		liberarListas();
    		free(YAMA_IP);
    		close(socketYAMA);
    		pthread_mutex_destroy(&mutexNodos);
    		pthread_mutex_destroy(&mutexReducciones);
    		pthread_mutex_destroy(&mutexTemporales);
    		log_destroy(loggerMaster);
    		exit(-1);
    		break;
    	}
    	default:{
    		log_error(loggerMaster,"La conexion recibida es erronea.\n");
    		cantidadFallos++;
    		mostrarMetricas();
    		finalizarHilos();
    		liberarListas();
    		free(YAMA_IP);
    		close(socketYAMA);
    		pthread_mutex_destroy(&mutexNodos);
    		pthread_mutex_destroy(&mutexReducciones);
    		pthread_mutex_destroy(&mutexTemporales);
    		log_destroy(loggerMaster);
    		exit(-1);
    	}
    	}
    }
	return EXIT_SUCCESS;
}
