#include "reduccionLocal.h"

infoReduccionLocal* recibirSolicitudReduccionLocal(){
	infoReduccionLocal* solicitud = malloc(sizeof(infoReduccionLocal));
	solicitud->conexion = (conexionNodo*) malloc(sizeof(conexionNodo));
	solicitud->temporalesTransformacion = list_create();
	solicitud->temporalReduccionLocal = string_new();

	uint32_t cantidadTemporales = recibirUInt(socketYAMA);
	int i;

	solicitud->conexion->nombreNodo = recibirString(socketYAMA);
	solicitud->conexion->ipNodo = recibirString(socketYAMA);
	solicitud->conexion->puertoNodo = recibirUInt(socketYAMA);
	solicitud->temporalReduccionLocal = recibirString(socketYAMA);

	for(i=0; i<cantidadTemporales;i++){
		char* temporal = recibirString(socketYAMA);
		list_add(solicitud->temporalesTransformacion,temporal);
	}

	return solicitud;
}

void conectarAWorkerReduccionLocal(void* solicitud){
	infoReduccionLocal nuevo = *(infoReduccionLocal*) solicitud;
	void * datosToWorker = serializarReduccionLocalToWorker(nuevo.temporalesTransformacion, nuevo.temporalReduccionLocal);
	int tamanioDatosToWorker = obtenerTamanioReduccionToWorker(nuevo.temporalesTransformacion, nuevo.temporalReduccionLocal);

	log_info(loggerMaster,"Realizando conexion con Worker. IP: %s. Puerto: %d.", nuevo.conexion->ipNodo, nuevo.conexion->puertoNodo);
	int socketWorker = conectarAServer(nuevo.conexion->ipNodo, nuevo.conexion->puertoNodo);
	sendDeNotificacion(socketWorker, REDUCCION_LOCAL);
	log_info(loggerMaster,"Conexion con worker exitosa.");
	log_info(loggerMaster,"Propagando archivo de reduccion local a Worker.");
	propagarArchivo(scriptReduccion,socketWorker);
	log_info(loggerMaster,"Propagacion de archivo a Worker exitosa.");
	log_info(loggerMaster,"Enviando archivos temporales de transformacion y temporal de reduccion local: %s.",nuevo.temporalReduccionLocal);
	sendRemasterizado(socketWorker,REDUCCION_LOCAL,tamanioDatosToWorker, datosToWorker);
	uint32_t respuesta = recibirUInt(socketWorker);                                                 //CONFIRMACION DE WORKER
    log_info(loggerMaster,"Recibo respuesta por parte del Worker.");
    log_info(loggerMaster,"Notifico a YAMA del resultado de la transformacion.");
    //sendRemasterizado(socketYAMA,respuesta, tamanioDatosToY, datosToYama);        

    free(datosToWorker);
    free(solicitud);
}


void procesarReduccionLocal(){
	uint32_t i;

	for(i=0; i<cantidadDeProcesosNodos; i++){
	   pthread_t hiloInit;
	   infoReduccionLocal* solicitud = malloc(sizeof(infoReduccionLocal));
	   solicitud->conexion = (conexionNodo*) malloc(sizeof(conexionNodo));
	   solicitud->temporalesTransformacion = list_create();
	   solicitud->temporalReduccionLocal = string_new();
	   solicitud = recibirSolicitudReduccionLocal();
	   log_info(loggerMaster,"Recibo solicitud de reduccion local.");
	   log_info(loggerMaster, "Realizando reduccion local en el Nodo: %s.",solicitud->conexion->nombreNodo);
       pthread_create(&hiloInit,NULL, (void*) conectarAWorkerReduccionLocal,(void*)solicitud);
       pthread_join(hiloInit,NULL);
	}
}

// LO QUE  ME MANDA WORKER DE REDUCCION LOCAL:
/*
   //char* script = recibirString(socketMaster);
   char* pathDestino = recibirString(socketMaster);
   char* nombreScript = recibirString(socketMaster);
   uint32_t cantidadTemporales = recibirUInt(socketMaster);
   uint32_t posicion;
   t_list* archivosTemporales = list_create();

   for(posicion = 0; posicion < cantidadTemporales; posicion++){
    char* unArchivoTemporal = recibirString(socketMaster);
    list_add(archivosTemporales,unArchivoTemporal);
   }
*/

uint32_t obtenerTamanioReduccionToWorker(t_list* temporales, char* temporalResultante){
	uint32_t tamanio = 0, posicion;

	tamanio += sizeof(uint32_t);
	tamanio += string_length(temporalResultante);
	
	tamanio += sizeof(uint32_t);
	tamanio += string_length(scriptReduccion);
	
	tamanio += list_size(temporales);	
		

	for(posicion = 0; posicion < list_size(temporales); posicion++){
		tamanio += sizeof(uint32_t);
		char* temporal = list_get(temporales,posicion); 
		tamanio += string_length(temporal);
	}

	return tamanio;
}

void* serializarReduccionLocalToWorker(t_list* temporales, char* temporalResultante){
	int posicionActual = 0;
	uint32_t i;
	uint32_t cantidadTemporales = list_size(temporales);
	void* infoSerializada = malloc(obtenerTamanioReduccionToWorker(temporales, temporalResultante));


	uint32_t tamanioTemporalResultante = string_length(temporalResultante);
	memcpy(infoSerializada + posicionActual, &tamanioTemporalResultante, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(infoSerializada + posicionActual, &temporalResultante, tamanioTemporalResultante);
	posicionActual += tamanioTemporalResultante;

	uint32_t tamanioScript = string_length(scriptReduccion);
	memcpy(infoSerializada + posicionActual, &tamanioScript, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(infoSerializada + posicionActual, &scriptReduccion, tamanioScript);
	posicionActual += tamanioScript;

	memcpy(infoSerializada + posicionActual, &cantidadTemporales, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);


	for(i=0; i<cantidadTemporales; i++){
		char* temporal = list_get(temporales, i);
		uint32_t tamanioTemporal = string_length(temporal);
		memcpy(infoSerializada + posicionActual, &tamanioTemporal, sizeof(uint32_t));
		posicionActual += sizeof(uint32_t);
		memcpy(infoSerializada + posicionActual, &temporal, tamanioTemporal);
		posicionActual += tamanioTemporal;
	}

	return infoSerializada;	

}

