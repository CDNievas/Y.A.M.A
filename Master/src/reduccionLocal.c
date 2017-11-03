#include "reduccionLocal.h"

infoReduccionLocal* recibirSolicitudReduccionLocal(){
	infoReduccionLocal* solicitud = (infoReduccionLocal*) malloc(sizeof(infoReduccionLocal));
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

	char* contenidoScript = obtenerContenido(scriptReduccion);
	void * datosToWorker = serializarReduccionLocalToWorker(nuevo.temporalesTransformacion, nuevo.temporalReduccionLocal, contenidoScript);
	int tamanioDatosToWorker = obtenerTamanioReduccionToWorker(nuevo.temporalesTransformacion, nuevo.temporalReduccionLocal, contenidoScript);

	log_info(loggerMaster,"Realizando conexion con Worker. IP: %s. Puerto: %d.", nuevo.conexion->ipNodo, nuevo.conexion->puertoNodo);
	int socketWorker = conectarAServer(nuevo.conexion->ipNodo, nuevo.conexion->puertoNodo);
	// VERIFICO SI ME PUDE CONECTAR CON WORKER
	if(socketWorker==0){
		log_info(loggerMaster, "No se pudo conectar con Worker en ETAPA REDUCCION LOCAL."); 
        log_info(loggerMaster, "Avisando a YAMA que fallo conexion con: %s.", nuevo.conexion->nombreNodo);  
        //pthread_mutex_lock(&mutexReplanificacionTransformacion);        
        sendDeNotificacion(socketYAMA,ERROR_REDUCCION_LOCAL);        
        int respuestaReplanificacion = recvDeNotificacionMaster(socketYAMA);
        //pthread_mutex_unlock(&mutexReplanificacionTransformacion);

        switch (respuestaReplanificacion){
                case ABORTAR:{
                    log_info(loggerMaster, "Fallo etapa de Reduccion local en nodo: %s .", nuevo.conexion->nombreNodo);
                    log_info(loggerMaster, "Cerrando proceso Master!.");
                    free(datosToWorker);
                    free(solicitud);
                    exit(-1);
                } break;          
                default:{
                    log_info(loggerMaster, "Tipo de mensaje no coincidente.");
                    log_info(loggerMaster, "Cerrando proceso Master!.");
                    free(datosToWorker);
                    free(solicitud);
                    exit(-1);
                } break;

        }
    // ME PUDE CONECTAR    
	} else{
		log_info(loggerMaster,"Conexion con worker exitosa.");
		log_info(loggerMaster,"Enviando archivos temporales de transformacion y temporal de reduccion local: %s.",nuevo.temporalReduccionLocal);
		sendRemasterizado(socketWorker,REDUCCION_LOCAL,tamanioDatosToWorker, datosToWorker);
		log_info(loggerMaster,"Recibo respuesta por parte del Worker.");
		//pthread_mutex_lock(&mutexReplanificacionTransformacion);
		int respuesta = recvDeNotificacionMaster(socketWorker);                                                 //CONFIRMACION DE WORKER
		//pthread_mutex_unlock(&mutexReplanificacionTransformacion);

	// VERIFICO SI WORKER NO SE CAYO EN EL MEDIO	
		if(respuesta==0){
			log_info(loggerMaster, "Worker se cayo y no se pudo recibir una respuesta.");  
            log_info(loggerMaster, "Avisando a YAMA que fallo conexion con: %s.", nuevo.conexion->nombreNodo);                      
            //pthread_mutex_lock(&mutexReplanificacionTransformacion);
            sendDeNotificacion(socketYAMA,ERROR_REDUCCION_LOCAL);
            int respuestaReplanificacion = recvDeNotificacionMaster(socketYAMA);
            //pthread_mutex_unlock(&mutexReplanificacionTransformacion);

            switch (respuestaReplanificacion){
                case ABORTAR:{
                    log_info(loggerMaster, "Fallo etapa de Reduccion local en nodo: %s .", nuevo.conexion->nombreNodo);
                    log_info(loggerMaster, "Cerrando proceso Master!.");
                    free(datosToWorker);
                    free(solicitud);
                    exit(-1);
                } break;          
                default:{
                    log_info(loggerMaster, "Tipo de mensaje no coincidente.");
                    log_info(loggerMaster, "Cerrando proceso Master!.");
                    free(datosToWorker);
                    free(solicitud);
                    exit(-1);
                } break;
        	}
		} else{
			switch (respuesta){
				case REDUCCION_LOCAL_TERMINADA:{
					log_info(loggerMaster,"Notifico a YAMA que la REDUCCION LOCAL fue TERMINADA.");
					sendDeNotificacion(socketYAMA,REDUCCION_LOCAL_TERMINADA);
					free(datosToWorker);
    				free(solicitud);
    				exit(-1);
				} break;
				case ERROR_REDUCCION_LOCAL:{
					log_info(loggerMaster, "Worker me avisa que fallo la REDUCCION LOCAL.");  
		            log_info(loggerMaster, "Avisando a YAMA que fallo REDUCCION LOCAL con: %s.", nuevo.conexion->nombreNodo);                      
		            //pthread_mutex_lock(&mutexReplanificacionTransformacion);
		            sendDeNotificacion(socketYAMA,ERROR_REDUCCION_LOCAL);
		            int respuestaReplanificacion = recvDeNotificacionMaster(socketYAMA);
		            //pthread_mutex_unlock(&mutexReplanificacionTransformacion);

		            switch (respuestaReplanificacion){
		                case ABORTAR:{
		                    log_info(loggerMaster, "Fallo etapa de Reduccion local en nodo: %s .", nuevo.conexion->nombreNodo);
		                    log_info(loggerMaster, "Cerrando proceso Master!.");
		                    free(datosToWorker);
		                    free(solicitud);
		                    exit(-1);
		                } break;          
		                default:{
		                    log_info(loggerMaster, "Tipo de mensaje no coincidente.");
		                    log_info(loggerMaster, "Cerrando proceso Master!.");
		                    free(datosToWorker);
		                    free(solicitud);
		                    exit(-1);
		                } break;
		        	}
				} break;
				default:{
					log_info(loggerMaster, "Tipo de mensaje no coincidente.");
		            log_info(loggerMaster, "Cerrando proceso Master!.");
		            free(datosToWorker);
		            free(solicitud);
		            exit(-1);	
				} break;
			} // aca cierro el switch de respuesta
		}
	}   
}


void procesarReduccionLocal(){
	uint32_t i;

	for(i=0; i<cantidadDeProcesosNodos; i++){
	   pthread_t hiloInit;
	   infoReduccionLocal* solicitud = (infoReduccionLocal*) malloc(sizeof(infoReduccionLocal));
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

uint32_t obtenerTamanioReduccionToWorker(t_list* temporales, char* temporalResultante, char* contenidoScript){
	uint32_t tamanio = 0, posicion;

	tamanio += sizeof(uint32_t);
	tamanio += string_length(contenidoScript);

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

void* serializarReduccionLocalToWorker(t_list* temporales, char* temporalResultante,char* contenidoScript){
	int posicionActual = 0;
	uint32_t i;
	uint32_t cantidadTemporales = list_size(temporales);
	void* infoSerializada = malloc(obtenerTamanioReduccionToWorker(temporales, temporalResultante,contenidoScript));

	uint32_t tamanioContenido = string_length(contenidoScript);
    memcpy(infoSerializada + posicionActual, &tamanioContenido, sizeof(uint32_t));
    posicionActual += sizeof(uint32_t);
    memcpy(infoSerializada + posicionActual, contenidoScript, tamanioContenido);
    posicionActual += tamanioContenido;

	uint32_t tamanioTemporalResultante = string_length(temporalResultante);
	memcpy(infoSerializada + posicionActual, &tamanioTemporalResultante, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(infoSerializada + posicionActual, temporalResultante, tamanioTemporalResultante);
	posicionActual += tamanioTemporalResultante;

	uint32_t tamanioScript = string_length(scriptReduccion);
	memcpy(infoSerializada + posicionActual, &tamanioScript, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(infoSerializada + posicionActual, scriptReduccion, tamanioScript);
	posicionActual += tamanioScript;

	memcpy(infoSerializada + posicionActual, &cantidadTemporales, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);


	for(i=0; i<cantidadTemporales; i++){
		char* temporal = list_get(temporales, i);
		uint32_t tamanioTemporal = string_length(temporal);
		memcpy(infoSerializada + posicionActual, &tamanioTemporal, sizeof(uint32_t));
		posicionActual += sizeof(uint32_t);
		memcpy(infoSerializada + posicionActual, temporal, tamanioTemporal);
		posicionActual += tamanioTemporal;
	}

	return infoSerializada;	

}

