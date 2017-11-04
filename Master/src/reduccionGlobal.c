#include "reduccionGlobal.h"


t_list * recibirSolicitudReduccionGlobal(){
	encargado = (infoEncargadoRG*) malloc(sizeof(infoEncargadoRG));
	encargado->conexion = (conexionNodo*) malloc(sizeof(conexionNodo));
	t_list * listaPRecibir = list_create();

	encargado->conexion->nombreNodo = recibirString(socketYAMA);
	encargado->conexion->ipNodo = recibirString(socketYAMA);
	encargado->conexion->puertoNodo = recibirUInt(socketYAMA);
	encargado->temporalReduccionLocal = recibirString(socketYAMA);
	encargado->temporalReduccionGlobal = recibirString(socketYAMA);

	uint32_t cantidadNodos = recibirUInt(socketYAMA);
	uint32_t posicion;

	for(posicion=0; posicion < cantidadNodos; posicion++){
		infoReduccionGlobal * nodoActual = (infoReduccionGlobal*) malloc(sizeof(infoReduccionGlobal));
		nodoActual->conexion = (conexionNodo*) malloc(sizeof(conexionNodo));

		nodoActual->conexion->nombreNodo = recibirString(socketYAMA);
		nodoActual->conexion->ipNodo = recibirString(socketYAMA);
		nodoActual->conexion->puertoNodo = recibirUInt(socketYAMA);
		nodoActual->temporalReduccionLocal = recibirString(socketYAMA);

		list_add(listaPRecibir,nodoActual);
	}

	return listaPRecibir;

}


void procesarReduccionGlobal(){
	t_list * solicitudesReduccionGlobal = recibirSolicitudReduccionGlobal();
	char* contenidoScript = obtenerContenido(scriptReduccion);
	void * datos = serializarReduccionGlobalToWorker(solicitudesReduccionGlobal, contenidoScript);
	uint32_t tamanioSolicitudes = tamanioDatosToWorkerReduccion(solicitudesReduccionGlobal,contenidoScript);

	log_info(loggerMaster,"Realizando conexion con Worker. IP: %s. Puerto: %d.", encargado->conexion->ipNodo, encargado->conexion->puertoNodo);
	int workerEncargado = conectarAServer(encargado->conexion->ipNodo, encargado->conexion->puertoNodo);

	// VERIFICO SI ME PUDE CONECTAR CON WORKER
	if(workerEncargado==0){
		log_info(loggerMaster, "No se pudo conectar con Worker en ETAPA REDUCCION GLOBAL."); 
        log_info(loggerMaster, "Avisando a YAMA que fallo conexion con nodo ENCARGADO.");  
        //pthread_mutex_lock(&mutexReplanificacionTransformacion);        
        sendDeNotificacion(socketYAMA,ERROR_REDUCCION_GLOBAL);        
        int respuestaReplanificacion = recvDeNotificacionMaster(socketYAMA);
        //pthread_mutex_unlock(&mutexReplanificacionTransformacion);

        switch (respuestaReplanificacion){
                case ABORTAR:{
                    log_info(loggerMaster, "Fallo etapa de Reduccion global en nodo encargado: %s .",encargado->conexion->nombreNodo);
                    log_info(loggerMaster, "Cerrando proceso Master!.");
                    free(datos);
                    free(solicitudesReduccionGlobal);
                    exit(-1);
                } break;          
                default:{
                    log_info(loggerMaster, "Tipo de mensaje no coincidente.");
                    log_info(loggerMaster, "Cerrando proceso Master!.");
                    free(datos);
                    free(solicitudesReduccionGlobal);
                    exit(-1);
                } break;

        }
    // ME PUDE CONECTAR    
	} else{
		log_info(loggerMaster,"Conexion con worker encargado exitosa.");
		sendRemasterizado(workerEncargado, REDUCCION_GLOBAL, tamanioSolicitudes, datos);
		log_info(loggerMaster,"Recibo respuesta por parte del Worker.");
		//pthread_mutex_lock(&mutexReplanificacionTransformacion);
		int respuesta = recvDeNotificacionMaster(workerEncargado);                                                 //CONFIRMACION DE WORKER
		//pthread_mutex_unlock(&mutexReplanificacionTransformacion);

		// VERIFICO SI WORKER NO SE CAYO EN EL MEDIO	
		if(respuesta==0){
			log_info(loggerMaster, "Worker se cayo y no se pudo recibir una respuesta.");  
            log_info(loggerMaster, "Avisando a YAMA que fallo conexion con: %s.",encargado->conexion->nombreNodo);                      
            //pthread_mutex_lock(&mutexReplanificacionTransformacion);
            sendDeNotificacion(socketYAMA,ERROR_REDUCCION_LOCAL);
            int respuestaReplanificacion = recvDeNotificacionMaster(socketYAMA);
            //pthread_mutex_unlock(&mutexReplanificacionTransformacion);

            switch (respuestaReplanificacion){
                case ABORTAR:{
                    log_info(loggerMaster, "Fallo etapa de Reduccion global en nodo encargado.");
                    log_info(loggerMaster, "Cerrando proceso Master!.");
                    free(datos);
                    free(solicitudesReduccionGlobal);
                    exit(-1);
                } break;          
                default:{
                    log_info(loggerMaster, "Tipo de mensaje no coincidente.");
                    log_info(loggerMaster, "Cerrando proceso Master!.");
                    free(datos);
                    free(solicitudesReduccionGlobal);
                    exit(-1);
                } break;
        	}
		} else{
			switch (respuesta){
				case REDUCCION_GLOBAL_TERMINADA:{
					log_info(loggerMaster,"Notifico a YAMA que la REDUCCION LOCAL fue TERMINADA.");
					sendDeNotificacion(socketYAMA,REDUCCION_GLOBAL_TERMINADA);
					free(datos);
    				free(solicitudesReduccionGlobal);
    				exit(-1);
				} break;
				case ERROR_REDUCCION_GLOBAL:{
					log_info(loggerMaster, "Worker me avisa que fallo la REDUCCION GLOBAL.");  
		            log_info(loggerMaster, "Avisando a YAMA que fallo REDUCCION GLOBAL con nodo encargado: %s.", encargado->conexion->nombreNodo);                      
		            //pthread_mutex_lock(&mutexReplanificacionTransformacion);
		            sendDeNotificacion(socketYAMA,ERROR_REDUCCION_GLOBAL);
		            int respuestaReplanificacion = recvDeNotificacionMaster(socketYAMA);
		            //pthread_mutex_unlock(&mutexReplanificacionTransformacion);

		            switch (respuestaReplanificacion){
		                case ABORTAR:{
		                    log_info(loggerMaster, "Fallo etapa de Reduccion global en nodo encargado.");
		                    log_info(loggerMaster, "Cerrando proceso Master!.");
		                    free(datos);
		                    free(solicitudesReduccionGlobal);
		                    exit(-1);
		                } break;          
		                default:{
		                    log_info(loggerMaster, "Tipo de mensaje no coincidente.");
		                    log_info(loggerMaster, "Cerrando proceso Master!.");
		                    free(datos);
		                    free(solicitudesReduccionGlobal);
		                    exit(-1);
		                } break;
		            }
				} break; // aca cierro case error reduccion global
				default:{
					log_info(loggerMaster, "Tipo de mensaje no coincidente.");
		            log_info(loggerMaster, "Cerrando proceso Master!.");
		            free(datos);
		            free(solicitudesReduccionGlobal);
		            exit(-1);	
				} break;
			} // aca cierro el switch de respuesta
		}
	}
}

/*

    log_info(loggerMaster,"Propagacion de archivo a Worker encargado exitosa.");
    log_info(loggerMaster,"Enviando archivos temporales de reduccion y temporal de reduccion global: %s.",encargado->temporalReduccionGlobal);
	
	int respuesta = recvDeNotificacion(workerEncargado);
	log_info(loggerMaster,"Recibo respuesta por parte del Worker encargado.");
	//sendRemasterizado() 									// TO YAMA QUE LE ENVIO?

*/
/*
void * serializarTemporalReduccionGlobalYScript(){
	void * infoSerializada = malloc(sizeof(uint32_t)*2 + string_length(encargado->temporalReduccionGlobal) + string_length(scriptReduccion));
	uint32_t posicionActual = 0;

	uint32_t tamanioTemporal = string_length(encargado->temporalReduccionGlobal);
	memcpy(infoSerializada + posicionActual, &tamanioTemporal, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(infoSerializada + posicionActual, encargado->temporalReduccionGlobal, tamanioTemporal);
	posicionActual += tamanioTemporal;

	uint32_t tamanioScript = string_length(scriptReduccion);
	memcpy(infoSerializada + posicionActual, &tamanioScript, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(infoSerializada + posicionActual, scriptReduccion, tamanioScript);
	posicionActual += tamanioScript;

	return infoSerializada;
}*/

uint32_t tamanioDatosToWorkerReduccion(t_list * reducciones, char* contenidoScript){
	uint32_t posicion;
	uint32_t tamanio = 0;

	tamanio += sizeof(uint32_t);
	tamanio += string_length(contenidoScript);

	tamanio += sizeof(uint32_t);
	tamanio += string_length(encargado->temporalReduccionGlobal);

	tamanio += sizeof(uint32_t);
	tamanio += string_length(scriptReduccion);

	tamanio += sizeof(uint32_t); // CANTIDAD NODOS 

	for(posicion=0; posicion<list_size(reducciones); posicion++){
		infoReduccionGlobal * nodoActual = list_get(reducciones, posicion);

		tamanio += sizeof(uint32_t);
		tamanio += string_length(nodoActual->temporalReduccionLocal);

		tamanio += sizeof(uint32_t);
		tamanio += string_length(nodoActual->conexion->ipNodo);

		tamanio += sizeof(uint32_t);		
	}

	return tamanio;
}



void* serializarReduccionGlobalToWorker(t_list * reducciones, char* contenidoScript){	
	uint32_t posicionFor;
	uint32_t posicionActual = 0;
	void* infoSerializada = malloc(tamanioDatosToWorkerReduccion(reducciones,contenidoScript));

	uint32_t tamanioContenido = string_length(contenidoScript);
    memcpy(infoSerializada + posicionActual, &tamanioContenido, sizeof(uint32_t));
    posicionActual += sizeof(uint32_t);
    memcpy(infoSerializada + posicionActual, contenidoScript, tamanioContenido);
    posicionActual += tamanioContenido;

	uint32_t tamanioTemporal = string_length(encargado->temporalReduccionGlobal);
	memcpy(infoSerializada + posicionActual, &tamanioTemporal, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(infoSerializada + posicionActual, encargado->temporalReduccionGlobal, tamanioTemporal);
	posicionActual += tamanioTemporal;

	uint32_t tamanioScript = string_length(scriptReduccion);
	memcpy(infoSerializada + posicionActual, &tamanioScript, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(infoSerializada + posicionActual, scriptReduccion, tamanioScript);
	posicionActual += tamanioScript;

	uint32_t largoLista = list_size(reducciones);
	memcpy(infoSerializada + posicionActual, &largoLista, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);

	for(posicionFor=0; posicionFor < largoLista; posicionFor++){
		infoReduccionGlobal * nodoActual = list_get(reducciones, posicionFor);

		uint32_t tamanioTemporalReduccionLocal = string_length(nodoActual->temporalReduccionLocal);
		memcpy(infoSerializada + posicionActual, &tamanioTemporalReduccionLocal, sizeof(uint32_t));
		posicionActual += sizeof(uint32_t);
		memcpy(infoSerializada + posicionActual, nodoActual->temporalReduccionLocal, tamanioTemporalReduccionLocal);
		posicionActual += tamanioTemporalReduccionLocal;

		uint32_t tamanioIP = string_length(nodoActual->conexion->ipNodo);
		memcpy(infoSerializada + posicionActual, &tamanioIP, sizeof(uint32_t));
		posicionActual += sizeof(uint32_t);
		memcpy(infoSerializada + posicionActual, nodoActual->conexion->ipNodo, tamanioIP);
		posicionActual += tamanioIP;

		memcpy(infoSerializada + posicionActual, &nodoActual->conexion->puertoNodo, sizeof(uint32_t));
		posicionActual += sizeof(uint32_t);


	}

	return infoSerializada;
}
