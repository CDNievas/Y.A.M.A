#include "transformaciones.h"

//EL SOCKETYAMA LO DECLARE COMO VARIABLE GLOBAL.

//Creo todos los hilos para cada solicitud.
void procesarTransformacion(){
    hilosCreados = list_create();    // Creo lista para almacenar INFO de todos los hilos
    t_list* solicitudesTransformacion = recibirSolicitudTransformacion();
    cantidadDeProcesosNodos = list_size(nombresNodos); // Asigno la cantidad de procesos nodos que tengo para la reduccion local
    log_info(loggerMaster,"Recibo lista de solicitudes de transformacion.");    
    int i;
    crearBanderasParaCadaNodo(); // CREO LAS BANDERAS DE REPLANIFICACION Y ABORTAR CON SUS RESPECTIVOS NODOS. FLAGS EN 0

    pthread_t threadKiller;
    pthread_create(&threadKiller,NULL, (void*) matadoraDeHilos,NULL);
    pthread_join(threadKiller,NULL);


    for(i=0; i < list_size(solicitudesTransformacion); i++){
        pthread_t hiloInit;
        infoNodo * nodoParaHilo = list_get(solicitudesTransformacion,i); // saco informacion de las posiciones una por una
        log_info(loggerMaster, "Realizando transformacion en el Nodo: %s.", nodoParaHilo->conexion->nombreNodo);
        pthread_create(&hiloInit,NULL, (void*) conectarAWorkerTransformacion,(void*)nodoParaHilo);
        pthread_join(hiloInit,NULL);
    }

}

// Creo lista de solicitudes de transformaciones
t_list* recibirSolicitudTransformacion(){
    t_list * listaPRecibir = list_create();   
    uint32_t cantidadNodos = recibirUInt(socketYAMA); // supuestamente recibo la cantidad total de datos primero.
    int i;
    for(i=0; i < cantidadNodos; i++){
        infoNodo * nodoActual = (infoNodo*) malloc(sizeof(infoNodo));
        nodoActual->conexion = (conexionNodo*) malloc(sizeof(conexionNodo));

        nodoActual->conexion->nombreNodo = recibirString(socketYAMA);
        verificarNodo(nodoActual->conexion->nombreNodo); // Verifico si puede agregar el nombre de nodo a la lista para saber cantidad de Procesos nodos 
        nodoActual->conexion->ipNodo = recibirString(socketYAMA);
        nodoActual->conexion->puertoNodo = recibirUInt(socketYAMA);
        nodoActual->nroBloque = recibirUInt(socketYAMA);
        nodoActual->bytesOcupados = recibirUInt(socketYAMA);
        nodoActual->nombreTemporal = recibirString(socketYAMA);
    
        list_add(listaPRecibir,nodoActual);
    }
    return listaPRecibir;
}


//Funcion que atiende las solicitudes de transformacion.
void conectarAWorkerTransformacion(void* nodoConInfo){
    infoNodo nuevo = *(infoNodo*) nodoConInfo;
    pthread_t thread = pthread_self();                      
    cargoHiloEnLista(thread,nuevo.conexion->nombreNodo);  // METO INFO (pthread y nodo) DEL HILO EN LISTA 

    char* contenidoScript = obtenerContenido(scriptTransformador); 
    void* datosToWorker = serializarTransformacionToWorker(nuevo.nroBloque, nuevo.bytesOcupados, nuevo.nombreTemporal, contenidoScript);
    void* datosToYama = serializarTransformacionToYama(nuevo.conexion->nombreNodo, nuevo.nroBloque);
    int tamanioDatosToW = tamanioDatosToWorker(nuevo.nombreTemporal, contenidoScript);
    int tamanioDatosToY = sizeof(uint32_t)*2 + string_length(nuevo.conexion->nombreNodo);

    log_info(loggerMaster,"Realizando conexion con Worker. IP: %s. Puerto: %d.", nuevo.conexion->ipNodo, nuevo.conexion->puertoNodo);
    int socketWorker = conectarAWorker(nuevo.conexion->ipNodo, nuevo.conexion->puertoNodo);         // CONECTAR A WORKER

    // 1) VERIFICO SI WORKER ESTA LEVANTADO.
    if(socketWorker == 0){
        log_info(loggerMaster, "No se pudo conectar con Worker en ETAPA TRANSFORMACION."); 
        log_info(loggerMaster, "Avisando a YAMA que fallo conexion con el nodo: %s.", nuevo.nroBloque);  
        //pthread_mutex_lock(&mutexReplanificacionTransformacion);        
        sendRemasterizado(socketYAMA,REPLANIFICAR, tamanioDatosToY, datosToYama);        
        int respuestaReplanificacion = recvDeNotificacionMaster(socketYAMA);
        //pthread_mutex_unlock(&mutexReplanificacionTransformacion);

        switch (respuestaReplanificacion){
                case ABORTAR:{
                    cambiarFlagNodo(nuevo.conexion->nombreNodo, NO,YES);
                } break;
                case REPLANIFICAR:{
                    cambiarFlagNodo(nuevo.conexion->nombreNodo, YES,NO);
                } break;
                default:{
                    cambiarFlagNodo(nuevo.conexion->nombreNodo, NO,YES);
                } break;

            }
    //  WORKER ESTA LEVANTADO.        
    } else{
        log_info(loggerMaster, "Conexion con Worker exitosa.");
        log_info(loggerMaster,"Enviando bloque:%d, bytes ocupados:%d y temporal:%s a Worker", nuevo.nroBloque, nuevo.bytesOcupados, nuevo.nombreTemporal);
        sendRemasterizado(socketWorker,TRANSFORMACION, tamanioDatosToW, datosToWorker);         
        // Uso las funciones wait y signal para recibir la respuesta. El mutex permite que no reciban dos hilos al mismo tiempo la rta
        //pthread_mutex_lock(&mutexRespuestaTransformacion);
        int respuesta = recvDeNotificacionMaster(socketWorker);                                                 //CONFIRMACION DE WORKER
        //pthread_mutex_unlock(&mutexRespuestaTransformacion);
        log_info(loggerMaster,"Recibo respuesta por parte del Worker.");

    // 2) VERIFICO RESPUESTA QUE ME DA WORKER.    
        if(respuesta == 0){
            log_info(loggerMaster, "Worker se cayo y no se pudo recibir una respuesta.");  
            log_info(loggerMaster, "Avisando a YAMA que fallo conexion con el nodo: %s.", nuevo.nroBloque);                      
            //pthread_mutex_lock(&mutexReplanificacionTransformacion);
            sendRemasterizado(socketYAMA,REPLANIFICAR, tamanioDatosToY, datosToYama);
            int respuestaReplanificacion = recvDeNotificacionMaster(socketYAMA);
            //pthread_mutex_unlock(&mutexReplanificacionTransformacion);

            // HACER SWITCH PARA ATENDER DISTINTAS RESPUESTAS POR PARTE DE YAMA
            switch (respuestaReplanificacion){
                case ABORTAR:{
                    cambiarFlagNodo(nuevo.conexion->nombreNodo, NO,YES);
                } break;
                case REPLANIFICAR:{
                    cambiarFlagNodo(nuevo.conexion->nombreNodo, YES,NO);
                } break;
                default:{
                    cambiarFlagNodo(nuevo.conexion->nombreNodo, NO,YES);
                } break;

            }
    //  WORKER RECIBE DATOS.         
        } else{ 
    // 3) VERIFICO DISTINTOS CASOS PARA LA RESPUESTA DE WORKER    
            switch (respuesta){
                case TRANSFORMACION_TERMINADA:{
                    log_info(loggerMaster,"Transformacion terminada con éxito. Nodo: %s, Bloque: %s",nuevo.conexion->nombreNodo, nuevo.nroBloque);
                    log_info(loggerMaster,"Notifico a YAMA del resultado de la transformacion.");
                    sendRemasterizado(socketYAMA,TRANSFORMACION_TERMINADA, tamanioDatosToY, datosToYama);
                    free(datosToWorker);
                    free(datosToYama);
                    free(nodoConInfo);
                } break;
                case ERROR_TRANSFORMACION:{
                    log_info(loggerMaster,"Transformacion terminada con ERRORES. Nodo: %s, Bloque: %s",nuevo.conexion->nombreNodo, nuevo.nroBloque);
                    log_info(loggerMaster,"Notifico a YAMA del resultado de la transformacion.");
                    //pthread_mutex_lock(&mutexReplanificacionTransformacion);
                    sendRemasterizado(socketYAMA,REPLANIFICAR, tamanioDatosToY, datosToYama);
                    int respuestaReplanificacion = recvDeNotificacionMaster(socketYAMA);
                    //pthread_mutex_unlock(&mutexReplanificacionTransformacion);

                    switch (respuestaReplanificacion){
                            case ABORTAR:{
                                cambiarFlagNodo(nuevo.conexion->nombreNodo, NO,YES);
                            } break;
                            case REPLANIFICAR:{
                                cambiarFlagNodo(nuevo.conexion->nombreNodo, YES,NO);
                            } break;
                            default:{
                                cambiarFlagNodo(nuevo.conexion->nombreNodo, NO,YES);
                            } break;
                    }
                }
            }
        }
    }
}


int tamanioDatosToWorker(char* nombreTemporal,char* contenidoScript){
    return sizeof(uint32_t)*5 + string_length(nombreTemporal) + string_length(scriptTransformador) + string_length(contenidoScript);
}

void matadoraDeHilos(){
    uint32_t pos;
    while(1){
        for(pos=0;pos<list_size(banderasReplanificacion);pos++){
            banderasParaNodos * banderas = list_get(banderasReplanificacion,pos);     // saco info de un nodo
            if(banderas->FLAG_REPLANIFICACION == YES){
                matarHilosDeTalNodo(banderas->nodo);
                recibirNuevasSolicitudesYReplanificar();
            } else{
                if(banderas->FLAG_ABORTAR == YES){
                    matarHilosDeTalNodo(banderas->nodo);
                    // ABORTAR! COMO?
                }
            }
        } // TERMINA FOR
    }
}

void recibirNuevasSolicitudesYReplanificar(){
    t_list* solicitudesTransformacion = recibirSolicitudTransformacion();

    uint32_t i;
    for(i=0; i < list_size(solicitudesTransformacion); i++){
        pthread_t hiloInit;
        infoNodo * nodoParaHilo = list_get(solicitudesTransformacion,i); // saco informacion de las posiciones una por una
        log_info(loggerMaster, "Realizando transformacion en el Nodo: %s.", nodoParaHilo->conexion->nombreNodo);
        pthread_create(&hiloInit,NULL, (void*) conectarAWorkerTransformacion,(void*)nodoParaHilo);
        pthread_join(hiloInit,NULL);
    }
}

void matarHilosDeTalNodo(char* nodo){
    uint32_t pos;

    for(pos=0; pos < list_size(hilosCreados); pos++){
        infoHilo * info = list_get(hilosCreados,pos);
        char* nodoAux = info->nodo;

        if(strcmp(nodoAux,nodo)==0){
            pthread_t threadAMatar = info->thread;
            pthread_detach(threadAMatar);
            list_remove(hilosCreados,pos);
        }
    }
}

// CREO UNA STRUCT BANDERA PARA CADA NODO.
void crearBanderasParaCadaNodo(){
    int i;
    banderasReplanificacion = list_create();

    for (i=0; i<list_size(nombresNodos); i++){
        banderasParaNodos * banderas = malloc(sizeof(banderasParaNodos));
        banderas->nodo = string_new();

        banderas->nodo = list_get(nombresNodos,i);
        banderas->FLAG_REPLANIFICACION = 0;
        banderas->FLAG_ABORTAR = 0;

        list_add(banderasReplanificacion,banderas);
    }
}

// CARGO INFO DE HILO EN UNA LISTA PARA PODER MATARLOS LUEGO
void cargoHiloEnLista(pthread_t thread, char* nodo){
    infoHilo * info = malloc(sizeof(infoHilo));
    info->nodo = string_new();
    info->nodo = nodo;
    info->thread = thread;
    list_add(hilosCreados,info);
}

// CON ESTO PUEDO CAMBIAR VALORES DE LAS FLAGS
void cambiarFlagNodo(char* nodo, uint32_t flagTransformacion, uint32_t flagAbortar){
    int i;
    for (i = 0; i<list_size(banderasReplanificacion); i++){
        banderasParaNodos * banderaAux = list_get(banderasReplanificacion,i);

        char* nodoAux = banderaAux->nodo;
        if(strcmp(nodoAux,nodo)==0){
            banderaAux->FLAG_REPLANIFICACION = flagTransformacion;
            banderaAux->FLAG_ABORTAR = flagAbortar;
        }    
    }
}



void* serializarTransformacionToWorker(uint32_t bloque, uint32_t bytesOcupados, char* nombreTemporal,char* contenidoScript){
    int posicionActual = 0;

    void* infoSerializada = malloc(tamanioDatosToWorker(nombreTemporal,contenidoScript));

    uint32_t tamanioContenido = string_length(contenidoScript);
    memcpy(infoSerializada + posicionActual, &tamanioContenido, sizeof(uint32_t));
    posicionActual += sizeof(uint32_t);
    memcpy(infoSerializada + posicionActual, contenidoScript, tamanioContenido);
    posicionActual += tamanioContenido;

    uint32_t tamanioScriptTransformador = string_length(scriptTransformador);
    memcpy(infoSerializada + posicionActual, &tamanioScriptTransformador, sizeof(uint32_t));
    posicionActual += sizeof(uint32_t);
    memcpy(infoSerializada + posicionActual, scriptTransformador, tamanioScriptTransformador);
    posicionActual += tamanioScriptTransformador;

    memcpy(infoSerializada + posicionActual, &bloque, sizeof(uint32_t));
    posicionActual += sizeof(uint32_t);

    memcpy(infoSerializada + posicionActual, &bytesOcupados, sizeof(uint32_t));
    posicionActual += sizeof(uint32_t);

    uint32_t tamanioTemporal = string_length(nombreTemporal);
    memcpy(infoSerializada + posicionActual, &tamanioTemporal,sizeof(uint32_t));
    posicionActual += sizeof(uint32_t);
    memcpy(infoSerializada + posicionActual, nombreTemporal, tamanioTemporal);
    posicionActual += tamanioTemporal;

    return infoSerializada;
}


void* serializarTransformacionToYama(char* nodo, uint32_t bloque){
    int posicionActual = 0;

    void* infoSerializada = malloc(sizeof(uint32_t)*2 + string_length(nodo));

    uint32_t tamanioNodo = string_length(nodo);
    memcpy(infoSerializada + posicionActual, &tamanioNodo, sizeof(uint32_t));
    posicionActual += sizeof(uint32_t);
    memcpy(infoSerializada + posicionActual, nodo, tamanioNodo);
    posicionActual += tamanioNodo;
    memcpy(infoSerializada + posicionActual, &bloque, sizeof(uint32_t));
    posicionActual += sizeof(uint32_t);

    return infoSerializada;
}


/*
infoNodo * recibirNodoAReplanificar(){
    infoNodo * nodoNuevo = (infoNodo*)malloc(sizeof(infoNodo));
    nodoNuevo->conexion = (conexionNodo*) malloc(sizeof(conexionNodo));

    nodoNuevo->conexion->nombreNodo = recibirString(socketYAMA);
    nodoNuevo->conexion->ipNodo = recibirString(socketYAMA);
    nodoNuevo->conexion->puertoNodo = recibirUInt(socketYAMA);
    nodoNuevo->nroBloque = recibirUInt(socketYAMA);
    nodoNuevo->bytesOcupados = recibirUInt(socketYAMA);
    nodoNuevo->nombreTemporal = recibirString(socketYAMA);

    return nodoNuevo;
}
*/