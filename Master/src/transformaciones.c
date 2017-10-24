#include "transformaciones.h"


//EL SOCKETYAMA LO DECLARE COMO VARIABLE GLOBAL.

// Creo lista de solicitudes de transformaciones
t_list* recibirSolicitudTransformacion(){
    t_list * listaPRecibir = list_create();   
    uint32_t cantidadNodos = recibirUInt(socketYAMA); // supuestamente recibo la cantidad total de datos primero.
    int i;

    for(i=0; i < cantidadNodos; i++){
        infoNodo * nodoActual = malloc(sizeof(infoNodo));
        nodoActual->conexion = (conexionNodo*) malloc(sizeof(conexionNodo));

        nodoActual->conexion->nombreNodo = recibirString(socketYAMA);
        verificarNodo(nodoActual->conexion->nombreNodo); // Verifico si puede agregar el nombre de nodo a la lista para saber cantidad de Procesos nodos 
        nodoActual->conexion->ipNodo = recibirString(socketYAMA);
        nodoActual->conexion->puertoNodo = recibirUInt(socketYAMA);
        nodoActual->nroBloque = recibirUInt(socketYAMA);
        nodoActual->bytesOcupados = recibirUInt(socketYAMA);
        nodoActual->nombreTemporal = recibirString(socketYAMA);
    
        list_add(listaPRecibir,nodoActual);

    return listaPRecibir;

}


//Funcion que atiende las solicitudes de transformacion.
void conectarAWorkerTransformacion(void* nodoConInfo){
    infoNodo nuevo = *(infoNodo*) nodoConInfo;

    void* datosToWorker = serializarTransformacionToWorker(nuevo.nroBloque, nuevo.bytesOcupados, nuevo.nombreTemporal);
    void* datosToYama = serializarTransformacionToYama(nuevo.conexion->nombreNodo, nuevo.nroBloque);
    int tamanioDatosToW = tamanioDatosToWorker(nuevo.nombreTemporal);
    int tamanioDatosToY = sizeof(uint32_t)*2 + string_length(nuevo.conexion->nombreNodo);

    log_info(loggerMaster,"Realizando conexion con Worker. IP: %s. Puerto: %d.", nuevo.conexion->ipNodo, nuevo.conexion->puertoNodo);
    int socketWorker = conectarAServer(nuevo.conexion->ipNodo, nuevo.conexion->puertoNodo);
    sendDeNotificacion(socketWorker, TRANSFORMACION);
    log_info(loggerMaster, "Conexion con Worker exitosa.");
    log_info(loggerMaster,"Propagando archivo transformador a Worker.");
    propagarArchivo(scriptTransformador,socketWorker);        // Enviar programa de transformacion.
    log_info(loggerMaster,"Propagacion de archivo a Worker exitosa.");
    log_info(loggerMaster,"Enviando bloque:%d, bytes ocupados:%d y temporal:%s a Worker", nuevo.nroBloque, nuevo.bytesOcupados, nuevo.nombreTemporal);
    sendRemasterizado(socketWorker,TRANSFORMACION, tamanioDatosToW, datosToWorker); 
    // Uso las funciones wait y signal para recibir la respuesta. El mutex permite que no reciban dos hilos al mismo tiempo la rta
    //***********************************************
    pthread_mutex_lock(&mutexTransformador);
    uint32_t respuesta = recibirUInt(socketWorker);                                                 //CONFIRMACION DE WORKER
    pthread_mutex_unlock(&mutexTransformador);
    //***********************************************
    log_info(loggerMaster,"Recibo respuesta por parte del Worker.");
    log_info(loggerMaster,"Notifico a YAMA del resultado de la transformacion.");
    sendRemasterizado(socketYAMA,respuesta, tamanioDatosToY, datosToYama);                          //NOTIFICAR A YAMA
}


int tamanioDatosToWorker(char* nombreTemporal){
    return sizeof(uint32_t)*4 + string_length(nombreTemporal) + string_length(scriptTransformador);
}

//Creo todos los hilos para cada solicitud.
void procesarTransformacion(){
    t_list* solicitudesTransformacion = recibirSolicitudTransformacion();
    cantidadDeProcesosNodos = list_size(nombresNodos); // Asigno la cantidad de procesos nodos que tengo para la reduccion local
    log_info(loggerMaster,"Recibo lista de solicitudes de transformacion.");    
    int i;

    for(i=0; i < list_size(solicitudesTransformacion); i++){
        pthread_t hiloInit;
        infoNodo * nodoParaHilo = malloc(sizeof(infoNodo));
        nodoParaHilo->conexion = (conexionNodo*) malloc(sizeof(conexionNodo));
        nodoParaHilo->conexion->nombreNodo = string_new();
        nodoParaHilo->conexion->ipNodo = string_new();
        nodoParaHilo->nombreTemporal = string_new();
        nodoParaHilo = list_get(solicitudesTransformacion,i); // saco informacion de las posiciones una por una
        log_info(loggerMaster, "Realizando transformacion en el Nodo: %s.", nodoParaHilo->conexion->nombreNodo);
        pthread_create(&hiloInit,NULL, (void*) conectarAWorkerTransformacion,(void*)nodoParaHilo);
        pthread_join(hiloInit,NULL);
    }

}


void* serializarTransformacionToWorker(uint32_t bloque, uint32_t bytesOcupados, char* nombreTemporal){
    int posicionActual = 0;

    void* infoSerializada = malloc(tamanioDatosToWorker(nombreTemporal));

    uint32_t tamanioScriptTransformador = string_length(scriptTransformador);
    memcpy(infoSerializada + posicionActual, &tamanioScriptTransformador, sizeof(uint32_t));
    posicionActual += sizeof(uint32_t);
    memcpy(infoSerializada + posicionActual, &scriptTransformador, tamanioScriptTransformador);
    posicionActual += tamanioScriptTransformador;
    memcpy(infoSerializada + posicionActual, &bloque, sizeof(uint32_t));
    posicionActual += sizeof(uint32_t);
    memcpy(infoSerializada + posicionActual, &bytesOcupados, sizeof(uint32_t));
    posicionActual += sizeof(uint32_t);
    uint32_t tamanioTemporal = string_length(nombreTemporal);
    memcpy(infoSerializada + posicionActual, &tamanioTemporal,sizeof(uint32_t));
    posicionActual += sizeof(uint32_t);
    memcpy(infoSerializada + posicionActual, &nombreTemporal, tamanioTemporal);
    posicionActual += tamanioTemporal;

    return infoSerializada;
}


void* serializarTransformacionToYama(char* nodo, uint32_t bloque){
    int posicionActual = 0;

    void* infoSerializada = malloc(sizeof(uint32_t)*2 + string_length(nodo));

    uint32_t tamanioNodo = string_length(nodo);
    memcpy(infoSerializada + posicionActual, &tamanioNodo, sizeof(uint32_t));
    posicionActual += sizeof(uint32_t);
    memcpy(infoSerializada + posicionActual, &nodo, tamanioNodo);
    posicionActual += tamanioNodo;
    memcpy(infoSerializada + posicionActual, &bloque, sizeof(uint32_t));
    posicionActual += sizeof(uint32_t);

    return infoSerializada;
}
