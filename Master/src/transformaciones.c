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

    void* datosToWorker = serializarTransformacionToWorker(nuevo.nroBloque, nuevo.bytesOcupados, nuevo.nombreTemporal);
    void* datosToYama = serializarTransformacionToYama(nuevo.conexion->nombreNodo, nuevo.nroBloque);
    int tamanioDatosToW = tamanioDatosToWorker(nuevo.nombreTemporal);
    int tamanioDatosToY = sizeof(uint32_t)*2 + string_length(nuevo.conexion->nombreNodo);

    int socketWorker = conectarAServer(nuevo.conexion->ipNodo, nuevo.conexion->puertoNodo);
    //propagarArchivo(archivo,socketWorker);        // Enviar programa de transformacion.
    sendRemasterizado(socketWorker,1 /*TRANSFORMACION*/, tamanioDatosToW, datosToWorker);  // SEGUN EL PROTOCOLO TRANSFORMACION = 1. Sin definir en este archivo.
    uint32_t respuesta = recibirUInt(socketWorker);                                                   //CONFIRMACION DE WORKER
    // LA NOTIFICACION A YAMA TIENE Q SER EL NOMBRE DEL NODO Y EL BLOQUE.
    // QUE CARAJO HAGO CON LA RESPUESTA DE WORKER?
    sendRemasterizado(socketYAMA,1 /*TRANSFORMACION*/, tamanioDatosToY, datosToYama);               //NOTIFICAR A YAMA


    //free(nodoParaHilo->conexion);
    //free(nodoParaHilo);
}

int tamanioDatosToWorker(char* nombreTemporal){
    return sizeof(uint32_t)*3 + string_length(nombreTemporal);
}

//Creo todos los hilos para cada solicitud.
void procesarTransformacion(){
    t_list * transformaciones = recibirSolicitudTransformacion();    
    int i;

    for(i=0; i < list_size(transformaciones); i++){
        pthread_t hiloInit;
        infoNodo * nodoParaHilo = malloc(sizeof(infoNodo));
        nodoParaHilo->conexion = (conexionNodo*) malloc(sizeof(conexionNodo));
        nodoParaHilo->conexion->nombreNodo = string_new();
        nodoParaHilo->conexion->ipNodo = string_new();
        nodoParaHilo->nombreTemporal = string_new();

        nodoParaHilo = list_get(transformaciones,i); // saco informacion de las posiciones una por una
        pthread_create(&hiloInit,NULL, (void*) conectarAWorkerTransformacion,(void*)nodoParaHilo);
        pthread_join(hiloInit,NULL);
    }

}

void* serializarTransformacionToWorker(uint32_t bloque, uint32_t bytesOcupados, char* nombreTemporal){
    int posicionActual = 0;

    void* infoSerializada = malloc(tamanioDatosToWorker(nombreTemporal));

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
