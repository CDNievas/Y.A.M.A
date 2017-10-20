#include "transformaciones.h"


//EL SOCKETYAMA LO DECLARE COMO VARIABLE GLOBAL.

// Creo lista de solicitudes de transformaciones
t_list* recibirSolicitudTransformacion(){
    t_list * listaPRecibir = list_create();   
    uint32_t cantidadNodos = recibirUInt(socketYama); // supuestamente recibo la cantidad total de datos primero.
    int i;

    for(i=0; i < cantidadNodos; i++){
        infoNodo * nodoActual = malloc(sizeof(infoNodo));
        nodoActual->conexion = (conexionNodo*) malloc(sizeof(conexionNodo));

        nodoActual->conexion->nombreNodo = recibirString(socketYama);
        nodoActual->conexion->ipNodo = recibirString(socketYama);
        nodoActual->conexion->puertoNodo = recibirUInt(socketYama);
        nodoActual->nroBloque = recibirUInt(socketYama);
        nodoActual->bytesOcupados = recibirUInt(socketYama);
        nodoActual->nombreTemporal = recibirString(socketYama);

        list_add(listaPRecibir,nodoActual);
    }

    return listaPRecibir;

}

//Funcion que atiende las solicitudes de transformacion.
void conectarAWorker(void* nodoConInfo){
    infoNodo nuevo = *(infoNodo*) nodoConInfo;

    void* datosSerializados = serializarToWorker(nuevo.nroBloque, nuevo.bytesOcupados, nuevo.nombreTemporal);
    int tamanioDatos = tamanioDatosToWorker(nuevo.nombreTemporal);

    int socketWorker = conectarAServer(nuevo.conexion->ipNodo, nuevo.conexion->puertoNodo);
    //propagarArchivo(archivo,socketWorker);        // Enviar programa de transformacion.

    sendRemasterizado(socketWorker,1 /*TRANSFORMACION*/, tamanioDatos,datosSerializados);  // SEGUN EL PROTOCOLO TRANSFORMACION = 1. Sin definir en este archivo.
    uint32_t respuesta = recibirUInt(socketWorker);                                                     //CONFIRMACION DE WORKER

    // LA NOTIFICACION A YAMA TIENE Q SER EL NOMBRE DEL NODO Y EL BLOQUE.
    //sendRemasterizado(socketYAMA,1 /*TRANSFORMACION*/, sizeof(int), (void*) respuesta);           //NOTIFICAR A YAMA

}

int tamanioDatosToWorker(char* nombreTemporal){
    return sizeof(int) + sizeof(long) + string_length(nombreTemporal);
}

//Creo todos los hilos para cada solicitud.
void procesarTransformacion(){
    t_list * transformaciones = recibirSolicitudTransformacion();    
    int i;

    for(i=0; i < list_size(transformaciones); i++){
        pthread_t hiloInit;
        infoNodo * nodoParaHilo = malloc(sizeof(infoNodo));
        nodoParaHilo->conexion = (conexionNodo*) malloc(sizeof(conexionNodo));
        nodoParaHilo = list_get(transformaciones,i); // saco informacion de las posiciones una por una
        pthread_create(&hiloInit,NULL, (void*) conectarAWorker,(void*)nodoParaHilo);
        free(nodoParaHilo->conexion);
        free(nodoParaHilo);
        pthread_join(hiloInit,NULL);
    }

}

/*
void* serializarToWorker(int bloque, long bytesOcupados, char* nombreTemporal){
    int posicionActual = 0;

    void* infoSerializada = malloc(tamanioDatosToWorker(nombreTemporal));

    memcpy(infoSerializada + posicionActual, &bloque, sizeof(int));
    posicionActual += sizeof(int);
    memcpy(infoSerializada + posicionActual, &bytesOcupados, sizeof(long));
    posicionActual += sizeof(long);
    int tamanioTemporal = string_length(nombreTemporal);
    memcpy(infoSerializada + posicionActual, &tamanioTemporal,sizeof(int));

    return infoSerializada;
}

void* serializarToYama(char* nodo, int bloque){}
*/