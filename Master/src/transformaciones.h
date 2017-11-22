#include "structMaster.h"
#include "funcionesMaster.h"

#ifndef TRANSFORMACION_H_
#define TRANSFORMACION_H_

t_list* recibirSolicitudTransformacion();
void conectarAWorkerTransformacion(void*);
int tamanioDatosToWorker(char*,char*);
void procesarTransformacion();
void* serializarTransformacionToWorker(uint32_t, uint32_t, char*,char*);
void* serializarTransformacionToYama(char*, uint32_t);
infoNodo * recibirNodoAReplanificar();
void crearBanderasParaCadaNodo();
void cargoHiloEnLista(pthread_t, char*);
void cambiarFlagNodo(char*, uint32_t, uint32_t);
void matadoraDeHilos();
void matarHilosDeTalNodo(char*);
void recibirNuevasSolicitudesYReplanificar();


#endif /* TRANSFORMACION_H_ */