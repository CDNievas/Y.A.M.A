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

#endif /* TRANSFORMACION_H_ */