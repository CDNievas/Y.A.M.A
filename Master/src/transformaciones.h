#include "structMaster.h"
#include "funcionesMaster.h"

#ifndef TRANSFORMACION_H_
#define TRANSFORMACION_H_

t_list* recibirSolicitudTransformacion();
void conectarAWorker(void*);
int tamanioDatosToWorker(char*);
void procesarTransformacion();
void* serializarTransformacionToWorker(uint32_t, uint32_t, char*);
void* serializarTransformacionToYama(char*, uint32_t);

#endif /* TRANSFORMACION_H_ */