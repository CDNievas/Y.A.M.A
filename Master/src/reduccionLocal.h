#include "funcionesMaster.h"
#include "structMaster.h"


#ifndef reduccionLocal_H_
#define reduccionLocal_H_

infoReduccionLocal* recibirSolicitudReduccionLocal();
void conectarAWorkerReduccionLocal(void*);
void procesarReduccionLocal();
uint32_t obtenerTamanioReduccionToWorker(t_list* , char*,char*);
void* serializarReduccionLocalToWorker(t_list*, char*,char*);


#endif /* reduccionLocal_H_ */