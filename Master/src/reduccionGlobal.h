#include "funcionesMaster.h"
#include "structMaster.h"

#ifndef reduccionGlobal_H_
#define reduccionGlobal_H_

infoEncargadoRG * encargado;   // Var global donde se sabe quien es el encargado de la reducc global
// ACORDARSE DE HACER EL FREE DE ESTO.

t_list * recibirSolicitudReduccionGlobal();
void procesarReduccionGlobal();
uint32_t tamanioDatosToWorkerReduccion(t_list *);
void* serializarReduccionGlobalToWorker(t_list *);

#endif /* reduccionGlobal_H_ */
