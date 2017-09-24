/*
 * serializaciones.h
 *
 *  Created on: 22/9/2017
 *      Author: utnso
 */

#include "estructuras.h"
#include "funcionesYAMA.h"

#ifndef SERIALIZACIONES_H_
#define SERIALIZACIONES_H_

void *serializarInfoTransformacion(t_list*);
int obtenerTamanioInfoTransformacion(t_list*);
void* serializarInfoReduccionLocal(conexionNodo*, char*, t_list*);
int obtenerTamanioInfoReduccionLocal(conexionNodo*, char*, t_list*);

#endif /* SERIALIZACIONES_H_ */
