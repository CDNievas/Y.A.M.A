/*
 * serializaciones.h
 *
 *  Created on: 22/9/2017
 *      Author: utnso
 */

#include "estructuras.h"

#ifndef SERIALIZACIONES_H_
#define SERIALIZACIONES_H_

int obtenerTamanioCopia(copia*, conexionNodo*);
copia* deserializarCopia(void*);
void* serializarCopia(copia*, conexionNodo*);
void *serializarInfoTransformacion(t_list*);
int obtenerTamanioInfoTransformacion(t_list*);
void* serializarInfoReduccionLocal(conexionNodo*, char*, t_list*);
int obtenerTamanioInfoReduccionLocal(conexionNodo*, char*, t_list*);

#endif /* SERIALIZACIONES_H_ */
