/*
 * reduccionLocal.h
 *
 *  Created on: 22/9/2017
 *      Author: utnso
 */

#include "funcionesYAMA.h"
#include "estructuras.h"
#include "serializaciones.h"
#include "balanceoDeCargas.h"


#ifndef REDUCCIONLOCAL_H_
#define REDUCCIONLOCAL_H_

int cargarReduccionLocal(int, int, t_list*);
void terminarReduccionLocal(int, int);
bool sePuedeHacerReduccionLocal(t_list*);
void fallaReduccionLocal(int);
void reestablecerWLReducLocal(uint32_t);

#endif /* REDUCCIONLOCAL_H_ */
