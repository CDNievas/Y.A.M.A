/*
 * reduccionLocal.h
 *
 *  Created on: 22/9/2017
 *      Author: utnso
 */

#include "funcionesYAMA.h"
#include "estructuras.h"
#include "serializaciones.h"


#ifndef REDUCCIONLOCAL_H_
#define REDUCCIONLOCAL_H_

void cargarReduccionLocal(int, int, t_list*);
void terminarReduccionLocal(int, void*);
bool sePuedeHacerReduccionLocal(t_list*);

#endif /* REDUCCIONLOCAL_H_ */
