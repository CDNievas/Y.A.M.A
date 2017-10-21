/*
 * reduccionGlobal.h
 *
 *  Created on: 19/10/2017
 *      Author: utnso
 */

#include "funcionesYAMA.h"
#include "estructuras.h"
#include "serializaciones.h"
#include "balanceoDeCargas.h"

#ifndef REDUCCIONGLOBAL_H_
#define REDUCCIONGLOBAL_H_

bool sePuedeHacerReduccionGlobal(int);
t_list *filtrarReduccionesDelNodo(int);
t_list* obtenerConexionesDeNodos(t_list*);
char* cargarReduccionGlobal(int, int, t_list*);
void terminarReduccionGlobal(uint32_t);

#endif /* REDUCCIONGLOBAL_H_ */
