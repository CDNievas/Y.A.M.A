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
t_list *filtrarReduccionesDelNodo(uint32_t);
t_list* obtenerConexionesDeNodos(t_list*, char*);
int cargarReduccionGlobal(int, int, t_list*);
void terminarReduccionGlobal(uint32_t);
int almacenadoFinal(int, uint32_t);
void reestablecerWL(int);
void fallaReduccionGlobal(int);

#endif /* REDUCCIONGLOBAL_H_ */
