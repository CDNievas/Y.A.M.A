/*
 * balanceoDeCargas.h
 *
 *  Created on: 22/9/2017
 *      Author: utnso
 */

#include "estructuras.h"
#include "serializaciones.h"

#ifndef BALANCEODECARGAS_H_
#define BALANCEODECARGAS_H_

typedef struct{
	char* nombreNodo;
	int cantidadDeJobs;
}posibleElegido;

copia* balancearCarga(t_list*);
char* nodoParaReduccionGlobal(int);
copia* replanificarTransformacion(int, t_list* , void*);

#endif /* BALANCEODECARGAS_H_ */
