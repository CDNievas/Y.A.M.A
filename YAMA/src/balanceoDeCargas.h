/*
 * balanceoDeCargas.h
 *
 *  Created on: 22/9/2017
 *      Author: utnso
 */

#include "estructuras.h"

#ifndef BALANCEODECARGAS_H_
#define BALANCEODECARGAS_H_

typedef struct{
	char* nombreNodo;
	int cantidadDeJobs;
}posibleElegido;

copia* balancearCarga(int, infoDeFs*);
char* nodoParaReduccionGlobal(int);

#endif /* BALANCEODECARGAS_H_ */
