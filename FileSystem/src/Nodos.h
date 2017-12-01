/*
 * Nodos.h
 *
 *  Created on: 25/11/2017
 *      Author: utnso
 */
#include "Estructuras.h"
#include "Persistencia.h"

#ifndef NODOS_H_
#define NODOS_H_


int cantBloquesLibres(t_bitarray* , uint32_t) ;


int sacarPorcentajeOcioso(int , int );

void registrarNodo(int );


#endif /* NODOS_H_ */
