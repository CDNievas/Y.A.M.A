/*
 * nodos.h
 *
 *  Created on: 4/12/2017
 *      Author: utnso
 */

#ifndef NODOS_H_
#define NODOS_H_

#include "estructuras.h"
void verificarCopiasNodo(char* );
void perteneceAlSistema(char* , int , char* , uint32_t );
bool hayUnEstadoEstable();
void registrarNodosConectados();
void verificarSiEsNodoDesconectado(char* , uint32_t ,char* ,uint32_t );


void registrarNodo(int);
void simulaBalanceo(int);

#endif /* NODOS_H_ */
