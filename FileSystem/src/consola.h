/*
 * consola.h
 *
 *  Created on: 5/12/2017
 *      Author: utnso
 */

#ifndef CONSOLA_H_
#define CONSOLA_H_

#include "estructuras.h"
#include "funcionesConsola.h"

typedef struct {
	int flag;
	char *nombre;     /* Nombre de la funcion */
} command;


void consolaFS();
void analizarComando(char *);
void ejecutarComando(uint32_t,char **);
void imprimirComandos();

#endif /* CONSOLA_H_ */
