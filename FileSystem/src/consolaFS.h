/*
 * consolaFS.h
 *
 *  Created on: 15/9/2017
 *      Author: utnso
 */

#include "../../Biblioteca/src/configParser.h"
#include <readline/chardefs.h>
#include <readline/history.h>
#include <commons/bitarray.h>
#include <sys/mman.h>

#ifndef CONSOLAFS_H_
#define CONSOLAFS_H_

void imprimirComandos();
void analizarComando(char *);
void consolaFS();

#endif /* CONSOLAFS_H_ */
