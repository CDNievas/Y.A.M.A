/*
 * funcionesConsola.h
 *
 *  Created on: 7/12/2017
 *      Author: utnso
 */

#include "estructuras.h"
#include "persistencia.h"

#ifndef FUNCIONESCONSOLA_H_
#define FUNCIONESCONSOLA_H_

int cantParam(char **);
bool chequearParamCom(char **, int , int);
bool contieneYamafs(char *);
int crearDirectorio(char *);
bool almacenarArchivo(char*, char*, char*);
int borrarDirectorio(char *);
uint32_t sacarTamanioArchivo (FILE*);

#endif /* FUNCIONESCONSOLA_H_ */
