/*
 * funcionesPath.h
 *
 *  Created on: 7/12/2017
 *      Author: utnso
 */

#ifndef FUNCIONESPATH_H_
#define FUNCIONESPATH_H_

#include "estructuras.h"

char* obtenerNombreUltimoPath(char**);
bool existePath(char *);
bool recorrerPath(char **,int,int);
strArchivo * existeArchivoPath(char * ,int );
strDirectorio * existeDirectorioPath(char * , int );
int directorioInexistente(char **, int , int );
bool existePathLocal(char *);
int obtenerIdDirectorio(char **,int, int);
int obtenerIdPadreDirectorio(char **,int, int);

#endif /* FUNCIONESPATH_H_ */
