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
uint32_t obtenerIdDirectorio(char **,int, int);
uint32_t obtenerIdPadreDirectorio(char **,int, int);
uint32_t obtenerIdPadreArchivo(char **,int, int);

#endif /* FUNCIONESPATH_H_ */
