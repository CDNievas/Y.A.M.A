/*
 * FuncionesDirectorios.h
 *
 *  Created on: 2/12/2017
 *      Author: utnso
 */

#ifndef FUNCIONESDIRECTORIOS_H_
#define FUNCIONESDIRECTORIOS_H_

#include "Estructuras.h"

int buscarYamafs(char **);
tablaArchivos* esArchivoPath(char *,int );
t_directory* esDirectorioPath(char *, int );
bool recorrerPath(char **,int ,int );
bool existePath(char * pathDirectorio);
void borrarArchivo(char * path);
int obtenerIdPadreArchivo(char ** pathDesc,int indice,int idPadre);

#endif /* FUNCIONESDIRECTORIOS_H_ */
